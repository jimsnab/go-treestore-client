package treestore_client

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jimsnab/go-lane"
	"github.com/jimsnab/go-treestore"
)

type (
	tsClient struct {
		sync.Mutex
		l           lane.Lane
		cxn         net.Conn
		hostAndPort string
		inbound     []byte
		invoked     atomic.Int32
	}
)

func NewTSClient(l lane.Lane) TSClient {
	tsc := &tsClient{
		l:           l,
		hostAndPort: "localhost:6770",
	}

	return tsc
}

func (tsc *tsClient) close() (err error) {
	for {
		invoked := false
		tsc.Lock()
		if tsc.cxn != nil {
			err = tsc.cxn.Close()
			tsc.cxn = nil
		}
		invoked = tsc.invoked.Load() != 0
		tsc.Unlock()

		if !invoked {
			break
		}

		time.Sleep(time.Millisecond)
	}
	return
}

// Assigns the host and port, which is used on the next API call to connect
// to the treestore server.
func (tsc *tsClient) SetServer(host string, port int) {
	tsc.close()

	tsc.Lock()
	defer tsc.Unlock()
	tsc.hostAndPort = fmt.Sprintf("%s:%d", host, port)
}

// Disconnects from the treestore server.
func (tsc *tsClient) Close() (err error) {
	err = tsc.close()
	return
}

// Sends a raw command-line encoded command to the treestore server. This
// can be used to implement a CLI client.
func (tsc *tsClient) RawCommand(args ...string) (response map[string]any, err error) {
	tsc.invoked.Add(1)
	defer tsc.invoked.Add(-1)

	//
	// Ensure connection
	//

	err = func() (err error) {
		tsc.Lock()
		defer tsc.Unlock()

		if tsc.cxn == nil {
			var cxn net.Conn
			cxn, err = net.Dial("tcp", tsc.hostAndPort)
			if err != nil {
				tsc.l.Errorf("can't connect to %s: %s", tsc.hostAndPort, err.Error())
				return
			}

			tsc.cxn = cxn
		}
		return
	}()
	if err != nil {
		return
	}

	//
	// Send the command with args separated by \n
	//
	// "setk\n/key/path\n"
	//

	joined := strings.Join(args, "\n")

	req := make([]byte, len(joined)+4)
	binary.BigEndian.PutUint32(req, uint32(len(joined)))
	copy(req[4:], []byte(joined))

	n, err := tsc.cxn.Write(req)
	if err != nil {
		tsc.l.Errorf("failed to write request: %s", err.Error())
		tsc.cxn.Close()
		tsc.cxn = nil
		return
	}
	if n != len(req) {
		err = fmt.Errorf("%d bytes sent of %d", n, len(req))
		tsc.l.Errorf("failed to write request: %s", err.Error())
		tsc.cxn.Close()
		tsc.cxn = nil
		return
	}

	//
	// The response will be returned in json.
	//

	for {
		// buffer must be allocated for each read, because tsc.inbound slice is referencing it
		buffer := make([]byte, 1024*8)

		// put a time limit on an api
		tsc.cxn.SetReadDeadline(time.Now().Add(20 * time.Second))
		n, err = tsc.cxn.Read(buffer)

		if err != nil {
			if !errors.Is(err, io.EOF) && !strings.HasSuffix(err.Error(), "use of closed network connection") {
				tsc.l.Errorf("read error from %s: %s", tsc.cxn.RemoteAddr().String(), err.Error())
			}
			tsc.cxn.Close()
			tsc.cxn = nil
			return
		}

		if tsc.inbound == nil {
			tsc.inbound = buffer[0:n]
		} else {
			tsc.inbound = append(tsc.inbound, buffer[0:n]...)
		}

		tsc.l.Tracef("received %d bytes from server", len(tsc.inbound))

		var length int
		length, response, err = tsc.parseResponse()
		if err != nil {
			tsc.l.Errorf("bad response from %s: %s", tsc.cxn.RemoteAddr().String(), err.Error())
			tsc.cxn.Close()
			tsc.cxn = nil
			return
		}
		if response != nil {
			tsc.inbound = tsc.inbound[length:]

			errText, isError := response["error"].(string)
			if isError {
				err = errors.New(errText)
				return
			}
			return
		}
	}
}

func (tsc *tsClient) parseResponse() (length int, response map[string]any, err error) {
	if len(tsc.inbound) < 4 {
		return
	}

	packetSize := binary.BigEndian.Uint32(tsc.inbound)
	if len(tsc.inbound)-4 < int(packetSize) {
		tsc.l.Tracef("insufficient input, expecting %d bytes, have %d bytes", packetSize, len(tsc.inbound)-4)
		return
	}

	packet := tsc.inbound[4 : 4+packetSize]
	if err = json.Unmarshal(packet, &response); err != nil {
		return
	}

	length = 4 + int(packetSize)
	return
}

// Set a key without a value and without an expiration, doing nothing if the
// key already exists. The key index is not altered.
func (tsc *tsClient) SetKey(sk StoreKey) (address StoreAddress, exists bool, err error) {
	response, err := tsc.RawCommand("setk", string(sk.Path))
	if err != nil {
		return
	}

	address = responseAddress(response["address"])
	exists = responseBool(response["exists"])
	return
}

// Set a key with a value, without an expiration, adding to value history if the
// key already exists.
func (tsc *tsClient) SetKeyValue(sk StoreKey, value any) (address StoreAddress, firstValue bool, err error) {
	val, valType, err := nativeValueToCmdline(value)
	if err != nil {
		return
	}

	args := []string{"setv", string(sk.Path), val}
	if valType != "" {
		args = append(args, "--value-type", valType)
	}

	response, err := tsc.RawCommand(args...)
	if err != nil {
		return
	}

	address = responseAddress(response["address"])
	firstValue = responseBool(response["firstValue"])
	return
}

// Ensures a key exists, optionally sets a value, optionally sets or removes key expiration, and
// optionally replaces the relationships array.
//
// Flags:
//
//	SetExNoValueUpdate - do not alter the key's value (ignore `value` argument, do not alter key index)
//	SetExMustExist - perform only if the key exists
//	SetExMustNotExist - perform only if the key does not exist
//
// For `expireNs`, specify the Unix nanosecond tick of when the key will expire. Specify zero to
// remove expiration. Specify -1 to retain the current key expiration.
//
// `originalValue` will be provided if the key exists and has a value, even if no change is made.
//
// A non-nil `relationships` will replace the relationships of the key node. An empty array
// removes all relationships. Specify nil to retain the current key relationships.
func (tsc *tsClient) SetKeyValueEx(sk StoreKey, value any, flags SetExFlags, expire *time.Time, relationships []StoreAddress) (address StoreAddress, exists bool, originalValue any, err error) {
	args := []string{"setex", string(sk.Path)}
	if (flags & SetExNoValueUpdate) == 0 {
		if value == nil {
			args = append(args, "--nil")
		} else {
			var val, valType string
			val, valType, err = nativeValueToCmdline(value)
			if err != nil {
				return
			}

			args = append(args, "--value", val)
			if valType != "" {
				args = append(args, "--value-type", valType)
			}
		}
	}

	if (flags & SetExMustExist) != 0 {
		args = append(args, "--mx")
	} else if (flags & SetExMustNotExist) != 0 {
		args = append(args, "--nx")
	}

	if expire != nil {
		args = append(args, "--ns", fmt.Sprintf("%d", expire.UnixNano()))
	}

	if relationships != nil {
		var sb strings.Builder
		for idx, addr := range relationships {
			if idx > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(fmt.Sprintf("%d", addr))
		}
		args = append(args, "--relationships", sb.String())
	}

	response, err := tsc.RawCommand(args...)
	if err != nil {
		return
	}

	address = responseAddress(response["address"])
	exists = responseBool(response["exists"])

	orgVal, hasOrgVal := response["original_value"].(string)
	if hasOrgVal {
		orgValType, _ := response["original_type"].(string)
		if originalValue, err = cmdlineToNativeValue(orgVal, orgValType); err != nil {
			return
		}
	}
	return
}

// Looks up the key in the index and returns true if it exists and has value history.
func (tsc *tsClient) IsKeyIndexed(sk StoreKey) (address StoreAddress, exists bool, err error) {
	response, err := tsc.RawCommand("indexed", string(sk.Path))
	if err != nil {
		return
	}

	addrStr, exists := response["address"]
	address = responseAddress(addrStr)
	return
}

// Walks the tree level by level and returns the current address, whether or not
// the key path is indexed. This avoids putting a lock on the index, but will lock
// tree levels while walking the tree.
func (tsc *tsClient) LocateKey(sk StoreKey) (address StoreAddress, exists bool, err error) {
	response, err := tsc.RawCommand("getk", string(sk.Path))
	if err != nil {
		return
	}

	addrStr, exists := response["address"].(float64)
	if exists {
		address = responseAddress(addrStr)
	}

	return
}

// Navigates to the valueInstance key node and returns the expiration time in Unix nanoseconds, or
// -1 if the key path does not exist.
func (tsc *tsClient) GetKeyTtl(sk StoreKey) (ttl *time.Time, err error) {
	response, err := tsc.RawCommand("ttlk", string(sk.Path))
	if err != nil {
		return
	}

	ttlStr, exists := response["ttl"].(string)
	if exists {
		ttl = responseEpochNs(ttlStr)
	}
	return
}

// Navigates to the valueInstance key node and sets the expiration time in Unix nanoseconds.
// Specify 0 for no expiration.
func (tsc *tsClient) SetKeyTtl(sk StoreKey, expiration *time.Time) (exists bool, err error) {
	response, err := tsc.RawCommand("expirekns", string(sk.Path), requestEpochNs(expiration))
	if err != nil {
		return
	}

	exists = responseBool(response["exists"])
	return
}

// Looks up the key in the index and returns the current value and flags
// that indicate if the key was set, and if so, if it has a value.
func (tsc *tsClient) GetKeyValue(sk StoreKey) (value any, keyExists, valueExists bool, err error) {
	response, err := tsc.RawCommand("getv", string(sk.Path))
	if err != nil {
		return
	}

	keyExists = responseBool(response["key_exists"])
	if keyExists {
		var valStr string
		valStr, valueExists = response["value"].(string)
		if valueExists {
			valType, _ := response["type"].(string)
			value, err = cmdlineToNativeValue(valStr, valType)
			if err != nil {
				return
			}
		}
	}
	return
}

// Looks up the key and returns the expiration time in Unix nanoseconds, or
// nil if the key value does not exist.
func (tsc *tsClient) GetKeyValueTtl(sk StoreKey) (ttl *time.Time, err error) {
	response, err := tsc.RawCommand("ttlv", string(sk.Path))
	if err != nil {
		return
	}

	ttlStr, exists := response["ttl"].(string)
	if exists {
		ttl = responseEpochNs(ttlStr)
	}
	return
}

// Looks up the key and sets the expiration time in Unix nanoseconds. Specify
// expiration as nil to clear the ttl.
func (tsc *tsClient) SetKeyValueTtl(sk StoreKey, expiration *time.Time) (exists bool, err error) {
	response, err := tsc.RawCommand("expirevns", string(sk.Path), requestEpochNs(expiration))
	if err != nil {
		return
	}

	exists = responseBool(response["exists"])
	return
}

// Looks up the key in the index and scans history for the specified Unix ns tick,
// returning the value at that moment in time, if one exists.
//
// To specify a relative time, specify `tickNs` as the negative ns from the current
// time, e.g., -1000000000 is one second ago.
func (tsc *tsClient) GetKeyValueAtTime(sk StoreKey, when *time.Time) (value any, exists bool, err error) {
	response, err := tsc.RawCommand("vat", string(sk.Path), requestEpochNs(when))
	if err != nil {
		return
	}

	var valStr string
	valStr, exists = response["value"].(string)
	if exists {
		valType, _ := response["value_type"].(string)
		if value, err = cmdlineToNativeValue(valStr, valType); err != nil {
			return
		}
	}
	return
}

// Deletes an indexed key that has a value, including its value history, and its metadata.
// Specify `clean` as `true` to delete parent key nodes that become empty, or `false` to only
// remove the valueInstance key node.
//
// Returns `removed` == true if the value was deleted.
//
// The valueInstance key will still exist if it has children or if it is the sentinel key node.
func (tsc *tsClient) DeleteKeyWithValue(sk StoreKey, clean bool) (removed bool, originalValue any, err error) {
	args := []string{"delv", string(sk.Path)}
	if clean {
		args = append(args, "--clean")
	}
	response, err := tsc.RawCommand(args...)
	if err != nil {
		return
	}

	var orgValStr string
	orgValStr, removed = response["original_value"].(string)
	if removed {
		orgValType, _ := response["original_type"].(string)
		if originalValue, err = cmdlineToNativeValue(orgValStr, orgValType); err != nil {
			return
		}
	}
	return
}

// Deletes a key value, including its value history, and its metadata - and the
// valueInstance key node also if it does not have children.
//
// The parent key node is not altered.
//
// `keyRemoved` == `true` when the valueInstance key node is deleted.
// `valueRemoved` == true if the key value is cleared.
//
// All key nodes along the store key path will be locked during the operation, so
// this operation blocks subsequent operations until it completes.
//
// The sentinal (root) key node cannot be deleted; only its value can be cleared.
func (tsc *tsClient) DeleteKey(sk StoreKey) (keyRemoved, valueRemoved bool, originalValue any, err error) {
	response, err := tsc.RawCommand("delk", string(sk.Path))
	if err != nil {
		return
	}

	keyRemoved = responseBool(response["key_removed"])

	var orgValStr string
	orgValStr, valueRemoved = response["original_value"].(string)
	if valueRemoved {
		orgValType, _ := response["original_type"].(string)
		if originalValue, err = cmdlineToNativeValue(orgValStr, orgValType); err != nil {
			return
		}
	}
	return
}

// Deletes a key and all of its child data.
//
// All key nodes along the store key path will be locked during the operation, so
// this operation blocks subsequent operations until it completes.
//
// The sentinal (root) key node cannot be deleted; only its value can be cleared.
func (tsc *tsClient) DeleteKeyTree(sk StoreKey) (removed bool, err error) {
	response, err := tsc.RawCommand("deltree", string(sk.Path))
	if err != nil {
		return
	}

	removed = responseBool(response["removed"])
	return
}

// Sets a metadata attribute on a key, returning the original value (if any)
func (tsc *tsClient) SetMetadataAttribute(sk StoreKey, attribute, value string) (keyExists bool, priorValue string, err error) {
	response, err := tsc.RawCommand("setmeta", string(sk.Path), attribute, value)
	if err != nil {
		return
	}

	keyExists = responseBool(response["key_exists"])
	priorValue = response["prior_value"].(string)
	return
}

// Removes a single metadata attribute from a key
func (tsc *tsClient) ClearMetadataAttribute(sk StoreKey, attribute string) (attributeExists bool, originalValue string, err error) {
	response, err := tsc.RawCommand("delmeta", string(sk.Path), attribute)
	if err != nil {
		return
	}

	originalValue, attributeExists = response["original_value"].(string)
	return
}

// Discards all metadata on the specific key
func (tsc *tsClient) ClearKeyMetadata(sk StoreKey) (err error) {
	_, err = tsc.RawCommand("resetmeta", string(sk.Path))
	return
}

// Fetches a key's metadata value for a specific attribute
func (tsc *tsClient) GetMetadataAttribute(sk StoreKey, attribute string) (attributeExists bool, value string, err error) {
	response, err := tsc.RawCommand("getmeta", string(sk.Path), attribute)
	if err != nil {
		return
	}

	value, attributeExists = response["value"].(string)
	return
}

// Returns an array of attribute names of metadata stored for the specified key
func (tsc *tsClient) GetMetadataAttributes(sk StoreKey) (attributes []string, err error) {
	response, err := tsc.RawCommand("lsmeta", string(sk.Path))
	if err != nil {
		return
	}

	attributesAny, _ := response["attributes"].([]any)
	if attributesAny != nil {
		attributes = make([]string, 0, len(attributesAny))
		for _, attribute := range attributesAny {
			attributes = append(attributes, attribute.(string))
		}
	}
	return
}

// Converts an address to a store key
func (tsc *tsClient) KeyFromAddress(addr StoreAddress) (sk StoreKey, exists bool, err error) {
	response, err := tsc.RawCommand("addrk", requestAddress(addr))
	if err != nil {
		return
	}

	tokenPath, exists := response["key"].(string)
	if exists {
		sk = StoreKey(treestore.MakeStoreKeyFromPath(treestore.TokenPath(tokenPath)))
	}
	return
}

// Fetches the current value by address
func (tsc *tsClient) KeyValueFromAddress(addr StoreAddress) (keyExists, valueExists bool, sk StoreKey, value any, err error) {
	response, err := tsc.RawCommand("addrv", requestAddress(addr))
	if err != nil {
		return
	}

	tokenPath, keyExists := response["key"].(string)
	if keyExists {
		sk = StoreKey(treestore.MakeStoreKeyFromPath(treestore.TokenPath(tokenPath)))

		var valStr string
		valStr, valueExists = response["value"].(string)
		if valueExists {
			valType, _ := response["type"].(string)
			if value, err = cmdlineToNativeValue(valStr, valType); err != nil {
				return
			}
		}
	}
	return
}

// Retreives a value by following a relationship link. The target value is
// returned in `rv`, and will be nil if the target doesn't exist. The
// `hasLink` flag indicates true when a relationship is stored at the
// specified `relationshipIndex`.
func (tsc *tsClient) GetRelationshipValue(sk StoreKey, relationshipIndex int) (hasLink bool, rv *RelationshipValue, err error) {
	response, err := tsc.RawCommand("follow", string(sk.Path), fmt.Sprintf("%d", relationshipIndex))
	if err != nil {
		return
	}

	hasLink = responseBool(response["has_link"])

	tokenPath, keyExists := response["key"].(string)
	if keyExists {
		rv = &RelationshipValue{}
		rvsk := StoreKey(treestore.MakeStoreKeyFromPath(treestore.TokenPath(tokenPath)))
		rv.Sk = rvsk

		valStr, valueExists := response["value"].(string)
		if valueExists {
			valType, _ := response["type"].(string)
			var v any
			if v, err = cmdlineToNativeValue(valStr, valType); err != nil {
				return
			}
			rv.CurrentValue = v
		}
	}
	return
}

// Navigates to the specified store key and returns all of the key segments
// matching the simple wildcard `pattern`. If the store key does not exist,
// the return `keys` will be nil.
//
// Memory is allocated up front to hold `limit` keys, so be careful to pass
// a reasonable limit.
func (tsc *tsClient) GetLevelKeys(sk StoreKey, pattern string, startAt, limit int) (keys []LevelKey, err error) {
	response, err := tsc.RawCommand("nodes", string(sk.Path), pattern, "--start", fmt.Sprintf("%d", startAt), "--limit", fmt.Sprintf("%d", limit), "--detailed")
	if err != nil {
		return
	}

	rawKeys, _ := response["keys"].([]any)
	keys = make([]LevelKey, 0, len(rawKeys))

	for _, rawKey := range rawKeys {
		key := rawKey.(map[string]any)
		segment := key["segment"].(string)
		hasValue := responseBool(key["has_value"])
		hasChildren := responseBool(key["has_children"])
		lk := LevelKey{
			Segment:     TokenSegment(UnescapeTokenString(segment)),
			HasValue:    hasValue,
			HasChildren: hasChildren,
		}

		keys = append(keys, lk)
	}
	return
}

// Full iteration function walks each tree store level according to skPattern and returns every
// detail of matching keys.
func (tsc *tsClient) GetMatchingKeys(skPattern StoreKey, startAt, limit int) (keys []*KeyMatch, err error) {
	response, err := tsc.RawCommand("lsk", string(skPattern.Path), "--start", fmt.Sprintf("%d", startAt), "--limit", fmt.Sprintf("%d", limit), "--detailed")
	if err != nil {
		return
	}

	rawKeys, _ := response["keys"].([]any)
	keys = make([]*KeyMatch, 0, len(rawKeys))

	for _, rawKey := range rawKeys {
		key := rawKey.(map[string]any)
		tokenPath := key["key"].(string)
		hasValue := responseBool(key["has_value"])
		hasChildren := responseBool(key["has_children"])
		valStr, vsExists := key["current_value"].(string)
		valType, _ := key["current_type"].(string)

		rawRelationships, relExists := key["relationships"].([]any)
		var relationships []StoreAddress
		if relExists {
			relationships = make([]StoreAddress, len(rawRelationships))
			for _, rel := range rawRelationships {
				relationships = append(relationships, rel.(StoreAddress))
			}
		}

		var metadata map[string]string
		rawMetadata, mdExists := key["metadata"].(map[string]any)
		if mdExists {
			metadata = make(map[string]string, len(rawMetadata))
			for k, v := range rawMetadata {
				metadata[k] = v.(string)
			}
		}

		km := &KeyMatch{
			Key:           TokenPath(tokenPath),
			Metadata:      metadata,
			HasValue:      hasValue,
			HasChildren:   hasChildren,
			Relationships: relationships,
		}
		if vsExists {
			var v any
			if v, err = cmdlineToNativeValue(valStr, valType); err != nil {
				return
			}
			km.CurrentValue = v
		}

		keys = append(keys, km)
	}
	return
}

// Full iteration function walks each tree store level according to skPattern and returns every
// detail of matching keys that have values.
func (tsc *tsClient) GetMatchingKeyValues(skPattern StoreKey, startAt, limit int) (values []*KeyValueMatch, err error) {
	response, err := tsc.RawCommand("lsv", string(skPattern.Path), "--start", fmt.Sprintf("%d", startAt), "--limit", fmt.Sprintf("%d", limit), "--detailed")
	if err != nil {
		return
	}

	rawValues, _ := response["values"].([]any)
	values = make([]*KeyValueMatch, 0, len(rawValues))

	for _, rawKey := range rawValues {
		value := rawKey.(map[string]any)
		tokenPath := value["key"].(string)
		hasChildren := responseBool(value["has_children"])
		valStr, vsExists := value["current_value"].(string)
		valType, _ := value["current_type"].(string)

		rawRelationships, relExists := value["relationships"].([]any)
		var relationships []StoreAddress
		if relExists {
			relationships = make([]StoreAddress, len(rawRelationships))
			for _, rel := range rawRelationships {
				relationships = append(relationships, rel.(StoreAddress))
			}
		}

		var metadata map[string]string
		rawMetadata, mdExists := value["metadata"].(map[string]any)
		if mdExists {
			metadata = make(map[string]string, len(rawMetadata))
			for k, v := range rawMetadata {
				metadata[k] = v.(string)
			}
		}

		kvm := &KeyValueMatch{
			Key:           TokenPath(tokenPath),
			Metadata:      metadata,
			HasChildren:   hasChildren,
			Relationships: relationships,
		}
		if vsExists {
			var v any
			if v, err = cmdlineToNativeValue(valStr, valType); err != nil {
				return
			}
			kvm.CurrentValue = v
		}

		values = append(values, kvm)
	}
	return
}

// Serialize the tree store into a single JSON doc.
//
// N.B., The document is constructed entirely in memory and will hold an
// exclusive lock during the operation.
func (tsc *tsClient) Export(sk StoreKey) (jsonData any, err error) {
	response, err := tsc.RawCommand("export", string(sk.Path))
	if err != nil {
		return
	}

	jsonData = response["data"]
	return
}

// Serialize the tree store into a single JSON doc.
//
// N.B., The document is constructed entirely in memory and will hold an
// exclusive lock during the operation.
//
// This variant provides the export data in a base64 encoded string.
func (tsc *tsClient) ExportBase64(sk StoreKey) (b64 string, err error) {
	response, err := tsc.RawCommand("export", string(sk.Path), "--base64")
	if err != nil {
		return
	}

	b64, _ = response["base64"].(string)
	return
}

// Creates a key from an export format json doc and adds it to the tree store
// at the specified sk. If the key exists, it and its children will be replaced.
func (tsc *tsClient) Import(sk StoreKey, jsonData any) (err error) {
	marshalled, err := json.Marshal(jsonData)
	if err != nil {
		return
	}

	_, err = tsc.RawCommand("import", string(sk.Path), string(marshalled))
	if err != nil {
		return
	}
	return
}

// Creates a key from an export format json doc and adds it to the tree store
// at the specified sk. If the key exists, it and its children will be replaced.
//
// This variant accepts the import data in a base64 encoded string.
func (tsc *tsClient) ImportBase64(sk StoreKey, b64 string) (err error) {
	_, err = tsc.RawCommand("import", string(sk.Path), b64, "--base64")
	if err != nil {
		return
	}
	return
}

// Retrieves the child key tree and leaf values in the form of json. If
// metadata "array" is "true" then the child key nodes are treated as
// array indicies. (They must be big endian uint32.)
func (tsc *tsClient) GetKeyAsJson(sk StoreKey, opt JsonOptions) (jsonData any, err error) {
	args := []string{"getjson", string(sk.Path)}
	if (opt & JsonStringValuesAsKeys) != 0 {
		args = append(args, "--straskey")
	}

	response, err := tsc.RawCommand(args...)
	if err != nil {
		return
	}

	jsonData = response["data"]
	return
}

// Retrieves the child key tree and leaf values in the form of json. If
// metadata "array" is "true" then the child key nodes are treated as
// array indicies. (They must be big endian uint32.)
//
// This variant provides the data in raw bytes, typically for an
// application to call json.Unmarshal on its own struct type.
func (tsc *tsClient) GetKeyAsJsonBytes(sk StoreKey, opt JsonOptions) (bytes []byte, err error) {
	args := []string{"getjson", string(sk.Path), "--base64"}
	if (opt & JsonStringValuesAsKeys) != 0 {
		args = append(args, "--straskey")
	}

	response, err := tsc.RawCommand(args...)
	if err != nil {
		return
	}

	b64, valid := response["base64"].(string)
	if !valid {
		err = errors.New("invalid getjson response")
		return
	}

	bytes, err = base64.StdEncoding.DecodeString(b64)
	return
}

// Retrieves the child key tree and leaf values in the form of json. If
// metadata "array" is "true" then the child key nodes are treated as
// array indicies. (They must be big endian uint32.)
//
// This variant provides the json data in a base64 encoded string.
func (tsc *tsClient) GetKeyAsJsonBase64(sk StoreKey, opt JsonOptions) (b64 string, err error) {
	args := []string{"getjson", string(sk.Path), "--base64"}
	if (opt & JsonStringValuesAsKeys) != 0 {
		args = append(args, "--straskey")
	}

	response, err := tsc.RawCommand(args...)
	if err != nil {
		return
	}

	b64, _ = response["base64"].(string)
	return
}

// Takes the generalized json data and stores it at the specified key path.
// If the sk exists, its value, children and history are deleted, and the new
// json data takes its place.
func (tsc *tsClient) SetKeyJson(sk StoreKey, jsonData any, opt JsonOptions) (replaced bool, address StoreAddress, err error) {
	marshalled, err := json.Marshal(jsonData)
	if err != nil {
		return
	}

	args := []string{"setjson", string(sk.Path), string(marshalled)}
	if (opt & JsonStringValuesAsKeys) != 0 {
		args = append(args, "--straskey")
	}

	response, err := tsc.RawCommand(args...)
	if err != nil {
		return
	}

	replaced, _ = response["replaced"].(bool)
	addrStr, exists := response["address"].(float64)
	if exists {
		address = responseAddress(addrStr)
	}
	return
}

// Takes the generalized json data and stores it at the specified key path.
// If the sk exists, its value, children and history are deleted, and the new
// json data takes its place.
//
// This variant accepts the json data in a base64 encoded string.
func (tsc *tsClient) SetKeyJsonBase64(sk StoreKey, b64 string, opt JsonOptions) (replaced bool, address StoreAddress, err error) {
	args := []string{"setjson", string(sk.Path), b64, "--base64"}
	if (opt & JsonStringValuesAsKeys) != 0 {
		args = append(args, "--straskey")
	}

	response, err := tsc.RawCommand(args...)
	if err != nil {
		return
	}

	replaced, _ = response["replaced"].(bool)
	addrStr, exists := response["address"].(float64)
	if exists {
		address = responseAddress(addrStr)
	}
	return
}

// Takes the generalized json data and stores it at the specified key path.
// If the sk exists, no changes are made. Otherwise a new key node is created
// with its child data set according to the json structure.
func (tsc *tsClient) CreateKeyJson(sk StoreKey, jsonData any, opt JsonOptions) (created bool, address StoreAddress, err error) {
	marshalled, err := json.Marshal(jsonData)
	if err != nil {
		return
	}

	args := []string{"createjson", string(sk.Path), string(marshalled)}
	if (opt & JsonStringValuesAsKeys) != 0 {
		args = append(args, "--straskey")
	}

	response, err := tsc.RawCommand(args...)
	if err != nil {
		return
	}

	addrStr, exists := response["address"].(float64)
	if exists {
		created = true
		address = responseAddress(addrStr)
	}
	return
}

// Takes the generalized json data and stores it at the specified key path.
// If the sk exists, no changes are made. Otherwise a new key node is created
// with its child data set according to the json structure.
//
// This variant accepts the json data in a base64 encoded string.
func (tsc *tsClient) CreateKeyJsonBase64(sk StoreKey, b64 string, opt JsonOptions) (created bool, address StoreAddress, err error) {
	args := []string{"createjson", string(sk.Path), b64, "--base64"}
	if (opt & JsonStringValuesAsKeys) != 0 {
		args = append(args, "--straskey")
	}

	response, err := tsc.RawCommand(args...)
	if err != nil {
		return
	}

	addrStr, exists := response["address"].(float64)
	if exists {
		created = true
		address = responseAddress(addrStr)
	}
	return
}

// Takes the generalized json data and stores it at the specified key path.
// If the sk doesn't exists, no changes are made. Otherwise the key node's
// value and children are deleted, and the new json data takes its place.
func (tsc *tsClient) ReplaceKeyJson(sk StoreKey, jsonData any, opt JsonOptions) (replaced bool, address StoreAddress, err error) {
	marshalled, err := json.Marshal(jsonData)
	if err != nil {
		return
	}

	args := []string{"replacejson", string(sk.Path), string(marshalled)}
	if (opt & JsonStringValuesAsKeys) != 0 {
		args = append(args, "--straskey")
	}

	response, err := tsc.RawCommand(args...)
	if err != nil {
		return
	}

	addrStr, exists := response["address"].(float64)
	if exists {
		replaced = true
		address = responseAddress(addrStr)
	}
	return
}

// Takes the generalized json data and stores it at the specified key path.
// If the sk doesn't exists, no changes are made. Otherwise the key node's
// value and children are deleted, and the new json data takes its place.
//
// This variant accepts the json data in a base64 encoded string.
func (tsc *tsClient) ReplaceKeyJsonBase64(sk StoreKey, b64 string, opt JsonOptions) (replaced bool, address StoreAddress, err error) {
	args := []string{"replacejson", string(sk.Path), b64, "--base64"}
	if (opt & JsonStringValuesAsKeys) != 0 {
		args = append(args, "--straskey")
	}

	response, err := tsc.RawCommand(args...)
	if err != nil {
		return
	}

	addrStr, exists := response["address"].(float64)
	if exists {
		replaced = true
		address = responseAddress(addrStr)
	}
	return
}

// Overlays json data on top of existing data. This is one of the slower APIs
// because each part of json is independently written to the store, and a
// write lock is required across the whole operation.
func (tsc *tsClient) MergeKeyJson(sk StoreKey, jsonData any, opt JsonOptions) (address StoreAddress, err error) {
	marshalled, err := json.Marshal(jsonData)
	if err != nil {
		return
	}

	args := []string{"mergejson", string(sk.Path), string(marshalled)}
	if (opt & JsonStringValuesAsKeys) != 0 {
		args = append(args, "--straskey")
	}

	response, err := tsc.RawCommand(args...)
	if err != nil {
		return
	}

	addrStr, exists := response["address"].(float64)
	if exists {
		address = responseAddress(addrStr)
	}
	return
}

// Overlays json data on top of existing data. This is one of the slower APIs
// because each part of json is independently written to the store, and a
// write lock is required across the whole operation.
//
// This variant accepts the json data in a base64 encoded string.
func (tsc *tsClient) MergeKeyJsonBase64(sk StoreKey, b64 string, opt JsonOptions) (address StoreAddress, err error) {
	args := []string{"mergejson", string(sk.Path), b64, "--base64"}
	if (opt & JsonStringValuesAsKeys) != 0 {
		args = append(args, "--straskey")
	}

	response, err := tsc.RawCommand(args...)
	if err != nil {
		return
	}

	addrStr, exists := response["address"].(float64)
	if exists {
		address = responseAddress(addrStr)
	}
	return
}

// Evaluate a math expression and store the result.
//
// The expression operators include + - / * & | ^ ** % >> <<,
// comparators >, <=, etc., and logical || &&.
//
// Constants are 64-bit floating point, string constants, dates or true/false.
//
// Parenthesis specify order of evaluation.
//
// Unary operators ! - ~ are supported.
//
// Ternary conditionals are supported with <expr> ? <on-true> : <on-false>
//
// Null coalescence is supported with ??
//
// Basic type conversion is supported - int(value), uint(value) and float(value)
//
// The target's store key original value is accessed with variable 'self'.
//
// The 'self' can also be referred to as 'i' for int, 'u' for uint or 'f' for float,
// for which if there are no other types specified, the result will be stored as
// the type specified. This is useful for compact, simple expressions such as:
//
//	"i+1"        increments existing int (or zero), stores result as int
//
// The operation is computed in 64-bit floating point before it is stored in its
// final type.
//
// String values can be converted in casts, e.g., int("-35")
//
// Other input keys can be accessed using the lookup(sk) function, where sk is the
// key path containing a value.
//
//	`lookup("/my/store/key")+25`
//
// If the initial slash is not specified, the store key path is a child of the
// target sk.
//
// For ternary conditionals, an operation can be skipped by using fail().
//
//	"i>100?i+1:fail()"        no modifications if the sk value is < 100
func (tsc *tsClient) CalculateKeyValue(sk StoreKey, expression string) (address StoreAddress, newValue any, err error) {
	response, err := tsc.RawCommand("calc", string(sk.Path), expression)
	if err != nil {
		return
	}

	address64, modified := response["address"].(float64)
	if modified {
		address = StoreAddress(address64)

		valStr, _ := response["value"].(string)
		valType, _ := response["type"].(string)

		if newValue, err = cmdlineToNativeValue(valStr, valType); err != nil {
			return
		}
	}
	return
}

// Moves a key tree to a new location, optionally overwriting an existing tree.
func (tsc *tsClient) MoveKey(srcSk StoreKey, destSk StoreKey, overwrite bool) (exists, moved bool, err error) {
	args := []string{"mv", string(srcSk.Path), string(destSk.Path)}
	if overwrite {
		args = append(args, "--overwrite")
	}
	response, err := tsc.RawCommand(args...)
	if err != nil {
		return
	}

	exists, _ = response["exists"].(bool)
	moved, _ = response["moved"].(bool)
	return
}

// This API is intended for an indexing scenario, where:
//
//   - A "source key" is staged with a temporary path, and with a short expiration
//   - The children of the source key are filled, usually with multiple steps
//   - When the source key is ready, it is moved to a "destination key" (its
//     permanent path), and the expiration is removed or set to a longer expiration.
//   - At the time of moving source to destination, separate "index keys" are
//     maintained atomically with a reference to the destination key.
//
// If the reference keys do not exist, they are created, and the destination
// address is placed in relationship index 0.
//
// If a ttl change is specified, it is applied to the destination key and the
// reference keys as well.
//
// If ttl == 0, expiration is cleared. If ttl > 0, it is the Unix nanosecond
// tick of key expiration. Specify -1 for ttl to retain the source key's expiration.
//
// N.B., the address of a child source node does not change when the parent
// key is moved. Also expiration is not altered for child keys.
//
// This move operation can be used to make a temporary key permanent, with
// overwrite false for create, or true for update. It can also be used for
// delete by making source and destination the same and specifying an already
// expired ttl.
func (tsc *tsClient) MoveReferencedKey(srcSk StoreKey, destSk StoreKey, overwrite bool, ttl *time.Time, refs []StoreKey) (exists, moved bool, err error) {
	args := []string{"mvref", string(srcSk.Path), string(destSk.Path)}
	if overwrite {
		args = append(args, "--overwrite")
	}
	if ttl != nil {
		args = append(args, "--ns", fmt.Sprintf("%d", ttl.UnixNano()))
	}
	for _, ref := range refs {
		args = append(args, "--ref", string(ref.Path))
	}

	response, err := tsc.RawCommand(args...)
	if err != nil {
		return
	}

	exists, _ = response["exists"].(bool)
	moved, _ = response["moved"].(bool)
	return
}
