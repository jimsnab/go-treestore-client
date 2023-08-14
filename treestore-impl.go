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

func (tsc *tsClient) SetServer(host string, port int) {
	tsc.close()

	tsc.Lock()
	defer tsc.Unlock()
	tsc.hostAndPort = fmt.Sprintf("%s:%d", host, port)
}

func (tsc *tsClient) Close() (err error) {
	err = tsc.close()
	return
}

func (tsc *tsClient) apiCall(args ...string) (response map[string]any, err error) {
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

func (tsc *tsClient) SetKey(sk StoreKey) (address StoreAddress, exists bool, err error) {
	response, err := tsc.apiCall("setk", string(sk.Path))
	if err != nil {
		return
	}

	address = responseAddress(response["address"])
	exists = responseBool(response["exists"])
	return
}

func (tsc *tsClient) SetKeyValue(sk StoreKey, value any) (address StoreAddress, firstValue bool, err error) {
	val, valType, err := nativeValueToCmdline(value)
	if err != nil {
		return
	}

	args := []string{"setv", string(sk.Path), val}
	if valType != "" {
		args = append(args, "--value-type", valType)
	}

	response, err := tsc.apiCall(args...)
	if err != nil {
		return
	}

	address = responseAddress(response["address"])
	firstValue = responseBool(response["firstValue"])
	return
}

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

	response, err := tsc.apiCall(args...)
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

func (tsc *tsClient) IsKeyIndexed(sk StoreKey) (address StoreAddress, exists bool, err error) {
	response, err := tsc.apiCall("indexed", string(sk.Path))
	if err != nil {
		return
	}

	addrStr, exists := response["address"]
	address = responseAddress(addrStr)
	return
}

func (tsc *tsClient) LocateKey(sk StoreKey) (address StoreAddress, exists bool, err error) {
	response, err := tsc.apiCall("getk", string(sk.Path))
	if err != nil {
		return
	}

	addrStr, exists := response["address"].(float64)
	if exists {
		address = responseAddress(addrStr)
	}

	return
}

func (tsc *tsClient) GetKeyTtl(sk StoreKey) (ttl *time.Time, err error) {
	response, err := tsc.apiCall("ttlk", string(sk.Path))
	if err != nil {
		return
	}

	ttlStr, exists := response["ttl"].(string)
	if exists {
		ttl = responseEpochNs(ttlStr)
	}
	return
}

func (tsc *tsClient) SetKeyTtl(sk StoreKey, expiration *time.Time) (exists bool, err error) {
	response, err := tsc.apiCall("expirekns", string(sk.Path), requestEpochNs(expiration))
	if err != nil {
		return
	}

	exists = responseBool(response["exists"])
	return
}

func (tsc *tsClient) GetKeyValue(sk StoreKey) (value any, keyExists, valueExists bool, err error) {
	response, err := tsc.apiCall("getv", string(sk.Path))
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

func (tsc *tsClient) GetKeyValueTtl(sk StoreKey) (ttl *time.Time, err error) {
	response, err := tsc.apiCall("ttlv", string(sk.Path))
	if err != nil {
		return
	}

	ttlStr, exists := response["ttl"].(string)
	if exists {
		ttl = responseEpochNs(ttlStr)
	}
	return
}

func (tsc *tsClient) SetKeyValueTtl(sk StoreKey, expiration *time.Time) (exists bool, err error) {
	response, err := tsc.apiCall("expirevns", string(sk.Path), requestEpochNs(expiration))
	if err != nil {
		return
	}

	exists = responseBool(response["exists"])
	return
}

func (tsc *tsClient) GetKeyValueAtTime(sk StoreKey, when *time.Time) (value any, exists bool, err error) {
	response, err := tsc.apiCall("vat", string(sk.Path), requestEpochNs(when))
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

func (tsc *tsClient) DeleteKeyWithValue(sk StoreKey, clean bool) (removed bool, originalValue any, err error) {
	args := []string{"delv", string(sk.Path)}
	if clean {
		args = append(args, "--clean")
	}
	response, err := tsc.apiCall(args...)
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

func (tsc *tsClient) DeleteKey(sk StoreKey) (keyRemoved, valueRemoved bool, originalValue any, err error) {
	response, err := tsc.apiCall("delk", string(sk.Path))
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

func (tsc *tsClient) SetMetadataAttribute(sk StoreKey, attribute, value string) (keyExists bool, priorValue string, err error) {
	response, err := tsc.apiCall("setmeta", string(sk.Path), attribute, value)
	if err != nil {
		return
	}

	keyExists = responseBool(response["key_exists"])
	priorValue = response["prior_value"].(string)
	return
}

func (tsc *tsClient) ClearMetdataAttribute(sk StoreKey, attribute string) (attributeExists bool, originalValue string, err error) {
	response, err := tsc.apiCall("delmeta", string(sk.Path), attribute)
	if err != nil {
		return
	}

	originalValue, attributeExists = response["original_value"].(string)
	return
}

func (tsc *tsClient) ClearKeyMetdata(sk StoreKey) (err error) {
	_, err = tsc.apiCall("resetmeta", string(sk.Path))
	return
}

func (tsc *tsClient) GetMetadataAttribute(sk StoreKey, attribute string) (attributeExists bool, value string, err error) {
	response, err := tsc.apiCall("getmeta", string(sk.Path), attribute)
	if err != nil {
		return
	}

	value, attributeExists = response["value"].(string)
	return
}

func (tsc *tsClient) GetMetadataAttributes(sk StoreKey) (attributes []string, err error) {
	response, err := tsc.apiCall("lsmeta", string(sk.Path))
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

func (tsc *tsClient) KeyFromAddress(addr StoreAddress) (sk StoreKey, exists bool, err error) {
	response, err := tsc.apiCall("addrk", requestAddress(addr))
	if err != nil {
		return
	}

	tokenPath, exists := response["key"].(string)
	if exists {
		sk = StoreKey(treestore.MakeStoreKeyFromPath(treestore.TokenPath(tokenPath)))
	}
	return
}

func (tsc *tsClient) KeyValueFromAddress(addr StoreAddress) (keyExists, valueExists bool, sk StoreKey, value any, err error) {
	response, err := tsc.apiCall("addrv", requestAddress(addr))
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

func (tsc *tsClient) GetRelationshipValue(sk StoreKey, relationshipIndex int) (hasLink bool, rv *RelationshipValue, err error) {
	response, err := tsc.apiCall("follow", string(sk.Path), fmt.Sprintf("%d", relationshipIndex))
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

func (tsc *tsClient) GetLevelKeys(sk StoreKey, pattern string, startAt, limit int) (keys []LevelKey, err error) {
	response, err := tsc.apiCall("nodes", string(sk.Path), pattern, "--start", fmt.Sprintf("%d", startAt), "--limit", fmt.Sprintf("%d", limit), "--detailed")
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

func (tsc *tsClient) GetMatchingKeys(skPattern StoreKey, startAt, limit int) (keys []*KeyMatch, err error) {
	response, err := tsc.apiCall("lsk", string(skPattern.Path), "--start", fmt.Sprintf("%d", startAt), "--limit", fmt.Sprintf("%d", limit), "--detailed")
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

func (tsc *tsClient) GetMatchingKeyValues(skPattern StoreKey, startAt, limit int) (values []*KeyValueMatch, err error) {
	response, err := tsc.apiCall("lsv", string(skPattern.Path), "--start", fmt.Sprintf("%d", startAt), "--limit", fmt.Sprintf("%d", limit), "--detailed")
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

func (tsc *tsClient) Export(sk StoreKey) (jsonData any, err error) {
	response, err := tsc.apiCall("export", string(sk.Path))
	if err != nil {
		return
	}

	jsonData = response["data"]
	return
}

func (tsc *tsClient) ExportBase64(sk StoreKey) (b64 string, err error) {
	response, err := tsc.apiCall("export", string(sk.Path), "--base64")
	if err != nil {
		return
	}

	b64, _ = response["base64"].(string)
	return
}

func (tsc *tsClient) Import(sk StoreKey, jsonData any) (err error) {
	marshalled, err := json.Marshal(jsonData)
	if err != nil {
		return
	}

	_, err = tsc.apiCall("import", string(sk.Path), string(marshalled))
	if err != nil {
		return
	}
	return
}

func (tsc *tsClient) ImportBase64(sk StoreKey, b64 string) (err error) {
	_, err = tsc.apiCall("import", string(sk.Path), b64, "--base64")
	if err != nil {
		return
	}
	return
}

func (tsc *tsClient) GetKeyAsJson(sk StoreKey) (jsonData any, err error) {
	response, err := tsc.apiCall("getjson", string(sk.Path))
	if err != nil {
		return
	}

	jsonData = response["data"]
	return
}

func (tsc *tsClient) GetKeyAsJsonBytes(sk StoreKey) (bytes []byte, err error) {
	response, err := tsc.apiCall("getjson", string(sk.Path), "--base64")
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

func (tsc *tsClient) GetKeyAsJsonBase64(sk StoreKey) (b64 string, err error) {
	response, err := tsc.apiCall("getjson", string(sk.Path), "--base64")
	if err != nil {
		return
	}

	b64, _ = response["base64"].(string)
	return
}

func (tsc *tsClient) SetKeyJson(sk StoreKey, jsonData any) (replaced bool, err error) {
	marshalled, err := json.Marshal(jsonData)
	if err != nil {
		return
	}

	response, err := tsc.apiCall("setjson", string(sk.Path), string(marshalled))
	if err != nil {
		return
	}

	replaced, _ = response["replaced"].(bool)
	return
}

func (tsc *tsClient) SetKeyJsonBase64(sk StoreKey, b64 string) (replaced bool, err error) {
	response, err := tsc.apiCall("setjson", string(sk.Path), b64, "--base64")
	if err != nil {
		return
	}

	replaced, _ = response["replaced"].(bool)
	return
}

func (tsc *tsClient) CreateKeyJson(sk StoreKey, jsonData any) (created bool, err error) {
	marshalled, err := json.Marshal(jsonData)
	if err != nil {
		return
	}

	response, err := tsc.apiCall("createjson", string(sk.Path), string(marshalled))
	if err != nil {
		return
	}

	created, _ = response["created"].(bool)
	return
}

func (tsc *tsClient) CreateKeyJsonBase64(sk StoreKey, b64 string) (created bool, err error) {
	response, err := tsc.apiCall("createjson", string(sk.Path), b64, "--base64")
	if err != nil {
		return
	}

	created, _ = response["created"].(bool)
	return
}

func (tsc *tsClient) ReplaceKeyJson(sk StoreKey, jsonData any) (replaced bool, err error) {
	marshalled, err := json.Marshal(jsonData)
	if err != nil {
		return
	}

	response, err := tsc.apiCall("replacejson", string(sk.Path), string(marshalled))
	if err != nil {
		return
	}

	replaced, _ = response["replaced"].(bool)
	return
}

func (tsc *tsClient) ReplaceKeyJsonBase64(sk StoreKey, b64 string) (replaced bool, err error) {
	response, err := tsc.apiCall("replacejson", string(sk.Path), b64, "--base64")
	if err != nil {
		return
	}

	replaced, _ = response["replaced"].(bool)
	return
}

func (tsc *tsClient) MergeKeyJson(sk StoreKey, jsonData any) (err error) {
	marshalled, err := json.Marshal(jsonData)
	if err != nil {
		return
	}

	_, err = tsc.apiCall("mergejson", string(sk.Path), string(marshalled))
	if err != nil {
		return
	}

	return
}

func (tsc *tsClient) MergeKeyJsonBase64(sk StoreKey, b64 string) (err error) {
	_, err = tsc.apiCall("mergejson", string(sk.Path), b64, "--base64")
	if err != nil {
		return
	}

	return
}

func (tsc *tsClient) CalculateKeyValue(sk StoreKey, expression string) (address StoreAddress, modified bool, err error) {
	response, err := tsc.apiCall("calc", string(sk.Path), expression)
	if err != nil {
		return
	}

	address64, modified := response["address"].(float64)
	address = StoreAddress(address64)
	return
}
