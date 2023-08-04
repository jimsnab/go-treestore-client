package treestore_client

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/jimsnab/go-lane"
	"github.com/jimsnab/go-treestore"
)

type (
	tsClient struct {
		sync.Mutex
		l lane.Lane
		cxn         net.Conn
		hostAndPort string
		inbound []byte
	}
)

func NewTSClient(l lane.Lane) TSClient {
	tsc := &tsClient{
		l: l,
		hostAndPort: "localhost:6770",
	}

	return tsc
}

func (tsc *tsClient) SetServer(host string, port int) {
	tsc.Lock()
	defer tsc.Unlock()

	if tsc.cxn != nil {
		tsc.cxn.Close()
	}

	tsc.hostAndPort = fmt.Sprintf("%s:%d", host, port)
}

func (tsc *tsClient) apiCall(args ...string) (response map[string]any, err error) {
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

	buffer := make([]byte, 1024*8)

	for {
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

		tsc.l.Tracef("received %d bytes from client", len(tsc.inbound))

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
	response, err := tsc.apiCall("setv", string(sk.Path), valueEscape(value))
	if err != nil {
		return
	}

	address = responseAddress(response["address"])
	firstValue = responseBool(response["firstValue"])
	return
}

func (tsc *tsClient) SetKeyValueEx(sk StoreKey, value any, flags SetExFlags, expire *time.Time, relationships []StoreAddress) (address StoreAddress, exists bool, originalValue any, err error) {
	args := []string{"setex", string(sk.Path)}
	if flags & SetExNoValueUpdate == 0 {
		args = append(args, valueEscape(value))
	}

	if flags & SetExMustExist != 0 {
		args = append(args, "--mx")
	} else if flags & SetExMustNotExist != 0 {
		args = append(args, "--nx")
	}

	if expire != nil {
		args = append(args, fmt.Sprintf("%d", expire.UnixNano()))
	}

	if relationships != nil {
		var sb strings.Builder
		for idx,addr := range relationships {
			if idx > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(fmt.Sprintf("%d", addr))
		}
		args = append(args, sb.String())
	}

	response, err := tsc.apiCall(args...)
	if err != nil {
		return
	}

	address = responseAddress(response["address"])
	exists = responseBool(response["exists"])

	orgVal, hasOrgVal := response["orginal_value"].(string)
	if hasOrgVal {
		originalValue = valueUnescape(orgVal)
	}
	return

}

func (tsc *tsClient) IsKeyIndexed(sk StoreKey) (address StoreAddress, exists bool, err error) {
	response, err := tsc.apiCall("indexed", string(sk.Path))
	if err != nil {
		return
	}

	address = responseAddress(response["address"])
	exists = responseBool(response["exists"])
	return
}

func (tsc *tsClient) LocateKey(sk StoreKey) (address StoreAddress, exists bool, err error) {
	response, err := tsc.apiCall("getk", string(sk.Path))
	if err != nil {
		return
	}

	addrStr, hasAddr := response["address"].(string)
	if hasAddr {
		address = responseAddress(addrStr)
		exists = responseBool(hasAddr)
	}

	return
}

func (tsc *tsClient) GetKeyTtl(sk StoreKey) (ttl *time.Time, err error) {
	response, err := tsc.apiCall("ttlk", string(sk.Path))
	if err != nil {
		return
	}

	ttl = responseEpochNs(response["ttl"])
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
			value = valueUnescape(valStr)
		}
	}
	return
}

func (tsc *tsClient) GetKeyValueTtl(sk StoreKey) (ttl *time.Time, err error) {
	response, err := tsc.apiCall("ttlv", string(sk.Path))
	if err != nil {
		return
	}

	ttl = responseEpochNs(response["ttl"])
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
		value = valueUnescape(valStr)
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
		originalValue = valueUnescape(orgValStr)
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
		originalValue = valueUnescape(orgValStr)
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

	value, attributeExists = response["original_value"].(string)
	return
}

func (tsc *tsClient) GetMetadataAttributes(sk StoreKey) (attributes []string, err error) {
	response, err := tsc.apiCall("lsmeta", string(sk.Path))
	if err != nil {
		return
	}

	attributesAny, _ := response["original_value"].([]any)
	if attributesAny != nil {
		attributes = make([]string, 0, len(attributesAny))
		for _,attribute := range attributesAny {
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
			value = valueUnescape(valStr)
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
			rv.CurrentValue = valueUnescape(valStr)
		}
	}
	return
}

func (tsc *tsClient) GetLevelKeys(sk StoreKey, pattern string, startAt, limit int) (keys []LevelKey, count int, err error) {
	response, err := tsc.apiCall("nodes", string(sk.Path), pattern, "--start", fmt.Sprintf("%d", startAt), "--limit", fmt.Sprintf("%d", limit), "--detailed")
	if err != nil {
		return
	}

	rawKeys, _ := response["keys"].([]any)
	keys = make([]LevelKey, 0, len(rawKeys))

	for _,rawKey := range rawKeys {
		key := rawKey.(map[string]any)
		segment := key["segment"].(string)
		hasValue := responseBool(key["has_value"])
		hasChildren := responseBool(key["has_children"])
		lk := LevelKey{
			Segment: TokenSegment(segment),
			HasValue: hasValue,
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

	for _,rawKey := range rawKeys {
		key := rawKey.(map[string]any)
		tokenPath := key["key"].(string)
		hasValue := responseBool(key["has_value"])
		hasChildren := responseBool(key["has_children"])
		valStr, vsExists := key["current_value"].(string)

		rawRelationships, relExists := key["relationships"].([]any)
		var relationships []StoreAddress
		if relExists {
			relationships = make([]StoreAddress, len(rawRelationships))
			for _,rel := range rawRelationships {
				relationships = append(relationships, rel.(StoreAddress))
			}
		}

		var metadata map[string]string
		rawMetadata, mdExists := key["metadata"].(map[string]any)
		if mdExists {
			metadata = make(map[string]string, len(rawMetadata))
			for k,v := range rawMetadata {
				metadata[k] = v.(string)
			}
		}

		km := &KeyMatch{
			Key: TokenPath(tokenPath),
			Metadata: metadata,
			HasValue: hasValue,
			HasChildren: hasChildren,
			Relationships: relationships,
		}
		if vsExists {
			km.CurrentValue = valueUnescape(valStr)
		}

		keys = append(keys, km)
	}
	return
}

func (tsc *tsClient) GetMatchingKeyValues(skPattern StoreKey, startAt, limit int) (values []*KeyValueMatch, err error) {
	response, err := tsc.apiCall("lsk", string(skPattern.Path), "--start", fmt.Sprintf("%d", startAt), "--limit", fmt.Sprintf("%d", limit), "--detailed")
	if err != nil {
		return
	}

	rawValues, _ := response["values"].([]any)
	values = make([]*KeyValueMatch, 0, len(rawValues))

	for _,rawKey := range rawValues {
		value := rawKey.(map[string]any)
		tokenPath := value["key"].(string)
		hasChildren := responseBool(value["has_children"])
		valStr, vsExists := value["current_value"].(string)

		rawRelationships, relExists := value["relationships"].([]any)
		var relationships []StoreAddress
		if relExists {
			relationships = make([]StoreAddress, len(rawRelationships))
			for _,rel := range rawRelationships {
				relationships = append(relationships, rel.(StoreAddress))
			}
		}

		var metadata map[string]string
		rawMetadata, mdExists := value["metadata"].(map[string]any)
		if mdExists {
			metadata = make(map[string]string, len(rawMetadata))
			for k,v := range rawMetadata {
				metadata[k] = v.(string)
			}
		}

		kvm := &KeyValueMatch{
			Key: TokenPath(tokenPath),
			Metadata: metadata,
			HasChildren: hasChildren,
			Relationships: relationships,
		}
		if vsExists {
			kvm.CurrentValue = valueUnescape(valStr)
		}

		values = append(values, kvm)
	}
	return
}
