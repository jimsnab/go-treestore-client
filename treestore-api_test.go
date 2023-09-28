package treestore_client

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jimsnab/go-lane"
	"github.com/jimsnab/go-treestore"
	tscmdsrv "github.com/jimsnab/go-treestore-cmdline"
)

func testSetup(t *testing.T) (l lane.Lane, tsc TSClient) {
	l = lane.NewTestingLane(context.Background())
	//l = lane.NewLogLaneWithCR(context.Background())
	srv := tscmdsrv.NewTreeStoreCmdLineServer(l)
	srv.StartServer("localhost", 6771, "")

	tsc = NewTSClient(l)
	tsc.SetServer("localhost", 6771)

	t.Cleanup(func() {
		srv.StopServer()
		srv.WaitForTermination()
		tsc.Close()
	})
	return
}

func TestSetGetK(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("client", "test", "key")

	addr, exists, err := tsc.SetKey(sk)
	if err != nil {
		t.Fatal(err)
	}
	if addr != 4 || exists {
		t.Error("initial key")
	}

	verifyAddr, located, err := tsc.LocateKey(sk)
	if err != nil {
		t.Fatal(err)
	}

	if verifyAddr != addr || !located {
		t.Error("verify")
	}
}

func TestSetGetKIfExists(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("client", "test", "key")
	testSk := MakeStoreKey("other")

	addr, exists, err := tsc.SetKeyIfExists(testSk, sk)
	if err != nil {
		t.Fatal(err)
	}
	if addr != 0 || exists {
		t.Error("test key missing")
	}

	tsc.SetKey(testSk)

	addr, exists, err = tsc.SetKeyIfExists(testSk, sk)
	if err != nil {
		t.Fatal(err)
	}
	if addr != 5 || exists {
		t.Error("initial key")
	}

	verifyAddr, located, err := tsc.LocateKey(sk)
	if err != nil {
		t.Fatal(err)
	}

	if verifyAddr != addr || !located {
		t.Error("verify")
	}
}

func TestSetGetV(t *testing.T) {
	_, tsc := testSetup(t)

	tick := fmt.Sprintf("%d", time.Now().UnixNano())

	_, _, err := tsc.SetKeyValue(MakeStoreKey("client", "test", "key"), tick)
	if err != nil {
		t.Fatal(err)
	}

	value, ke, vs, err := tsc.GetKeyValue(MakeStoreKey("client", "test", "key"))
	if err != nil {
		t.Fatal(err)
	}

	if value.(string) != tick {
		t.Error("not check")
	}
	if !ke || !vs {
		t.Error("wrong flags")
	}
}

func TestSetEx(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("client", "test", "key")

	addr, exists, orgVal, err := tsc.SetKeyValueEx(sk, nil, SetExNoValueUpdate, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if addr != 4 || exists || orgVal != nil {
		t.Error("first setex")
	}

	v1 := 100

	addr, exists, orgVal, err = tsc.SetKeyValueEx(sk, v1, 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if addr != 4 || !exists || orgVal != nil {
		t.Error("setex value 1")
	}

	verifyAddr, exists, err := tsc.IsKeyIndexed(sk)
	if err != nil {
		t.Fatal(err)
	}
	if verifyAddr != addr || !exists {
		t.Error("verify key is indexed")
	}

	v2 := 200

	addr, exists, orgVal, err = tsc.SetKeyValueEx(sk, v2, 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	if orgVal != v1 {
		t.Error("value bytes")
	}

	if addr != 4 || !exists || orgVal != 100 {
		t.Error("setex value 2")
	}

	addr, exists, orgVal, err = tsc.SetKeyValueEx(sk, nil, SetExMustNotExist|SetExNoValueUpdate, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	if addr != 0 || !exists || orgVal != 200 {
		t.Error("setex value 3")
	}

	addr, exists, orgVal, err = tsc.SetKeyValueEx(sk, nil, SetExMustExist, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	if addr != 4 || !exists || orgVal != 200 {
		t.Error("setex value 4")
	}

	now := time.Now().UTC()
	addr, exists, orgVal, err = tsc.SetKeyValueEx(sk, v1, 0, &now, nil)
	if err != nil {
		t.Fatal(err)
	}

	if addr != 4 || !exists || orgVal != nil {
		t.Error("setex value 5")
	}

	addr, exists, orgVal, err = tsc.SetKeyValueEx(sk, v2, 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	if addr != 5 || exists || orgVal != nil {
		t.Error("setex value 6")
	}

	sk2 := MakeStoreKey("test")

	addr2, exists, orgVal, err := tsc.SetKeyValueEx(sk2, nil, SetExNoValueUpdate, nil, []StoreAddress{addr})
	if err != nil {
		t.Fatal(err)
	}

	if addr2 != 6 || exists || orgVal != nil {
		t.Error("setex value 7")
	}

	hasLink, rv, err := tsc.GetRelationshipValue(sk2, 0)
	if err != nil {
		t.Fatal(err)
	}

	if !hasLink || rv == nil || rv.CurrentValue != 200 || rv.Sk.Path != "/client/test/key" {
		t.Error("relationship")
	}

	hasLink, rv, err = tsc.GetRelationshipValue(sk2, 1)
	if err != nil {
		t.Fatal(err)
	}

	if hasLink || rv != nil {
		t.Error("nil relationship")
	}
}

func TestSetKeyNoValueRelationship(t *testing.T) {
	_, tsc := testSetup(t)

	sk1 := MakeStoreKey("a")
	sk2 := MakeStoreKey("b")

	addr1, _, err := tsc.SetKey(sk1)
	if err != nil {
		t.Fatal(err)
	}
	if addr1 != 2 {
		t.Error("set key")
	}

	addr2, _, _, err := tsc.SetKeyValueEx(sk2, nil, SetExNoValueUpdate, nil, []StoreAddress{addr1})
	if err != nil {
		t.Fatal(err)
	}
	if addr2 != 3 {
		t.Error("set key 2")
	}

	hasLink, rv, err := tsc.GetRelationshipValue(sk2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !hasLink || rv == nil || rv.Sk.Path != "/a" {
		t.Error("key link")
	}

	if rv.CurrentValue != nil {
		t.Error("value nil")
	}
}

func TestSetGetKeyTtl(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("x")

	addr, exists, err := tsc.SetKey(sk)
	if err != nil {
		t.Fatal(err)
	}
	if addr != 2 || exists {
		t.Error("initial key")
	}

	now := time.Now().UTC().Add(time.Hour)
	exists, err = tsc.SetKeyTtl(sk, &now)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Error("set long ttl")
	}

	ttl, err := tsc.GetKeyTtl(sk)
	if err != nil {
		t.Fatal(err)
	}
	if ttl.UnixNano() != now.UnixNano() {
		t.Error("verify long ttl")
	}

	verifyAddr, located, err := tsc.LocateKey(sk)
	if err != nil {
		t.Fatal(err)
	}

	if verifyAddr != addr || !located {
		t.Error("verify exists")
	}

	now = time.Now().UTC()
	exists, err = tsc.SetKeyTtl(sk, &now)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Error("set expired ttl")
	}

	ttl, err = tsc.GetKeyTtl(sk)
	if err != nil {
		t.Fatal(err)
	}
	if ttl != nil {
		t.Error("verify expired ttl")
	}

	verifyAddr, located, err = tsc.LocateKey(sk)
	if err != nil {
		t.Fatal(err)
	}

	if verifyAddr != 0 || located {
		t.Error("verify expired")
	}
}

func TestSetGetKeyValueTtl(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("base", "data")
	sk2 := MakeStoreKey("base")

	addr, firstValue, err := tsc.SetKeyValue(sk, ValueEncode(400))
	if err != nil {
		t.Fatal(err)
	}
	if addr != 3 || !firstValue {
		t.Error("initial key")
	}

	now := time.Now().UTC().Add(time.Hour)
	exists, err := tsc.SetKeyValueTtl(sk, &now)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Error("set long ttl")
	}

	exists, err = tsc.SetKeyValueTtl(sk2, &now)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Error("set long ttl on base")
	}

	ttl, err := tsc.GetKeyValueTtl(sk)
	if err != nil {
		t.Fatal(err)
	}
	if ttl.UnixNano() != now.UnixNano() {
		t.Error("verify long ttl")
	}

	ttl, err = tsc.GetKeyValueTtl(sk2)
	if err != nil {
		t.Fatal(err)
	}
	if ttl != nil {
		t.Error("verify long ttl on base")
	}

	verifyAddr, located, err := tsc.LocateKey(sk)
	if err != nil {
		t.Fatal(err)
	}

	if verifyAddr != addr || !located {
		t.Error("verify exists")
	}

	now = time.Now().UTC()
	exists, err = tsc.SetKeyValueTtl(sk, &now)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Error("set expired ttl")
	}

	ttl, err = tsc.GetKeyValueTtl(sk)
	if err != nil {
		t.Fatal(err)
	}
	if ttl != nil {
		t.Error("verify expired ttl")
	}

	verifyAddr, located, err = tsc.LocateKey(sk)
	if err != nil {
		t.Fatal(err)
	}

	if verifyAddr != 0 || located {
		t.Error("verify expired")
	}
}

func TestHistory(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("test")

	addr, firstValue, err := tsc.SetKeyValue(sk, []byte("test"))
	if err != nil {
		t.Fatal(err)
	}
	if addr != 2 || !firstValue {
		t.Error("initial key")
	}

	now := time.Now().UTC()

	_, _, err = tsc.SetKeyValue(sk, []byte("done"))
	if err != nil {
		t.Fatal(err)
	}

	value, exists, err := tsc.GetKeyValueAtTime(sk, &now)
	if err != nil {
		t.Fatal(err)
	}

	if !exists || !bytes.Equal([]byte("test"), value.([]byte)) {
		t.Error("first value")
	}
}

func TestSetDelK(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("client", "test", "key")

	addr, exists, err := tsc.SetKey(sk)
	if err != nil {
		t.Fatal(err)
	}
	if addr != 4 || exists {
		t.Error("initial key")
	}

	kr, vr, ov, err := tsc.DeleteKey(sk)
	if err != nil {
		t.Fatal(err)
	}
	if !kr || vr || ov != nil {
		t.Error("remove it")
	}

	kr, vr, ov, err = tsc.DeleteKey(sk)
	if err != nil {
		t.Fatal(err)
	}
	if kr || vr || ov != nil {
		t.Error("remove it again")
	}

	verifyAddr, located, err := tsc.LocateKey(sk)
	if err != nil {
		t.Fatal(err)
	}

	if verifyAddr != 0 || located {
		t.Error("verify")
	}
}

func TestSetDelTree(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("client", "test", "key")

	addr, exists, err := tsc.SetKey(sk)
	if err != nil {
		t.Fatal(err)
	}
	if addr != 4 || exists {
		t.Error("initial key")
	}

	removed, err := tsc.DeleteKeyTree(sk)
	if err != nil {
		t.Fatal(err)
	}
	if !removed {
		t.Error("remove it")
	}

	removed, err = tsc.DeleteKeyTree(sk)
	if err != nil {
		t.Fatal(err)
	}
	if removed {
		t.Error("remove it again")
	}

	verifyAddr, located, err := tsc.LocateKey(sk)
	if err != nil {
		t.Fatal(err)
	}

	if verifyAddr != 0 || located {
		t.Error("verify")
	}
}

func TestSetDelKV(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("client", "test", "key")

	addr, firstValue, err := tsc.SetKeyValue(sk, []byte("test"))
	if err != nil {
		t.Fatal(err)
	}
	if addr != 4 || !firstValue {
		t.Error("initial key")
	}

	kr, vr, ov, err := tsc.DeleteKey(sk)
	if err != nil {
		t.Fatal(err)
	}
	if !kr || !vr || !bytes.Equal(ov.([]byte), []byte("test")) {
		t.Error("remove it")
	}

	kr, vr, ov, err = tsc.DeleteKey(sk)
	if err != nil {
		t.Fatal(err)
	}
	if kr || vr || ov != nil {
		t.Error("remove it again")
	}

	verifyAddr, located, err := tsc.LocateKey(sk)
	if err != nil {
		t.Fatal(err)
	}

	if verifyAddr != 0 || located {
		t.Error("verify")
	}
}

func TestSetDelV(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("client", "test", "key")

	addr, firstValue, err := tsc.SetKeyValue(sk, []byte("test"))
	if err != nil {
		t.Fatal(err)
	}
	if addr != 4 || !firstValue {
		t.Error("initial key")
	}

	removed, ov, err := tsc.DeleteKeyWithValue(sk, true)
	if err != nil {
		t.Fatal(err)
	}
	if !removed || !bytes.Equal(ov.([]byte), []byte("test")) {
		t.Error("remove it")
	}

	removed, ov, err = tsc.DeleteKeyWithValue(sk, true)
	if err != nil {
		t.Fatal(err)
	}
	if removed || ov != nil {
		t.Error("remove it again")
	}

	verifyAddr, located, err := tsc.LocateKey(MakeStoreKey("client"))
	if err != nil {
		t.Fatal(err)
	}

	if verifyAddr != 0 || located {
		t.Error("verify")
	}
}

func TestMetadata(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey()

	exists, pv, err := tsc.SetMetadataAttribute(sk, "version", "1")
	if err != nil {
		t.Fatal(err)
	}
	if !exists || pv != "" {
		t.Error("initial attrib")
	}

	exists, pv, err = tsc.SetMetadataAttribute(MakeStoreKey("missing"), "version", "1")
	if err != nil {
		t.Fatal(err)
	}
	if exists || pv != "" {
		t.Error("missing attrib")
	}

	exists, pv, err = tsc.SetMetadataAttribute(sk, "version", "2")
	if err != nil {
		t.Fatal(err)
	}
	if !exists || pv != "1" {
		t.Error("attrb update")
	}

	err = tsc.ClearKeyMetadata(sk)
	if err != nil {
		t.Fatal(err)
	}

	exists, pv, err = tsc.GetMetadataAttribute(sk, "version")
	if err != nil {
		t.Fatal(err)
	}
	if exists || pv != "" {
		t.Error("get after reset")
	}

	exists, pv, err = tsc.SetMetadataAttribute(sk, "empty", "")
	if err != nil {
		t.Fatal(err)
	}
	if !exists || pv != "" {
		t.Error("empty set")
	}

	exists, pv, err = tsc.GetMetadataAttribute(sk, "empty")
	if err != nil {
		t.Fatal(err)
	}
	if !exists || pv != "" {
		t.Error("get after empty set")
	}

	exists, pv, err = tsc.ClearMetadataAttribute(sk, "empty")
	if err != nil {
		t.Fatal(err)
	}
	if !exists || pv != "" {
		t.Error("clear empty")
	}

	exists, pv, err = tsc.ClearMetadataAttribute(sk, "empty")
	if err != nil {
		t.Fatal(err)
	}
	if exists || pv != "" {
		t.Error("clear empty again")
	}

	tsc.SetMetadataAttribute(sk, "one", "1")
	tsc.SetMetadataAttribute(sk, "two", "2")
	attribs, err := tsc.GetMetadataAttributes(sk)
	if err != nil {
		t.Fatal(err)
	}
	if len(attribs) != 2 {
		t.Error("ls attribs")
	}

	attribs, err = tsc.GetMetadataAttributes(MakeStoreKey("missing"))
	if err != nil {
		t.Fatal(err)
	}
	if attribs != nil {
		t.Error("ls attribs missing")
	}
}

func TestAddrLookup(t *testing.T) {
	_, tsc := testSetup(t)

	sk, exists, err := tsc.KeyFromAddress(0)
	if err != nil {
		t.Fatal(err)
	}
	if exists || sk.Path != "" || len(sk.Tokens) != 0 {
		t.Error("no address")
	}

	sk, exists, err = tsc.KeyFromAddress(1)
	if err != nil {
		t.Fatal(err)
	}
	if !exists || sk.Path != "" || len(sk.Tokens) != 0 {
		t.Error("sentinel address")
	}

	boo := MakeStoreKey("boo")

	addr, _, err := tsc.SetKey(boo)
	if err != nil {
		t.Fatal(err)
	}

	sk, exists, err = tsc.KeyFromAddress(addr)
	if err != nil {
		t.Fatal(err)
	}
	if !exists || sk.Path != "/boo" || len(sk.Tokens) != 1 {
		t.Error("key address")
	}
}

func TestAddrValueLookup(t *testing.T) {
	_, tsc := testSetup(t)

	ke, ve, sk, val, err := tsc.KeyValueFromAddress(0)
	if err != nil {
		t.Fatal(err)
	}
	if ke || ve || sk.Path != "" || len(sk.Tokens) != 0 || val != nil {
		t.Error("no address")
	}

	ke, ve, sk, val, err = tsc.KeyValueFromAddress(1)
	if err != nil {
		t.Fatal(err)
	}
	if !ke || ve || sk.Path != "" || len(sk.Tokens) != 0 || val != nil {
		t.Error("sentinel address")
	}

	boo := MakeStoreKey("boo")

	addr, _, err := tsc.SetKey(boo)
	if err != nil {
		t.Fatal(err)
	}

	ke, ve, sk, val, err = tsc.KeyValueFromAddress(addr)
	if err != nil {
		t.Fatal(err)
	}
	if !ke || ve || sk.Path != "/boo" || len(sk.Tokens) != 1 || val != nil {
		t.Error("verify boo")
	}

	foo := MakeStoreKey("foo")

	addr2, _, err := tsc.SetKeyValue(foo, []byte("test"))
	if err != nil {
		t.Fatal(err)
	}

	ke, ve, sk, val, err = tsc.KeyValueFromAddress(addr2)
	if err != nil {
		t.Fatal(err)
	}
	if !ke || !ve || sk.Path != "/foo" || len(sk.Tokens) != 1 || !bytes.Equal(val.([]byte), []byte("test")) {
		t.Error("verify foo")
	}
}

func TestLevelKeys(t *testing.T) {
	_, tsc := testSetup(t)

	tsc.SetKey(MakeStoreKey("cat"))
	tsc.SetKey(MakeStoreKey("dog/s"))
	tsc.SetKey(MakeStoreKey("mouse"))

	keys, err := tsc.GetLevelKeys(MakeStoreKey(), "*o*", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || !bytes.Equal(keys[0].Segment, TokenSegment("mouse")) || keys[0].HasChildren || keys[0].HasValue {
		t.Error("start")
	}

	keys, err = tsc.GetLevelKeys(MakeStoreKey(), "*o*", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || !bytes.Equal(keys[0].Segment, TokenSegment("dog/s")) || keys[0].HasChildren || keys[0].HasValue || !bytes.Equal(keys[1].Segment, TokenSegment("mouse")) || keys[1].HasChildren || keys[1].HasValue {
		t.Error("level pattern")
	}

	keys, err = tsc.GetLevelKeys(MakeStoreKey(), "*o*", 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || !bytes.Equal(keys[0].Segment, TokenSegment("dog/s")) || keys[0].HasChildren || keys[0].HasValue {
		t.Error("limit")
	}
}

func TestMatchingKeys(t *testing.T) {
	_, tsc := testSetup(t)

	tsc.SetKey(MakeStoreKey("cat"))
	tsc.SetKey(MakeStoreKey("dog/s"))
	tsc.SetKey(MakeStoreKey("mouse"))

	keys, err := tsc.GetMatchingKeys(MakeStoreKey("*o*"), 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || keys[0].Key != `/dog\ss` || keys[0].HasChildren || keys[0].HasValue || keys[1].Key != `/mouse` || keys[1].HasChildren || keys[1].HasValue {
		t.Error("match pattern")
	}

	keys, err = tsc.GetMatchingKeys(MakeStoreKey("*o*"), 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0].Key != `/dog\ss` || keys[0].HasChildren || keys[0].HasValue {
		t.Error("limit")
	}

	keys, err = tsc.GetMatchingKeys(MakeStoreKey("*o*"), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0].Key != `/mouse` || keys[0].HasChildren || keys[0].HasValue {
		t.Error("start")
	}
}

func TestMatchingValues(t *testing.T) {
	_, tsc := testSetup(t)

	tsc.SetKeyValue(MakeStoreKey("cat"), "1")
	tsc.SetKeyValue(MakeStoreKey("dog/s"), "2")
	tsc.SetKeyValue(MakeStoreKey("mouse"), "3")

	values, err := tsc.GetMatchingKeyValues(MakeStoreKey("*o*"), 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 || values[0].Key != `/dog\ss` || values[0].HasChildren || values[0].CurrentValue != "2" ||
		values[1].Key != `/mouse` || values[1].HasChildren || values[1].CurrentValue != "3" {
		t.Error("match pattern")
	}

	values, err = tsc.GetMatchingKeyValues(MakeStoreKey("*o*"), 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || values[0].Key != `/dog\ss` || values[0].HasChildren || values[0].CurrentValue != "2" {
		t.Error("limit")
	}

	values, err = tsc.GetMatchingKeyValues(MakeStoreKey("*o*"), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || values[0].Key != `/mouse` || values[0].HasChildren || values[0].CurrentValue != "3" {
		t.Error("start")
	}
}

func TestImportExportPlain(t *testing.T) {
	_, tsc := testSetup(t)

	jsonData := map[string]any{
		"children": map[string]any{
			"key": map[string]any{
				"history": []any{
					map[string]any{
						"timestamp": int64(0),
						"type":      "int",
						"value":     "123",
					},
				},
			},
		},
	}

	err := tsc.Import(MakeStoreKey(), jsonData)
	if err != nil {
		t.Fatal(err)
	}

	jsonData2, err := tsc.Export(MakeStoreKey())
	if err != nil {
		t.Fatal(err)
	}

	exported := jsonData2.(map[string]any)

	children, _ := exported["children"].(map[string]any)
	if children == nil {
		t.Fatal("children")
	}
	keys, _ := children["key"].(map[string]any)
	if keys == nil {
		t.Fatal("keys")
	}
	history, _ := keys["history"].([]any)
	if len(history) != 1 {
		t.Fatal("history")
	}
	value, _ := history[0].(map[string]any)
	if value == nil {
		t.Fatal("value")
	}
	vt, _ := value["type"].(string)
	if vt != "int" {
		t.Fatal("value type")
	}
	val, _ := strconv.ParseInt(value["value"].(string), 10, 64)
	if val != 123 {
		t.Fatal("int val")
	}
}

func TestImportExportBase64(t *testing.T) {
	_, tsc := testSetup(t)

	err := tsc.ImportBase64(MakeStoreKey(), "eyJjaGlsZHJlbiI6eyJ0ZXN0Ijp7ImNoaWxkcmVuIjp7ImNhdCI6eyJjaGlsZHJlbiI6eyJtZW93Ijp7fX19LCJkb2ciOnsiY2hpbGRyZW4iOnsiYmFyayI6e319fX19fX0=")
	if err != nil {
		t.Fatal(err)
	}

	b64, err := tsc.ExportBase64(MakeStoreKey())
	if err != nil {
		t.Fatal(err)
	}

	if b64 != "eyJjaGlsZHJlbiI6eyJ0ZXN0Ijp7ImNoaWxkcmVuIjp7ImNhdCI6eyJjaGlsZHJlbiI6eyJtZW93Ijp7fX19LCJkb2ciOnsiY2hpbGRyZW4iOnsiYmFyayI6e319fX19fX0=" {
		t.Error("round trip verify")
	}
}

func doesJsonMatch(t *testing.T, context string, data1, data2 any) {
	v1, err := json.Marshal(data1)
	if err != nil {
		t.Fatal(err.Error())
	}
	v2, err := json.Marshal(data2)
	if err != nil {
		t.Fatal(err.Error())
	}

	if !bytes.Equal(v1, v2) {
		t.Errorf("%s json mismatch", context)
	}
}

func TestJsonSet(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("test")

	jsonText := []byte(`{"animals": {"cat": {"sound": "meow"}, "dog": {"sound": "bark", "breeds": 360}}}`)

	var jsonData any
	err := json.Unmarshal(jsonText, &jsonData)
	if err != nil {
		t.Fatal(err.Error())
	}

	replaced, addr, err := tsc.SetKeyJson(sk, jsonData, 0)
	if replaced || addr == 0 || err != nil {
		t.Error("set json")
	}

	data, err := tsc.GetKeyAsJson(sk, 0)
	if data == nil || err != nil {
		t.Error("get json")
	}

	doesJsonMatch(t, "set", jsonData, data)
}

func TestJsonSetStrAsKey(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("test")

	jsonText := []byte(`{"animals": {"cat": {"sound": "meow"}, "dog": {"sound": "bark", "breeds": 360}}}`)

	var jsonData any
	err := json.Unmarshal(jsonText, &jsonData)
	if err != nil {
		t.Fatal(err.Error())
	}

	replaced, addr, err := tsc.SetKeyJson(sk, jsonData, JsonStringValuesAsKeys)
	if replaced || addr == 0 || err != nil {
		t.Error("set json")
	}

	data, err := tsc.GetKeyAsJson(sk, JsonStringValuesAsKeys)
	if data == nil || err != nil {
		t.Error("get json")
	}

	doesJsonMatch(t, "set", jsonData, data)
}

func TestJsonSetBytes(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("test")

	jsonText := []byte(`{"animals": {"cat": {"sound": "meow"}, "dog": {"sound": "bark", "breeds": 360}}}`)
	jsonDataB64 := base64.StdEncoding.EncodeToString(jsonText)

	replaced, addr, err := tsc.SetKeyJsonBase64(sk, jsonDataB64, 0)
	if replaced || addr == 0 || err != nil {
		t.Error("set json")
	}

	data, err := tsc.GetKeyAsJsonBytes(sk, 0)
	if err != nil {
		t.Error("get json")
	}

	var m map[string]any
	err = json.Unmarshal(data, &m)
	if err != nil {
		t.Error("unmarshal")
	}

	var jsonData map[string]any
	err = json.Unmarshal(jsonText, &jsonData)
	if err != nil {
		t.Fatal("test data unmarshal")
	}

	doesJsonMatch(t, "set", jsonData, m)
}

func TestJsonSetBytesStrAsKey(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("test")

	jsonText := []byte(`{"animals": {"cat": {"sound": "meow"}, "dog": {"sound": "bark", "breeds": 360}}}`)
	jsonDataB64 := base64.StdEncoding.EncodeToString(jsonText)

	replaced, addr, err := tsc.SetKeyJsonBase64(sk, jsonDataB64, JsonStringValuesAsKeys)
	if replaced || addr == 0 || err != nil {
		t.Error("set json")
	}

	data, err := tsc.GetKeyAsJsonBytes(sk, JsonStringValuesAsKeys)
	if err != nil {
		t.Error("get json")
	}

	var m map[string]any
	err = json.Unmarshal(data, &m)
	if err != nil {
		t.Error("unmarshal")
	}

	var jsonData map[string]any
	err = json.Unmarshal(jsonText, &jsonData)
	if err != nil {
		t.Fatal("test data unmarshal")
	}

	doesJsonMatch(t, "set", jsonData, m)
}

func TestJsonStage(t *testing.T) {
	_, tsc := testSetup(t)

	stagingSk := MakeStoreKey("staging")

	jsonText := []byte(`{"animals": {"cat": {"sound": "meow"}, "dog": {"sound": "bark", "breeds": 360}}}`)

	var jsonData any
	err := json.Unmarshal(jsonText, &jsonData)
	if err != nil {
		t.Fatal(err.Error())
	}

	tempSk, addr, err := tsc.StageKeyJson(stagingSk, jsonData, 0)
	expectedKey := fmt.Sprintf("%s/%d", stagingSk.Path, addr)
	if tempSk.Path != treestore.TokenPath(expectedKey) || err != nil {
		t.Error("stage json")
	}

	data, err := tsc.GetKeyAsJson(tempSk, 0)
	if data == nil || err != nil {
		t.Error("get staged json")
	}

	doesJsonMatch(t, "staged", jsonData, data)
}

func TestJsonStageStrAsKey(t *testing.T) {
	_, tsc := testSetup(t)

	stagingSk := MakeStoreKey("test")

	jsonText := []byte(`{"animals": {"cat": {"sound": "meow"}, "dog": {"sound": "bark", "breeds": 360}}}`)

	var jsonData any
	err := json.Unmarshal(jsonText, &jsonData)
	if err != nil {
		t.Fatal(err.Error())
	}

	tempSk, addr, err := tsc.StageKeyJson(stagingSk, jsonData, JsonStringValuesAsKeys)
	expectedKey := fmt.Sprintf("%s/%d", stagingSk.Path, addr)
	if tempSk.Path != treestore.TokenPath(expectedKey) || err != nil {
		t.Error("stage json")
	}

	ttl, err := tsc.GetKeyTtl(AppendStoreKeySegmentStrings(tempSk, "animals", "cat", "sound", "meow"))
	if ttl == nil || ttl.UnixNano() != 0 || err != nil {
		t.Error("verify string is key")
	}

	data, err := tsc.GetKeyAsJson(tempSk, JsonStringValuesAsKeys)
	if data == nil || err != nil {
		t.Error("get staged json")
	}

	doesJsonMatch(t, "staged", jsonData, data)
}

func TestJsonStageBytes(t *testing.T) {
	_, tsc := testSetup(t)

	stagingSk := MakeStoreKey("test")

	jsonText := []byte(`{"animals": {"cat": {"sound": "meow"}, "dog": {"sound": "bark", "breeds": 360}}}`)
	jsonDataB64 := base64.StdEncoding.EncodeToString(jsonText)

	tempSk, addr, err := tsc.StageKeyJsonBase64(stagingSk, jsonDataB64, 0)
	expectedKey := fmt.Sprintf("%s/%d", stagingSk.Path, addr)
	if tempSk.Path != treestore.TokenPath(expectedKey) || err != nil {
		t.Error("stage json")
	}

	data, err := tsc.GetKeyAsJsonBytes(tempSk, 0)
	if data == nil || err != nil {
		t.Error("get staged json")
	}

	var m map[string]any
	err = json.Unmarshal(data, &m)
	if err != nil {
		t.Error("unmarshal")
	}

	var jsonData map[string]any
	err = json.Unmarshal(jsonText, &jsonData)
	if err != nil {
		t.Fatal("test data unmarshal")
	}

	doesJsonMatch(t, "staging", jsonData, m)
}

func TestJsonStageBytesStrAsKey(t *testing.T) {
	_, tsc := testSetup(t)

	stagingSk := MakeStoreKey("test")

	jsonText := []byte(`{"animals": {"cat": {"sound": "meow"}, "dog": {"sound": "bark", "breeds": 360}}}`)
	jsonDataB64 := base64.StdEncoding.EncodeToString(jsonText)

	tempSk, addr, err := tsc.StageKeyJsonBase64(stagingSk, jsonDataB64, JsonStringValuesAsKeys)
	expectedKey := fmt.Sprintf("%s/%d", stagingSk.Path, addr)
	if tempSk.Path != treestore.TokenPath(expectedKey) || err != nil {
		t.Error("stage json")
	}

	ttl, err := tsc.GetKeyTtl(AppendStoreKeySegmentStrings(tempSk, "animals", "cat", "sound", "meow"))
	if ttl == nil || ttl.UnixNano() != 0 || err != nil {
		t.Error("verify string is key")
	}

	data, err := tsc.GetKeyAsJsonBytes(tempSk, JsonStringValuesAsKeys)
	if data == nil || err != nil {
		t.Error("get staged json")
	}

	var m map[string]any
	err = json.Unmarshal(data, &m)
	if err != nil {
		t.Error("unmarshal")
	}

	var jsonData map[string]any
	err = json.Unmarshal(jsonText, &jsonData)
	if err != nil {
		t.Fatal("test data unmarshal")
	}

	doesJsonMatch(t, "staging", jsonData, m)
}

func TestJsonGetMissing(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("test")

	data, err := tsc.GetKeyAsJson(sk, 0)
	if err != nil {
		t.Error("get json")
	}

	if data != nil {
		t.Error("data should be nil")
	}
}

func TestJsonGetBytesMissing(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("test")

	data, err := tsc.GetKeyAsJsonBytes(sk, 0)
	if err != nil {
		t.Error("get json")
	}

	if data == nil {
		t.Error("data should not be nil")
	}

	var jsonData any
	err = json.Unmarshal(data, &jsonData)
	if err != nil {
		t.Fatal("test data unmarshal")
	}

	doesJsonMatch(t, "get bytes", jsonData, nil)
}

func TestJsonSetBase64(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("test")

	jsonText := []byte(`{"animals": {"cat": {"sound": "meow"}, "dog": {"sound": "bark", "breeds": 360}}}`)
	jsonDataB64 := base64.StdEncoding.EncodeToString(jsonText)

	replaced, addr, err := tsc.SetKeyJsonBase64(sk, jsonDataB64, 0)
	if replaced || addr == 0 || err != nil {
		t.Error("set json")
	}

	data, err := tsc.GetKeyAsJsonBase64(sk, 0)
	if data != "eyJhbmltYWxzIjp7ImNhdCI6eyJzb3VuZCI6Im1lb3cifSwiZG9nIjp7ImJyZWVkcyI6MzYwLCJzb3VuZCI6ImJhcmsifX19" || err != nil {
		t.Error("get json")
	}
}

func TestJsonSetBase64StrAsKey(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("test")

	jsonText := []byte(`{"animals": {"cat": {"sound": "meow"}, "dog": {"sound": "bark", "breeds": 360}}}`)
	jsonDataB64 := base64.StdEncoding.EncodeToString(jsonText)

	replaced, addr, err := tsc.SetKeyJsonBase64(sk, jsonDataB64, JsonStringValuesAsKeys)
	if replaced || addr == 0 || err != nil {
		t.Error("set json")
	}

	data, err := tsc.GetKeyAsJsonBase64(sk, JsonStringValuesAsKeys)
	if data != "eyJhbmltYWxzIjp7ImNhdCI6eyJzb3VuZCI6Im1lb3cifSwiZG9nIjp7ImJyZWVkcyI6MzYwLCJzb3VuZCI6ImJhcmsifX19" || err != nil {
		t.Error("get json")
	}
}

func TestJsonCreate(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("test")

	jsonText := []byte(`{"animals": {"cat": {"sound": "meow"}, "dog": {"sound": "bark", "breeds": 360}}}`)

	var jsonData any
	err := json.Unmarshal(jsonText, &jsonData)
	if err != nil {
		t.Fatal(err.Error())
	}

	created, addr, err := tsc.CreateKeyJson(sk, jsonData, 0)
	if !created || addr == 0 || err != nil {
		t.Error("create json")
	}

	data, err := tsc.GetKeyAsJson(sk, 0)
	if data == nil || err != nil {
		t.Error("get json")
	}

	doesJsonMatch(t, "create", jsonData, data)

	created, addr, err = tsc.CreateKeyJson(sk, jsonData, 0)
	if created || addr != 0 || err != nil {
		t.Error("create json 2")
	}
}

func TestJsonCreateStrAsKey(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("test")

	jsonText := []byte(`{"animals": {"cat": {"sound": "meow"}, "dog": {"sound": "bark", "breeds": 360}}}`)

	var jsonData any
	err := json.Unmarshal(jsonText, &jsonData)
	if err != nil {
		t.Fatal(err.Error())
	}

	created, addr, err := tsc.CreateKeyJson(sk, jsonData, JsonStringValuesAsKeys)
	if !created || addr == 0 || err != nil {
		t.Error("create json")
	}

	data, err := tsc.GetKeyAsJson(sk, JsonStringValuesAsKeys)
	if data == nil || err != nil {
		t.Error("get json")
	}

	doesJsonMatch(t, "create", jsonData, data)

	created, addr, err = tsc.CreateKeyJson(sk, jsonData, JsonStringValuesAsKeys)
	if created || addr != 0 || err != nil {
		t.Error("create json 2")
	}
}

func TestJsonCreateB64(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("test")

	jsonText := []byte(`{"animals": {"cat": {"sound": "meow"}, "dog": {"sound": "bark", "breeds": 360}}}`)
	jsonDataB64 := base64.StdEncoding.EncodeToString(jsonText)

	created, addr, err := tsc.CreateKeyJsonBase64(sk, jsonDataB64, 0)
	if !created || addr == 0 || err != nil {
		t.Error("create json")
	}

	data, err := tsc.GetKeyAsJsonBase64(sk, 0)
	if data != "eyJhbmltYWxzIjp7ImNhdCI6eyJzb3VuZCI6Im1lb3cifSwiZG9nIjp7ImJyZWVkcyI6MzYwLCJzb3VuZCI6ImJhcmsifX19" || err != nil {
		t.Error("get json")
	}

	created, addr, err = tsc.CreateKeyJsonBase64(sk, jsonDataB64, 0)
	if created || addr != 0 || err != nil {
		t.Error("create json 2")
	}
}

func TestJsonCreateB64StrAsKey(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("test")

	jsonText := []byte(`{"animals": {"cat": {"sound": "meow"}, "dog": {"sound": "bark", "breeds": 360}}}`)
	jsonDataB64 := base64.StdEncoding.EncodeToString(jsonText)

	created, addr, err := tsc.CreateKeyJsonBase64(sk, jsonDataB64, JsonStringValuesAsKeys)
	if !created || addr == 0 || err != nil {
		t.Error("create json")
	}

	data, err := tsc.GetKeyAsJsonBase64(sk, JsonStringValuesAsKeys)
	if data != "eyJhbmltYWxzIjp7ImNhdCI6eyJzb3VuZCI6Im1lb3cifSwiZG9nIjp7ImJyZWVkcyI6MzYwLCJzb3VuZCI6ImJhcmsifX19" || err != nil {
		t.Error("get json")
	}

	created, addr, err = tsc.CreateKeyJsonBase64(sk, jsonDataB64, JsonStringValuesAsKeys)
	if created || addr != 0 || err != nil {
		t.Error("create json 2")
	}
}

func TestJsonReplace(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("test")

	jsonText := []byte(`{"animals": {"cat": {"sound": "meow"}, "dog": {"sound": "bark", "breeds": 360}}}`)

	var jsonData any
	err := json.Unmarshal(jsonText, &jsonData)
	if err != nil {
		t.Fatal(err.Error())
	}

	replaced, addr, err := tsc.ReplaceKeyJson(sk, jsonData, 0)
	if replaced || addr != 0 || err != nil {
		t.Error("replace json 1")
	}

	created, addr, err := tsc.CreateKeyJson(sk, jsonData, 0)
	if !created || addr == 0 || err != nil {
		t.Error("create json")
	}

	data, err := tsc.GetKeyAsJson(sk, 0)
	if data == nil || err != nil {
		t.Error("get json")
	}

	doesJsonMatch(t, "replace 1", jsonData, data)

	replaced, addr, err = tsc.ReplaceKeyJson(sk, jsonData, 0)
	if !replaced || addr == 0 || err != nil {
		t.Error("replace json 2")
	}

	jsonText = []byte(`{"animals": {"fox": {"sound": "howl"}}}`)

	err = json.Unmarshal(jsonText, &jsonData)
	if err != nil {
		t.Fatal(err.Error())
	}

	replaced, addr, err = tsc.ReplaceKeyJson(sk, jsonData, 0)
	if !replaced || addr == 0 || err != nil {
		t.Error("replace json 2")
	}

	data, err = tsc.GetKeyAsJson(sk, 0)
	if data == nil || err != nil {
		t.Error("get json 2")
	}

	doesJsonMatch(t, "replace 2", jsonData, data)
}

func TestJsonReplaceStrAsKey(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("test")

	jsonText := []byte(`{"animals": {"cat": {"sound": "meow"}, "dog": {"sound": "bark", "breeds": 360}}}`)

	var jsonData any
	err := json.Unmarshal(jsonText, &jsonData)
	if err != nil {
		t.Fatal(err.Error())
	}

	replaced, addr, err := tsc.ReplaceKeyJson(sk, jsonData, JsonStringValuesAsKeys)
	if replaced || addr != 0 || err != nil {
		t.Error("replace json 1")
	}

	created, addr, err := tsc.CreateKeyJson(sk, jsonData, JsonStringValuesAsKeys)
	if !created || addr == 0 || err != nil {
		t.Error("create json")
	}

	data, err := tsc.GetKeyAsJson(sk, JsonStringValuesAsKeys)
	if data == nil || err != nil {
		t.Error("get json")
	}

	doesJsonMatch(t, "replace 1", jsonData, data)

	replaced, addr, err = tsc.ReplaceKeyJson(sk, jsonData, JsonStringValuesAsKeys)
	if !replaced || addr == 0 || err != nil {
		t.Error("replace json 2")
	}

	jsonText = []byte(`{"animals": {"fox": {"sound": "howl"}}}`)

	err = json.Unmarshal(jsonText, &jsonData)
	if err != nil {
		t.Fatal(err.Error())
	}

	replaced, addr, err = tsc.ReplaceKeyJson(sk, jsonData, JsonStringValuesAsKeys)
	if !replaced || addr == 0 || err != nil {
		t.Error("replace json 2")
	}

	data, err = tsc.GetKeyAsJson(sk, JsonStringValuesAsKeys)
	if data == nil || err != nil {
		t.Error("get json 2")
	}

	doesJsonMatch(t, "replace 2", jsonData, data)
}

func TestJsonReplaceBase64(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("test")

	jsonText := []byte(`{"animals": {"cat": {"sound": "meow"}, "dog": {"sound": "bark", "breeds": 360}}}`)
	jsonDataB64 := base64.StdEncoding.EncodeToString(jsonText)

	replaced, addr, err := tsc.ReplaceKeyJsonBase64(sk, jsonDataB64, 0)
	if replaced || addr != 0 || err != nil {
		t.Error("replace json 1")
	}

	created, addr, err := tsc.CreateKeyJsonBase64(sk, jsonDataB64, 0)
	if !created || addr == 0 || err != nil {
		t.Error("create json")
	}

	data, err := tsc.GetKeyAsJsonBase64(sk, 0)
	if data != "eyJhbmltYWxzIjp7ImNhdCI6eyJzb3VuZCI6Im1lb3cifSwiZG9nIjp7ImJyZWVkcyI6MzYwLCJzb3VuZCI6ImJhcmsifX19" || err != nil {
		t.Error("get json")
	}

	replaced, addr, err = tsc.ReplaceKeyJson(sk, jsonDataB64, 0)
	if !replaced || addr == 0 || err != nil {
		t.Error("replace json 2")
	}

	jsonText = []byte(`{"animals": {"fox": {"sound": "howl"}}}`)
	jsonDataB64 = base64.StdEncoding.EncodeToString(jsonText)

	replaced, addr, err = tsc.ReplaceKeyJsonBase64(sk, jsonDataB64, 0)
	if !replaced || addr == 0 || err != nil {
		t.Error("replace json 2")
	}

	data, err = tsc.GetKeyAsJsonBase64(sk, 0)
	if data != "eyJhbmltYWxzIjp7ImZveCI6eyJzb3VuZCI6Imhvd2wifX19" || err != nil {
		t.Error("get json 2")
	}
}

func TestJsonReplaceBase64StrAsKey(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("test")

	jsonText := []byte(`{"animals": {"cat": {"sound": "meow"}, "dog": {"sound": "bark", "breeds": 360}}}`)
	jsonDataB64 := base64.StdEncoding.EncodeToString(jsonText)

	replaced, addr, err := tsc.ReplaceKeyJsonBase64(sk, jsonDataB64, JsonStringValuesAsKeys)
	if replaced || addr != 0 || err != nil {
		t.Error("replace json 1")
	}

	created, addr, err := tsc.CreateKeyJsonBase64(sk, jsonDataB64, JsonStringValuesAsKeys)
	if !created || addr == 0 || err != nil {
		t.Error("create json")
	}

	data, err := tsc.GetKeyAsJsonBase64(sk, JsonStringValuesAsKeys)
	if data != "eyJhbmltYWxzIjp7ImNhdCI6eyJzb3VuZCI6Im1lb3cifSwiZG9nIjp7ImJyZWVkcyI6MzYwLCJzb3VuZCI6ImJhcmsifX19" || err != nil {
		t.Error("get json")
	}

	replaced, addr, err = tsc.ReplaceKeyJson(sk, jsonDataB64, JsonStringValuesAsKeys)
	if !replaced || addr == 0 || err != nil {
		t.Error("replace json 2")
	}

	jsonText = []byte(`{"animals": {"fox": {"sound": "howl"}}}`)
	jsonDataB64 = base64.StdEncoding.EncodeToString(jsonText)

	replaced, addr, err = tsc.ReplaceKeyJsonBase64(sk, jsonDataB64, JsonStringValuesAsKeys)
	if !replaced || addr == 0 || err != nil {
		t.Error("replace json 2")
	}

	data, err = tsc.GetKeyAsJsonBase64(sk, JsonStringValuesAsKeys)
	if data != "eyJhbmltYWxzIjp7ImZveCI6eyJzb3VuZCI6Imhvd2wifX19" || err != nil {
		t.Error("get json 2")
	}
}

func TestJsonMerge(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("test")

	jsonText := []byte(`{"animals": {"cat": {"sound": "meow"}, "dog": {"sound": "bark", "breeds": 360}}}`)

	var jsonData any
	err := json.Unmarshal(jsonText, &jsonData)
	if err != nil {
		t.Fatal(err.Error())
	}

	addr, err := tsc.MergeKeyJson(sk, jsonData, 0)
	if err != nil {
		t.Error("merge 1")
	}
	if addr == 0 {
		t.Error("addr 0")
	}

	jsonText = []byte(`{"animals": {"fox": {"sound": "howl"}}}`)

	err = json.Unmarshal(jsonText, &jsonData)
	if err != nil {
		t.Fatal(err.Error())
	}

	addr, err = tsc.MergeKeyJson(sk, jsonData, 0)
	if err != nil {
		t.Error("merge 2")
	}
	if addr == 0 {
		t.Error("addr 0")
	}

	data, err := tsc.GetKeyAsJson(sk, 0)
	if data == nil || err != nil {
		t.Error("get json 2")
	}

	jsonText = []byte(`{"animals": {"cat": {"sound": "meow"}, "dog": {"sound": "bark", "breeds": 360}, "fox": {"sound": "howl"}}}`)

	err = json.Unmarshal(jsonText, &jsonData)
	if err != nil {
		t.Fatal(err.Error())
	}

	doesJsonMatch(t, "merge", jsonData, data)
}

func TestJsonMergeStrAsKey(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("test")

	jsonText := []byte(`{"animals": {"cat": {"sound": "meow"}, "dog": {"sound": "bark", "breeds": 360}}}`)

	var jsonData any
	err := json.Unmarshal(jsonText, &jsonData)
	if err != nil {
		t.Fatal(err.Error())
	}

	addr, err := tsc.MergeKeyJson(sk, jsonData, JsonStringValuesAsKeys)
	if err != nil {
		t.Error("merge 1")
	}
	if addr == 0 {
		t.Error("addr 0")
	}

	jsonText = []byte(`{"animals": {"fox": {"sound": "howl"}}}`)

	err = json.Unmarshal(jsonText, &jsonData)
	if err != nil {
		t.Fatal(err.Error())
	}

	addr, err = tsc.MergeKeyJson(sk, jsonData, JsonStringValuesAsKeys)
	if err != nil {
		t.Error("merge 2")
	}
	if addr == 0 {
		t.Error("addr 0")
	}

	data, err := tsc.GetKeyAsJson(sk, JsonStringValuesAsKeys)
	if data == nil || err != nil {
		t.Error("get json 2")
	}

	jsonText = []byte(`{"animals": {"cat": {"sound": "meow"}, "dog": {"sound": "bark", "breeds": 360}, "fox": {"sound": "howl"}}}`)

	err = json.Unmarshal(jsonText, &jsonData)
	if err != nil {
		t.Fatal(err.Error())
	}

	doesJsonMatch(t, "merge", jsonData, data)
}

func TestJsonMergeBase64(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("test")

	jsonText := []byte(`{"animals": {"cat": {"sound": "meow"}, "dog": {"sound": "bark", "breeds": 360}}}`)
	jsonDataB64 := base64.StdEncoding.EncodeToString(jsonText)

	addr, err := tsc.MergeKeyJsonBase64(sk, jsonDataB64, 0)
	if err != nil {
		t.Error("merge 1")
	}
	if addr == 0 {
		t.Error("addr 0")
	}

	jsonText = []byte(`{"animals": {"fox": {"sound": "howl"}}}`)
	jsonDataB64 = base64.StdEncoding.EncodeToString(jsonText)

	addr, err = tsc.MergeKeyJsonBase64(sk, jsonDataB64, 0)
	if err != nil {
		t.Error("merge 2")
	}
	if addr == 0 {
		t.Error("addr 0")
	}

	data, err := tsc.GetKeyAsJsonBase64(sk, 0)
	if data != "eyJhbmltYWxzIjp7ImNhdCI6eyJzb3VuZCI6Im1lb3cifSwiZG9nIjp7ImJyZWVkcyI6MzYwLCJzb3VuZCI6ImJhcmsifSwiZm94Ijp7InNvdW5kIjoiaG93bCJ9fX0=" || err != nil {
		t.Error("get json 2")
	}
}

func TestJsonMergeBase64StrAsKey(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("test")

	jsonText := []byte(`{"animals": {"cat": {"sound": "meow"}, "dog": {"sound": "bark", "breeds": 360}}}`)
	jsonDataB64 := base64.StdEncoding.EncodeToString(jsonText)

	addr, err := tsc.MergeKeyJsonBase64(sk, jsonDataB64, JsonStringValuesAsKeys)
	if err != nil {
		t.Error("merge 1")
	}
	if addr == 0 {
		t.Error("addr 0")
	}

	jsonText = []byte(`{"animals": {"fox": {"sound": "howl"}}}`)
	jsonDataB64 = base64.StdEncoding.EncodeToString(jsonText)

	addr, err = tsc.MergeKeyJsonBase64(sk, jsonDataB64, JsonStringValuesAsKeys)
	if err != nil {
		t.Error("merge 2")
	}
	if addr == 0 {
		t.Error("addr 0")
	}

	data, err := tsc.GetKeyAsJsonBase64(sk, JsonStringValuesAsKeys)
	if data != "eyJhbmltYWxzIjp7ImNhdCI6eyJzb3VuZCI6Im1lb3cifSwiZG9nIjp7ImJyZWVkcyI6MzYwLCJzb3VuZCI6ImJhcmsifSwiZm94Ijp7InNvdW5kIjoiaG93bCJ9fX0=" || err != nil {
		t.Error("get json 2")
	}
}

func TestCalculateKeyValue(t *testing.T) {
	_, tsc := testSetup(t)

	sk := MakeStoreKey("test")

	addr, newVal, err := tsc.CalculateKeyValue(sk, "i+1")
	if err != nil {
		t.Fatal(err)
	}
	if addr != 2 || newVal != 1 {
		t.Error("calc increment")
	}

	value, ke, vs, err := tsc.GetKeyValue(sk)
	if err != nil {
		t.Fatal(err)
	}
	if !ke || !vs || value != 1 {
		t.Error("value verify")
	}
}

func TestMoveKey(t *testing.T) {
	_, tsc := testSetup(t)

	ssk := MakeStoreKey("source")
	dsk := MakeStoreKey("dest")

	tick := fmt.Sprintf("%d", time.Now().UnixNano())
	_, _, err := tsc.SetKeyValue(ssk, tick)
	if err != nil {
		t.Fatal(err)
	}

	exists, moved, err := tsc.MoveKey(ssk, dsk, false)
	if err != nil {
		t.Fatal(err)
	}
	if !exists || !moved {
		t.Error("should have moved")
	}

	value, ke, vs, err := tsc.GetKeyValue(dsk)
	if err != nil {
		t.Fatal(err)
	}
	if !ke || !vs || value != tick {
		t.Error("value verify")
	}
}

func TestMoveKeyOverwrite(t *testing.T) {
	_, tsc := testSetup(t)

	ssk := MakeStoreKey("source")
	dsk := MakeStoreKey("dest")

	tick := fmt.Sprintf("%d", time.Now().UnixNano())
	_, _, err := tsc.SetKeyValue(ssk, tick)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = tsc.SetKeyValue(dsk, 100)
	if err != nil {
		t.Fatal(err)
	}

	exists, moved, err := tsc.MoveKey(ssk, dsk, false)
	if err != nil {
		t.Fatal(err)
	}
	if !exists || moved {
		t.Error("should not have moved")
	}

	value, ke, vs, err := tsc.GetKeyValue(dsk)
	if err != nil {
		t.Fatal(err)
	}
	if !ke || !vs || value != 100 {
		t.Error("value verify")
	}

	exists, moved, err = tsc.MoveKey(ssk, dsk, true)
	if err != nil {
		t.Fatal(err)
	}
	if !exists || !moved {
		t.Error("should have moved")
	}

	value, ke, vs, err = tsc.GetKeyValue(dsk)
	if err != nil {
		t.Fatal(err)
	}
	if !ke || !vs || value != tick {
		t.Error("value verify")
	}
}

func TestMoveReferencedKey(t *testing.T) {
	_, tsc := testSetup(t)

	ssk := MakeStoreKey("source")
	dsk := MakeStoreKey("dest")
	rsk1 := MakeStoreKey("index1")
	rsk2 := MakeStoreKey("index2")

	tick := fmt.Sprintf("%d", time.Now().UnixNano())
	_, _, err := tsc.SetKeyValue(ssk, tick)
	if err != nil {
		t.Fatal(err)
	}

	exists, moved, err := tsc.MoveReferencedKey(ssk, dsk, false, nil, []StoreKey{rsk1, rsk2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !exists || !moved {
		t.Error("should have moved")
	}

	value, ke, vs, err := tsc.GetKeyValue(dsk)
	if err != nil {
		t.Fatal(err)
	}
	if !ke || !vs || value != tick {
		t.Error("value verify")
	}

	hasLink, rv, err := tsc.GetRelationshipValue(rsk1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !hasLink || rv == nil {
		t.Error("follow relationship 1")
	}

	if rv.CurrentValue != tick {
		t.Error("ref value verify 1")
	}

	hasLink, rv, err = tsc.GetRelationshipValue(rsk2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !hasLink || rv == nil {
		t.Error("follow relationship 2")
	}

	if rv.CurrentValue != tick {
		t.Error("ref value verify 2")
	}

	// move back to src and remove reference keys
	exists, moved, err = tsc.MoveReferencedKey(dsk, ssk, false, nil, nil, []StoreKey{rsk1, rsk2})
	if err != nil {
		t.Fatal(err)
	}
	if !exists || !moved {
		t.Error("should have moved")
	}

	hasLink, rv, err = tsc.GetRelationshipValue(rsk1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if hasLink || rv != nil {
		t.Error("follow unref relationship 1")
	}

	hasLink, rv, err = tsc.GetRelationshipValue(rsk2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if hasLink || rv != nil {
		t.Error("follow unref relationship 2")
	}
}

func TestMoveReferencedKeyTtl(t *testing.T) {
	_, tsc := testSetup(t)

	ssk := MakeStoreKey("source")
	dsk := MakeStoreKey("dest")
	rsk := MakeStoreKey("index")

	tick := fmt.Sprintf("%d", time.Now().UnixNano())
	_, _, err := tsc.SetKeyValue(ssk, tick)
	if err != nil {
		t.Fatal(err)
	}

	expire := time.Now().Add(time.Minute)

	exists, moved, err := tsc.MoveReferencedKey(ssk, dsk, false, &expire, []StoreKey{rsk}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !exists || !moved {
		t.Error("should have moved")
	}

	value, ke, vs, err := tsc.GetKeyValue(dsk)
	if err != nil {
		t.Fatal(err)
	}
	if !ke || !vs || value != tick {
		t.Error("value verify")
	}

	hasLink, rv, err := tsc.GetRelationshipValue(rsk, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !hasLink || rv == nil {
		t.Error("follow relationship")
	}

	if rv.CurrentValue != tick {
		t.Error("ref value verify")
	}

	ttl, err := tsc.GetKeyTtl(dsk)
	if err != nil {
		t.Fatal(err)
	}
	if ttl.UnixNano() != expire.UnixNano() {
		t.Error("dest ttl")
	}

	ttl, err = tsc.GetKeyTtl(rsk)
	if err != nil {
		t.Fatal(err)
	}
	if ttl.UnixNano() != expire.UnixNano() {
		t.Error("ref ttl")
	}
}

func TestMoveReferencedKeyTtlZero(t *testing.T) {
	_, tsc := testSetup(t)

	ssk := MakeStoreKey("source")
	dsk := MakeStoreKey("dest")
	rsk := MakeStoreKey("index")

	tick := fmt.Sprintf("%d", time.Now().UnixNano())
	_, _, err := tsc.SetKeyValue(ssk, tick)
	if err != nil {
		t.Fatal(err)
	}

	expire := ZeroTime

	exists, moved, err := tsc.MoveReferencedKey(ssk, dsk, false, &expire, []StoreKey{rsk}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !exists || !moved {
		t.Error("should have moved")
	}

	value, ke, vs, err := tsc.GetKeyValue(dsk)
	if err != nil {
		t.Fatal(err)
	}
	if !ke || !vs || value != tick {
		t.Error("should not have expired")
	}

	hasLink, rv, err := tsc.GetRelationshipValue(rsk, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !hasLink || rv == nil {
		t.Error("must have relationship to moved key")
	}
}

func TestMoveReferencedKeyTtlExpired(t *testing.T) {
	_, tsc := testSetup(t)

	ssk := MakeStoreKey("source")
	dsk := MakeStoreKey("dest")
	rsk := MakeStoreKey("index")

	tick := fmt.Sprintf("%d", time.Now().UnixNano())
	_, _, err := tsc.SetKeyValue(ssk, tick)
	if err != nil {
		t.Fatal(err)
	}

	expire := ExpiredTime

	exists, moved, err := tsc.MoveReferencedKey(ssk, dsk, false, &expire, []StoreKey{rsk}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !exists || !moved {
		t.Error("should have moved")
	}

	value, ke, vs, err := tsc.GetKeyValue(dsk)
	if err != nil {
		t.Fatal(err)
	}
	if ke || vs || value != nil {
		t.Error("should have expired")
	}

	hasLink, rv, err := tsc.GetRelationshipValue(rsk, 0)
	if err != nil {
		t.Fatal(err)
	}
	if hasLink || rv != nil {
		t.Error("expired relationship should not exist")
	}
}

func TestMoveReferencedKey3(t *testing.T) {
	_, tsc := testSetup(t)

	ssk := MakeStoreKey("source")
	dsk := MakeStoreKey("dest")

	tick := fmt.Sprintf("%d", time.Now().UnixNano())
	_, _, err := tsc.SetKeyValue(ssk, tick)
	if err != nil {
		t.Fatal(err)
	}

	exists, moved, err := tsc.MoveReferencedKey(ssk, dsk, false, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !exists || !moved {
		t.Error("should have moved")
	}

	value, ke, vs, err := tsc.GetKeyValue(dsk)
	if err != nil {
		t.Fatal(err)
	}
	if !ke || !vs || value != tick {
		t.Error("value verify")
	}
}

func TestOverlapped(t *testing.T) {
	_, tsc := testSetup(t)

	count := 200
	wgs := make([]*sync.WaitGroup, 0, count)

	for i := 0; i < count; i++ {
		wg := sync.WaitGroup{}
		wg.Add(1)
		wgs = append(wgs, &wg)

		go func(n int) {
			sk := MakeStoreKey("test", fmt.Sprintf("%d", n))
			addr, _, err := tsc.SetKeyValue(sk, n)
			if err != nil {
				t.Error("error", err)
			}

			val, ke, ve, err := tsc.GetKeyValue(sk)
			if err != nil {
				t.Error("error", err)
			}

			if !ke || !ve || val.(int) != n {
				t.Error("value mismatch")
			}

			sk2, exists, err := tsc.KeyFromAddress(addr)
			if err != nil {
				t.Error("error", err)
			}

			if !exists || sk2.Path != sk.Path {
				t.Error("addr lookup fail")
			}

			wgs[n].Done()
		}(i)
	}

	for i := 0; i < count; i++ {
		wgs[i].Wait()
	}
}
