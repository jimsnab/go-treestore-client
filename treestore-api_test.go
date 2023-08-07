package treestore_client

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/jimsnab/go-lane"
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

func TestSetGetV(t *testing.T) {
	_, tsc := testSetup(t)

	tick := fmt.Sprintf("%d", time.Now().UnixNano())

	_, _, err := tsc.SetKeyValue(MakeStoreKey("client", "test", "key"), []byte(tick))
	if err != nil {
		t.Fatal(err)
	}

	value, ke, vs, err := tsc.GetKeyValue(MakeStoreKey("client", "test", "key"))
	if err != nil {
		t.Fatal(err)
	}

	if string(value) != tick {
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

	v1 := ValueEncode(100)

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

	v2 := ValueEncode(200)

	addr, exists, orgVal, err = tsc.SetKeyValueEx(sk, v2, 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(orgVal, v1) {
		t.Error("value bytes")
	}

	if addr != 4 || !exists || ValueDecode[int](orgVal) != 100 {
		t.Error("setex value 2")
	}

	addr, exists, orgVal, err = tsc.SetKeyValueEx(sk, nil, SetExMustNotExist|SetExNoValueUpdate, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	if addr != 0 || !exists || ValueDecode[int](orgVal) != 200 {
		t.Error("setex value 3")
	}

	addr, exists, orgVal, err = tsc.SetKeyValueEx(sk, nil, SetExMustExist, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	if addr != 4 || !exists || ValueDecode[int](orgVal) != 200 {
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

	if !hasLink || rv == nil || ValueDecode[int](rv.CurrentValue) != 200 || rv.Sk.Path != "/client/test/key" {
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

	if !exists || !bytes.Equal([]byte("test"), value) {
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
	if !kr || !vr || !bytes.Equal(ov, []byte("test")) {
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
	if !removed || !bytes.Equal(ov, []byte("test")) {
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

	err = tsc.ClearKeyMetdata(sk)
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

	exists, pv, err = tsc.ClearMetdataAttribute(sk, "empty")
	if err != nil {
		t.Fatal(err)
	}
	if !exists || pv != "" {
		t.Error("clear empty")
	}

	exists, pv, err = tsc.ClearMetdataAttribute(sk, "empty")
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
	if !ke || !ve || sk.Path != "/foo" || len(sk.Tokens) != 1 || !bytes.Equal(val, []byte("test")) {
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

	tsc.SetKeyValue(MakeStoreKey("cat"), []byte("1"))
	tsc.SetKeyValue(MakeStoreKey("dog/s"), []byte("2"))
	tsc.SetKeyValue(MakeStoreKey("mouse"), []byte("3"))

	values, err := tsc.GetMatchingKeyValues(MakeStoreKey("*o*"), 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 || values[0].Key != `/dog\ss` || values[0].HasChildren || !bytes.Equal(values[0].CurrentValue, []byte("2")) ||
		values[1].Key != `/mouse` || values[1].HasChildren || !bytes.Equal(values[1].CurrentValue, []byte("3")) {
		t.Error("match pattern")
	}

	values, err = tsc.GetMatchingKeyValues(MakeStoreKey("*o*"), 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || values[0].Key != `/dog\ss` || values[0].HasChildren || !bytes.Equal(values[0].CurrentValue, []byte("2")) {
		t.Error("limit")
	}

	values, err = tsc.GetMatchingKeyValues(MakeStoreKey("*o*"), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || values[0].Key != `/mouse` || values[0].HasChildren || !bytes.Equal(values[0].CurrentValue, []byte("3")) {
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
