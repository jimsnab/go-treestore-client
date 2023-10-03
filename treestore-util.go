package treestore_client

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jimsnab/go-treestore"
)

func bytesToEscapedValue(v []byte) string {
	var sb strings.Builder
	for _, by := range v {
		if by < 32 || by == '\\' {
			sb.WriteString(fmt.Sprintf("\\%02X", by))
		} else {
			sb.WriteByte(by)
		}
	}
	return sb.String()
}

func valueEscape(v any) string {
	switch t := v.(type) {
	case string:
		return bytesToEscapedValue([]byte(t))

	case TokenPath:
		return bytesToEscapedValue([]byte(t))

	case []byte:
		return bytesToEscapedValue(t)

	default:
		return ""
	}
}

func valueUnescape(v string) []byte {
	unescaped := make([]byte, 0, len(v))

	pos := 0
	for pos < len(v) {
		by := v[pos]
		if by == '\\' && pos+2 < len(v) {
			decoded, err := hex.DecodeString(string(v[pos+1 : pos+3]))
			if err != nil {
				unescaped = append(unescaped, by)
				continue
			}
			by = decoded[0]
			pos += 2
		}
		unescaped = append(unescaped, by)
		pos++
	}

	return unescaped
}

func nativeValueToCmdline(val any) (value, valueType string, err error) {
	switch t := val.(type) {
	case []byte:
		value = bytesToEscapedValue(t)

	case string:
		value = bytesToEscapedValue([]byte(t))
		valueType = "string"

	case int:
		by := make([]byte, 4)
		binary.BigEndian.PutUint32(by, uint32(t))
		value = bytesToEscapedValue(by)
		valueType = "int"
	case int8:
		by := []byte{byte(t)}
		value = bytesToEscapedValue(by)
		valueType = "int8"
	case int16:
		by := make([]byte, 2)
		binary.BigEndian.PutUint16(by, uint16(t))
		value = bytesToEscapedValue(by)
		valueType = "int16"
	case int32:
		by := make([]byte, 4)
		binary.BigEndian.PutUint32(by, uint32(t))
		value = bytesToEscapedValue(by)
		valueType = "int32"
	case int64:
		by := make([]byte, 8)
		binary.BigEndian.PutUint64(by, uint64(t))
		value = bytesToEscapedValue(by)
		valueType = "int64"

	case uint:
		by := make([]byte, 4)
		binary.BigEndian.PutUint32(by, uint32(t))
		value = bytesToEscapedValue(by)
		valueType = "uint"
	case uint8:
		by := []byte{byte(t)}
		value = bytesToEscapedValue(by)
		valueType = "uint8"
	case uint16:
		by := make([]byte, 2)
		binary.BigEndian.PutUint16(by, uint16(t))
		value = bytesToEscapedValue(by)
		valueType = "uint16"
	case uint32:
		by := make([]byte, 4)
		binary.BigEndian.PutUint32(by, uint32(t))
		value = bytesToEscapedValue(by)
		valueType = "uint32"
	case uint64:
		by := make([]byte, 8)
		binary.BigEndian.PutUint64(by, uint64(t))
		value = bytesToEscapedValue(by)
		valueType = "uint64"

	case float32, float64, bool, complex64, complex128:
		str := fmt.Sprintf("%v", t)
		value = bytesToEscapedValue([]byte(str))
		valueType = fmt.Sprintf("%T", t)

	default:
		var by []byte
		by, err = json.Marshal(t)
		if err != nil {
			return
		}
		value = bytesToEscapedValue(by)
		valueType = fmt.Sprintf("json-%T", t)
	}
	return
}

func cmdlineToNativeValue(valStr, valueType string) (val any, err error) {
	value := valueUnescape(valStr)

	switch valueType {
	case "int":
		if len(value) != 4 {
			err = errors.New("invalid int value")
			return
		}
		val = int(binary.BigEndian.Uint32(value))
		return
	case "int8":
		if len(value) != 1 {
			err = errors.New("invalid int8 value")
			return
		}
		val = int8(value[0])
		return
	case "int16":
		if len(value) != 2 {
			err = errors.New("invalid int16 value")
			return
		}
		val = int16(binary.BigEndian.Uint16(value))
		return
	case "int32":
		if len(value) != 4 {
			err = errors.New("invalid int32 value")
			return
		}
		val = int32(binary.BigEndian.Uint32(value))
		return
	case "int64":
		if len(value) != 8 {
			err = errors.New("invalid int64 value")
			return
		}
		val = int64(binary.BigEndian.Uint64(value))
		return
	case "uint":
		if len(value) != 4 {
			err = errors.New("invalid uint value")
			return
		}
		val = binary.BigEndian.Uint32(value)
		return
	case "uint8":
		if len(value) != 1 {
			err = errors.New("invalid uint8 value")
			return
		}
		val = int8(value[0])
		return
	case "uint16":
		if len(value) != 2 {
			err = errors.New("invalid uint16 value")
			return
		}
		val = binary.BigEndian.Uint16(value)
		return
	case "uint32":
		if len(value) != 4 {
			err = errors.New("invalid uint32 value")
			return
		}
		val = binary.BigEndian.Uint32(value)
		return
	case "uint64":
		if len(value) != 8 {
			err = errors.New("invalid uint64 value")
			return
		}
		val = binary.BigEndian.Uint64(value)
		return
	case "float32":
		var f64 float64
		f64, err = strconv.ParseFloat(string(value), 32)
		if err != nil {
			return
		}
		val = float32(f64)
		return
	case "float64":
		val, err = strconv.ParseFloat(string(value), 32)
		if err != nil {
			return
		}
		return
	case "bool":
		val, err = strconv.ParseBool(string(value))
		if err != nil {
			return
		}
		return
	case "complex64":
		var c128 complex128
		c128, err = strconv.ParseComplex(string(value), 64)
		if err != nil {
			return
		}
		val = complex64(c128)
		return
	case "complex128":
		val, err = strconv.ParseComplex(string(value), 128)
		if err != nil {
			return
		}
		return
	case "string":
		val = string(value)
		return
	case "":
		val = value
		return
	}

	if strings.HasPrefix(valueType, "json-") {
		val = value
		return
	}

	err = errors.New("unrecognized value type " + valueType)
	return
}

// Simple wrapper of gob to binary-encode before storing as a treestore value.
// panics on an error
func ValueEncode[T any](v T) []byte {
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)

	err := e.Encode(&v)
	if err != nil {
		panic(err)
	}
	return b.Bytes()
}

// Simple wrapper of gob to binary-decode after retrieving a treestore value.
// panics on an error
func ValueDecode[T any](v []byte) (result T) {
	if v != nil {
		b := bytes.Buffer{}
		b.Write(v)
		d := gob.NewDecoder(&b)

		err := d.Decode(&result)
		if err != nil {
			panic(err)
		}
	}
	return
}

func requestEpochNs(v *time.Time) string {
	if v == nil {
		return "0"
	}
	return fmt.Sprintf("%d", v.UnixNano())
}

func requestAddress(v StoreAddress) string {
	return fmt.Sprintf("%d", v)
}

func responseAddress(v any) StoreAddress {
	return StoreAddress(v.(float64))
}

func responseBool(v any) bool {
	return v.(bool)
}

func responseEpochNs(v any) *time.Time {
	n, _ := strconv.ParseInt(v.(string), 10, 64)
	if n < 0 {
		return nil
	}

	t := time.Unix(n/(1000*1000*1000), n%(1000*1000*1000))
	return &t
}

var EscapeTokenString = treestore.EscapeTokenString
var UnescapeTokenString = treestore.UnescapeTokenString
var MakeTokenPath = treestore.MakeTokenPath
var SplitTokenPath = treestore.SplitTokenPath
var TokenSegmentToString = treestore.TokenSegmentToString
var TokenPathToTokenSet = treestore.TokenPathToTokenSet
var TokenSetToTokenPath = treestore.TokenSetToTokenPath
var MakeStoreKey = treestore.MakeStoreKey
var MakeStoreKeyFromPath = treestore.MakeStoreKeyFromPath
var MakeStoreKeyFromTokenSegments = treestore.MakeStoreKeyFromTokenSegments
var SplitStoreKey = treestore.SplitStoreKey
var AppendStoreKeySegments = treestore.AppendStoreKeySegments
var AppendStoreKeySegmentStrings = treestore.AppendStoreKeySegmentStrings
var MakeRecordSubPath = treestore.MakeRecordSubPath
var MakeRecordSubPathFromSegments = treestore.MakeRecordSubPathFromSegments
