package treestore_client

import (
	"bytes"
	"encoding/gob"
	"encoding/hex"
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

	case treestore.TokenPath:
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
	if n == 0 {
		return nil
	}

	t := time.Unix(n/(1000*1000*1000), n%(1000*1000*1000))
	return &t
}

func EscapeTokenString(plainText string) string {
	return treestore.EscapeTokenString(plainText)
}

func UnescapeTokenString(tokenText string) string {
	return treestore.UnescapeTokenString(tokenText)
}

func MakeTokenPath(parts ...string) TokenPath {
	return TokenPath(treestore.MakeTokenPath(parts...))
}

func SplitTokenPath(tokenPath TokenPath) []string {
	return treestore.SplitTokenPath(treestore.TokenPath(tokenPath))
}

func TokenSegmentToString(segment TokenSegment) string {
	return treestore.TokenSegmentToString(treestore.TokenSegment(segment))
}

func TokenPathToTokenSet(tokenPath TokenPath) TokenSet {
	return TokenSet(treestore.TokenPathToTokenSet(treestore.TokenPath(tokenPath)))
}

func TokenSetToTokenPath(tokens TokenSet) TokenPath {
	return TokenPath(treestore.TokenSetToTokenPath(treestore.TokenSet(tokens)))
}

func MakeStoreKey(parts ...string) StoreKey {
	return StoreKey(treestore.MakeStoreKey(parts...))
}

func MakeStoreKeyFromPath(tokenPath TokenPath) StoreKey {
	return StoreKey(treestore.MakeStoreKeyFromPath(treestore.TokenPath(tokenPath)))
}

func MakeStoreKeyFromTokenSegments(segments ...TokenSegment) StoreKey {
	arg := make([]treestore.TokenSegment, 0, len(segments))
	for _, seg := range segments {
		arg = append(arg, treestore.TokenSegment(seg))
	}
	return StoreKey(treestore.MakeStoreKeyFromTokenSegments(arg...))
}

func SplitStoreKey(sk StoreKey) []string {
	return treestore.SplitStoreKey(treestore.StoreKey(sk))
}
