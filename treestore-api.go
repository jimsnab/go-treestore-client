package treestore_client

import (
	"time"

	"github.com/jimsnab/go-treestore"
)

type (
	TokenSegment      treestore.TokenSegment
	TokenPath         treestore.TokenPath
	TokenSet          treestore.TokenSet
	StoreKey          treestore.StoreKey
	StoreAddress      treestore.StoreAddress
	SetExFlags        treestore.SetExFlags
	RelationshipValue struct {
		Sk           StoreKey
		CurrentValue any
	}
	LevelKey struct {
		Segment     TokenSegment
		HasValue    bool
		HasChildren bool
	}
	KeyMatch struct {
		Key           TokenPath
		Metadata      map[string]string
		HasValue      bool
		HasChildren   bool
		CurrentValue  any
		Relationships []StoreAddress
	}
	KeyValueMatch struct {
		Key           TokenPath
		Metadata      map[string]string
		HasChildren   bool
		CurrentValue  any
		Relationships []StoreAddress
	}

	TSClient interface {
		Close() error
		SetServer(host string, port int)
		SetKey(sk StoreKey) (address StoreAddress, exists bool, err error)
		SetKeyValue(sk StoreKey, value any) (address StoreAddress, firstValue bool, err error)
		SetKeyValueEx(sk StoreKey, value any, flags SetExFlags, expire *time.Time, relationships []StoreAddress) (address StoreAddress, exists bool, originalValue any, err error)
		IsKeyIndexed(sk StoreKey) (address StoreAddress, exists bool, err error)
		LocateKey(sk StoreKey) (address StoreAddress, exists bool, err error)
		GetKeyTtl(sk StoreKey) (ttl *time.Time, err error)
		SetKeyTtl(sk StoreKey, expiration *time.Time) (exists bool, err error)
		GetKeyValue(sk StoreKey) (value any, keyExists, valueExists bool, err error)
		GetKeyValueTtl(sk StoreKey) (ttl *time.Time, err error)
		SetKeyValueTtl(sk StoreKey, expiration *time.Time) (exists bool, err error)
		GetKeyValueAtTime(sk StoreKey, when *time.Time) (value any, exists bool, err error)
		DeleteKeyWithValue(sk StoreKey, clean bool) (removed bool, originalValue any, err error)
		DeleteKey(sk StoreKey) (keyRemoved, valueRemoved bool, originalValue any, err error)
		SetMetadataAttribute(sk StoreKey, attribute, value string) (keyExists bool, priorValue string, err error)
		ClearMetdataAttribute(sk StoreKey, attribute string) (attributeExists bool, originalValue string, err error)
		ClearKeyMetdata(sk StoreKey) (err error)
		GetMetadataAttribute(sk StoreKey, attribute string) (attributeExists bool, value string, err error)
		GetMetadataAttributes(sk StoreKey) (attributes []string, err error)
		KeyFromAddress(addr StoreAddress) (sk StoreKey, exists bool, err error)
		KeyValueFromAddress(addr StoreAddress) (keyExists, valueExists bool, sk StoreKey, value any, err error)
		GetRelationshipValue(sk StoreKey, relationshipIndex int) (hasLink bool, rv *RelationshipValue, err error)
		GetLevelKeys(sk StoreKey, pattern string, startAt, limit int) (keys []LevelKey, err error)
		GetMatchingKeys(skPattern StoreKey, startAt, limit int) (keys []*KeyMatch, err error)
		GetMatchingKeyValues(skPattern StoreKey, startAt, limit int) (values []*KeyValueMatch, err error)
		Export(sk StoreKey) (jsonData any, err error)
		ExportBase64(sk StoreKey) (b64 string, err error)
		Import(sk StoreKey, jsonData any) (err error)
		ImportBase64(sk StoreKey, b64 string) (err error)
		GetKeyAsJson(sk StoreKey) (jsonData any, err error)
		GetKeyAsJsonBytes(sk StoreKey) (jsonData []byte, err error)
		GetKeyAsJsonBase64(sk StoreKey) (b64 string, err error)
		SetKeyJson(sk StoreKey, jsonData any) (replaced bool, err error)
		SetKeyJsonBase64(sk StoreKey, b64 string) (replaced bool, err error)
		CreateKeyJson(sk StoreKey, jsonData any) (created bool, err error)
		CreateKeyJsonBase64(sk StoreKey, b64 string) (created bool, err error)
		ReplaceKeyJson(sk StoreKey, jsonData any) (replaced bool, err error)
		ReplaceKeyJsonBase64(sk StoreKey, b64 string) (replaced bool, err error)
		MergeKeyJson(sk StoreKey, jsonData any) (err error)
		MergeKeyJsonBase64(sk StoreKey, b64 string) (err error)
		CalculateKeyValue(sk StoreKey, expression string) (address StoreAddress, modified bool, err error)
	}
)

const (
	SetExMustExist SetExFlags = 1 << iota
	SetExMustNotExist
	SetExNoValueUpdate
)
