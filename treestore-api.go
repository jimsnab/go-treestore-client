package treestore_client

import (
	"time"

	"github.com/jimsnab/go-treestore"
)

type (
	TokenSegment treestore.TokenSegment
	TokenPath treestore.TokenPath
	TokenSet treestore.TokenSet
	StoreKey treestore.StoreKey
	StoreAddress treestore.StoreAddress
	SetExFlags treestore.SetExFlags
	RelationshipValue struct {
		Sk           StoreKey
		CurrentValue []byte
	}
	LevelKey struct {
		Segment     TokenSegment 
		HasValue    bool         
		HasChildren bool         
	}
	KeyMatch struct {
		Key TokenPath
		Metadata      map[string]string 
		HasValue      bool              
		HasChildren   bool              
		CurrentValue  []byte               
		Relationships []StoreAddress    
	}
	KeyValueMatch struct {
		Key TokenPath
		Metadata      map[string]string 
		HasChildren   bool              
		CurrentValue  []byte               
		Relationships []StoreAddress    
	}

	TSClient interface {
		Close() error
		SetServer(host string, port int)
		SetKey(sk StoreKey) (address StoreAddress, exists bool, err error)
		SetKeyValue(sk StoreKey, value []byte) (address StoreAddress, firstValue bool, err error)
		SetKeyValueEx(sk StoreKey, value []byte, flags SetExFlags, expire *time.Time, relationships []StoreAddress) (address StoreAddress, exists bool, originalValue []byte, err error)
		IsKeyIndexed(sk StoreKey) (address StoreAddress, exists bool, err error)
		LocateKey(sk StoreKey) (address StoreAddress, exists bool, err error)
		GetKeyTtl(sk StoreKey) (ttl *time.Time, err error)
		SetKeyTtl(sk StoreKey, expiration *time.Time) (exists bool, err error)
		GetKeyValue(sk StoreKey) (value []byte, keyExists, valueExists bool, err error)
		GetKeyValueTtl(sk StoreKey) (ttl *time.Time, err error)
		SetKeyValueTtl(sk StoreKey, expiration *time.Time) (exists bool, err error)
		GetKeyValueAtTime(sk StoreKey, when *time.Time) (value []byte, exists bool, err error)
		DeleteKeyWithValue(sk StoreKey, clean bool) (removed bool, originalValue []byte, err error)
		DeleteKey(sk StoreKey) (keyRemoved, valueRemoved bool, originalValue []byte, err error)
		SetMetadataAttribute(sk StoreKey, attribute, value string) (keyExists bool, priorValue string, err error)
		ClearMetdataAttribute(sk StoreKey, attribute string) (attributeExists bool, originalValue string, err error)
		ClearKeyMetdata(sk StoreKey) (err error)
		GetMetadataAttribute(sk StoreKey, attribute string) (attributeExists bool, value string, err error)
		GetMetadataAttributes(sk StoreKey) (attributes []string, err error)
		KeyFromAddress(addr StoreAddress) (sk StoreKey, exists bool, err error)
		KeyValueFromAddress(addr StoreAddress) (keyExists, valueExists bool, sk StoreKey, value []byte, err error)
		GetRelationshipValue(sk StoreKey, relationshipIndex int) (hasLink bool, rv *RelationshipValue, err error)
		GetLevelKeys(sk StoreKey, pattern string, startAt, limit int) (keys []LevelKey, err error)
		GetMatchingKeys(skPattern StoreKey, startAt, limit int) (keys []*KeyMatch, err error)
		GetMatchingKeyValues(skPattern StoreKey, startAt, limit int) (values []*KeyValueMatch, err error)
	}
)

const (
	SetExMustExist SetExFlags = 1 << iota
	SetExMustNotExist
	SetExNoValueUpdate
)