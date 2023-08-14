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
		// Closes the connection to the TreeStore server, if one is open.
		Close() error

		// Configures the TSClient instance to use a specific server/port on the
		// next API call.
		SetServer(host string, port int)

		// Set a key without a value and without an expiration, doing nothing if the
		// key already exists. The key index is not altered.
		SetKey(sk StoreKey) (address StoreAddress, exists bool, err error)

		// Set a key with a value, without an expiration, adding to value history if the
		// key already exists.
		SetKeyValue(sk StoreKey, value any) (address StoreAddress, firstValue bool, err error)

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
		SetKeyValueEx(sk StoreKey, value any, flags SetExFlags, expire *time.Time, relationships []StoreAddress) (address StoreAddress, exists bool, originalValue any, err error)

		// Looks up the key in the index and returns true if it exists and has value history.
		IsKeyIndexed(sk StoreKey) (address StoreAddress, exists bool, err error)

		// Walks the tree level by level and returns the current address, whether or not
		// the key path is indexed. This avoids putting a lock on the index, but will lock
		// tree levels while walking the tree.
		LocateKey(sk StoreKey) (address StoreAddress, exists bool, err error)

		// Navigates to the valueInstance key node and returns the expiration time in Unix nanoseconds, or
		// -1 if the key path does not exist.
		GetKeyTtl(sk StoreKey) (ttl *time.Time, err error)

		// Navigates to the valueInstance key node and sets the expiration time in Unix nanoseconds.
		// Specify 0 for no expiration.
		SetKeyTtl(sk StoreKey, expiration *time.Time) (exists bool, err error)

		// Looks up the key in the index and returns the current value and flags
		// that indicate if the key was set, and if so, if it has a value.
		GetKeyValue(sk StoreKey) (value any, keyExists, valueExists bool, err error)

		// Looks up the key and returns the expiration time in Unix nanoseconds, or
		// -1 if the key value does not exist.
		GetKeyValueTtl(sk StoreKey) (ttl *time.Time, err error)

		// Looks up the key and sets the expiration time in Unix nanoseconds. Specify
		// 0 to clear the expiration.
		SetKeyValueTtl(sk StoreKey, expiration *time.Time) (exists bool, err error)

		// Looks up the key in the index and scans history for the specified Unix ns tick,
		// returning the value at that moment in time, if one exists.
		//
		// To specify a relative time, specify `tickNs` as the negative ns from the current
		// time, e.g., -1000000000 is one second ago.
		GetKeyValueAtTime(sk StoreKey, when *time.Time) (value any, exists bool, err error)

		// Deletes an indexed key that has a value, including its value history, and its metadata.
		// Specify `clean` as `true` to delete parent key nodes that become empty, or `false` to only
		// remove the valueInstance key node.
		//
		// Returns `removed` == true if the value was deleted.
		//
		// The valueInstance key will still exist if it has children or if it is the sentinel key node.
		DeleteKeyWithValue(sk StoreKey, clean bool) (removed bool, originalValue any, err error)

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
		DeleteKey(sk StoreKey) (keyRemoved, valueRemoved bool, originalValue any, err error)

		// Sets a metadata attribute on a key, returning the original value (if any)
		SetMetadataAttribute(sk StoreKey, attribute, value string) (keyExists bool, priorValue string, err error)

		// Removes a single metadata attribute from a key
		ClearMetdataAttribute(sk StoreKey, attribute string) (attributeExists bool, originalValue string, err error)

		// Discards all metdata on the specific key
		ClearKeyMetdata(sk StoreKey) (err error)

		// Fetches a key's metadata value for a specific attribute
		GetMetadataAttribute(sk StoreKey, attribute string) (attributeExists bool, value string, err error)

		// Returns an array of attribute names of metadata stored for the specified key
		GetMetadataAttributes(sk StoreKey) (attributes []string, err error)

		// Converts an address to a store key
		KeyFromAddress(addr StoreAddress) (sk StoreKey, exists bool, err error)

		// Fetches the current value by address
		KeyValueFromAddress(addr StoreAddress) (keyExists, valueExists bool, sk StoreKey, value any, err error)

		// Retreives a value by following a relationship link. The target value is
		// returned in `rv`, and will be nil if the target doesn't exist. The
		// `hasLink` flag indicates true when a relationship is stored at the
		// specified `relationshipIndex`.
		GetRelationshipValue(sk StoreKey, relationshipIndex int) (hasLink bool, rv *RelationshipValue, err error)

		// Navigates to the specified store key and returns all of the key segments
		// matching the simple wildcard `pattern`. If the store key does not exist,
		// the return `keys` will be nil.
		//
		// Memory is allocated up front to hold `limit` keys, so be careful to pass
		// a reasonable limit.
		GetLevelKeys(sk StoreKey, pattern string, startAt, limit int) (keys []LevelKey, err error)

		// Full iteration function walks each tree store level according to skPattern and returns every
		// detail of matching keys.
		GetMatchingKeys(skPattern StoreKey, startAt, limit int) (keys []*KeyMatch, err error)

		// Full iteration function walks each tree store level according to skPattern and returns every
		// detail of matching keys that have values.
		GetMatchingKeyValues(skPattern StoreKey, startAt, limit int) (values []*KeyValueMatch, err error)

		// Serialize the tree store into a single JSON doc.
		//
		// N.B., The document is constructed entirely in memory and will hold an
		// exclusive lock during the operation.
		Export(sk StoreKey) (jsonData any, err error)

		// Serialize the tree store into a single JSON doc.
		//
		// N.B., The document is constructed entirely in memory and will hold an
		// exclusive lock during the operation.
		ExportBase64(sk StoreKey) (b64 string, err error)

		// Creates a key from an export format json doc and adds it to the tree store
		// at the specified sk. If the key exists, it and its children will be replaced.
		Import(sk StoreKey, jsonData any) (err error)

		// Creates a key from an export format json doc and adds it to the tree store
		// at the specified sk. If the key exists, it and its children will be replaced.
		ImportBase64(sk StoreKey, b64 string) (err error)

		// Retrieves the child key tree and leaf values in the form of json. If
		// metdata "array" is "true" then the child key nodes are treated as
		// array indicies. (They must be big endian uint32.)
		GetKeyAsJson(sk StoreKey) (jsonData any, err error)

		// Retrieves the child key tree and leaf values in the form of json. If
		// metdata "array" is "true" then the child key nodes are treated as
		// array indicies. (They must be big endian uint32.)
		//
		// This entry point is useful for code that will unmarshal the json into
		// a specific struct.
		GetKeyAsJsonBytes(sk StoreKey) (jsonData []byte, err error)

		// Retrieves the child key tree and leaf values in the form of json. If
		// metdata "array" is "true" then the child key nodes are treated as
		// array indicies. (They must be big endian uint32.)
		GetKeyAsJsonBase64(sk StoreKey) (b64 string, err error)

		// Takes the generalized json data and stores it at the specified key path.
		// If the sk exists, its value, children and history are deleted, and the new
		// json data takes its place.
		SetKeyJson(sk StoreKey, jsonData any) (replaced bool, err error)

		// Takes the generalized json data and stores it at the specified key path.
		// If the sk exists, its value, children and history are deleted, and the new
		// json data takes its place.
		SetKeyJsonBase64(sk StoreKey, b64 string) (replaced bool, err error)

		// Takes the generalized json data and stores it at the specified key path.
		// If the sk exists, no changes are made. Otherwise a new key node is created
		// with its child data set according to the json structure.
		CreateKeyJson(sk StoreKey, jsonData any) (created bool, err error)

		// Takes the generalized json data and stores it at the specified key path.
		// If the sk exists, no changes are made. Otherwise a new key node is created
		// with its child data set according to the json structure.
		CreateKeyJsonBase64(sk StoreKey, b64 string) (created bool, err error)

		// Takes the generalized json data and stores it at the specified key path.
		// If the sk doesn't exists, no changes are made. Otherwise the key node's
		// value and children are deleted, and the new json data takes its place.
		ReplaceKeyJson(sk StoreKey, jsonData any) (replaced bool, err error)

		// Takes the generalized json data and stores it at the specified key path.
		// If the sk doesn't exists, no changes are made. Otherwise the key node's
		// value and children are deleted, and the new json data takes its place.
		ReplaceKeyJsonBase64(sk StoreKey, b64 string) (replaced bool, err error)

		// Overlays json data on top of existing data. This is one of the slower APIs
		// because each part of json is independently written to the store, and a
		// write lock is required across the whole operation.
		MergeKeyJson(sk StoreKey, jsonData any) (err error)

		// Overlays json data on top of existing data. This is one of the slower APIs
		// because each part of json is independently written to the store, and a
		// write lock is required across the whole operation.
		MergeKeyJsonBase64(sk StoreKey, b64 string) (err error)

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
		CalculateKeyValue(sk StoreKey, expression string) (address StoreAddress, modified bool, err error)
	}
)

const (
	SetExMustExist SetExFlags = 1 << iota
	SetExMustNotExist
	SetExNoValueUpdate
)
