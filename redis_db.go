package go_redistest

import (
	"fmt"
	"sync"
	"time"
	"path"
	"path/filepath"
	"github.com/gobwas/glob"
)

// Key Type enumeration
type KeyType int

const (
	StringKeyType    KeyType = 0
	ListKeyType      KeyType = 1
	HashKeyType      KeyType = 2
	SetKeyType       KeyType = 3
	SortedSetKeyType KeyType = 4
)

type RedisDB struct {
	StringKeys    map[string]*RedisStringValue
	ListKeys      map[string]*RedisListValue
	HashKeys      map[string]*RedisHashValue
	SetKeys       map[string]*RedisSetValue
	SortedSetKeys map[string]*RedisSortedSetValue
	keysToTypes   map[string]KeyType
	lock          sync.Mutex
}

func (db *RedisDB) DEL(key string) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	keyType := db.locateKey(key)
	if keyType == -1 {
		fmt.Errorf("key is missing")
	}

	db.deleteKeyByType(key, keyType)
	return nil
}

func (db *RedisDB) EXISTS(key string) bool {
	return db.locateKey(key) != -1
}

func (db *RedisDB) EXPIRE(key string, seconds int) int64 {
	if err := db.setTTL(key, time.Duration(seconds)*time.Second); err != nil {
		return -1
	}
	return 0
}

func (db *RedisDB) EXPIREAT(key string, timestamp int64) int64 {
	expirationTime := time.Unix(timestamp,0)
	if err := db.setExpirationTime(key, expirationTime) ;err != nil {
		return -1
	}
	return 0
}

func (db *RedisDB) PEXPIRE(key string, milliseconds int) int64 {
	if err := db.setTTL(key, time.Duration(milliseconds)*time.Millisecond); err != nil {
		return -1
	}

	return 0
}

func (db *RedisDB) PEAPIREAT(key string, timestamp int64) int64 {
	expirationTime := time.Unix(0,timestamp * 1000000)
	if err := db.setExpirationTime(key, expirationTime) ;err != nil {
		return -1
	}
	return 0
}

func (db *RedisDB) KEYS(globString string) []string {
	var matchedKeys []string
	searchGlob := glob.MustCompile(globString)
	for key := range db.keysToTypes {
		if searchGlob.Match(key) {
			matchedKeys = append(matchedKeys, key)
		}
	}

	return matchedKeys
}

func (db *RedisDB) TTL(key string) int64 {
	keyType := db.locateKey(key)
	if keyType == -1 {
		return -1
	}

	value := db.getValueAsExpirable(key, keyType)
	return value.TTL()
}

func (db *RedisDB) PTTL(key string) int64 {
	keyType := db.locateKey(key)
	if keyType == -1 {
		return -1
	}

	value := db.getValueAsExpirable(key, keyType)
	return value.PTTL()
}

func (db *RedisDB) PERSIST(key string) int64 {
	keyType := db.locateKey(key)
	if keyType == -1 {
		return 0
	}

	value := db.getValueAsExpirable(key,keyType)
	value.RemoveTTL()

	return 1
}


// utility functions
func (db *RedisDB) setTTL(key string, duration time.Duration) error {
	keyType := db.locateKey(key)
	if keyType == -1 {
		return fmt.Errorf("missing key")
	}

	value := db.getValueAsExpirable(key, keyType)
	if value == nil {
		return fmt.Errorf("missing key")
	}
	value.SetTTL(duration)

	return nil
}

func (db *RedisDB) setExpirationTime(key string, duration time.Time) error {
	keyType := db.locateKey(key)
	if keyType == -1 {
		return fmt.Errorf("missing key")
	}

	value := db.getValueAsExpirable(key, keyType)
	if value == nil {
		return fmt.Errorf("missing key")
	}
	value.SetExpirationTime(duration)

	return nil
}

func (db *RedisDB) getValueAsExpirable(key string, keyType KeyType) Expirable {
	switch keyType {
	case StringKeyType:
		return db.StringKeys[key]
	case ListKeyType:
		return db.ListKeys[key]
	case HashKeyType:
		return db.HashKeys[key]
	case SetKeyType:
		return db.SetKeys[key]
	case SortedSetKeyType:
		return db.SortedSetKeys[key]
	}

	return nil
}

func (db *RedisDB) deleteKeyByType(key string, keyType KeyType) {
	switch keyType {
	case StringKeyType:
		delete(db.StringKeys, key)
	case ListKeyType:
		delete(db.ListKeys, key)
	case HashKeyType:
		delete(db.HashKeys, key)
	case SetKeyType:
		delete(db.SetKeys, key)
	case SortedSetKeyType:
		delete(db.SortedSetKeys, key)
	}
}

func (db *RedisDB) locateKey(key string) (KeyType) {
	keyType, exists := db.keysToTypes[key]
	if !exists {
		return -1
	}

	if db.deleteKeyIfExpired(key, keyType) {
		return -1
	}

	return keyType
}

func (db *RedisDB) deleteKeyIfExpired(key string, keyType KeyType) bool {
	value := db.getValueAsExpirable(key, keyType)
	if value == nil {
		return false
	}

	if value.IsExpired() {
		db.deleteKeyByType(key, keyType)
		return true
	}

	return false
}

func (db *RedisDB) validateKeyType(key string, keyType KeyType) (bool, error) {
	typeInDB := db.locateKey(key)
	if keyType == -1 {
		return false, fmt.Errorf("key not found")
	}

	return typeInDB != StringKeyType, nil
}
