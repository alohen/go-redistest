package go_redistest

import (
	"fmt"
	"sync"
	"time"
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

func (db *RedisDB) EXPIRE(key string, seconds int) error {
	return db.setKeyExpiration(key, time.Duration(seconds) * time.Second)
}



// utility functions
func (db *RedisDB) setKeyExpiration(key string, duration time.Duration) error {
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
	var exists bool
	var keyType KeyType = -1

	if _, exists = db.StringKeys[key]; exists {
		keyType = StringKeyType
	}
	if _, exists = db.ListKeys[key]; exists {
		keyType =  ListKeyType
	}
	if _, exists = db.HashKeys[key]; exists {
		keyType = HashKeyType
	}
	if _, exists = db.SetKeys[key]; exists {
		keyType =  SetKeyType
	}
	if _, exists = db.SortedSetKeys[key]; exists {
		keyType = SortedSetKeyType
	}

	if db.deleteKeyIfExpired(key,keyType) {
		return -1
	}

	return keyType
}

func (db *RedisDB) deleteKeyIfExpired(key string, keyType KeyType) bool {
	value := db.getValueAsExpirable(key,keyType)
	if value == nil {
		return false
	}

	if value.IsExpired() {
		db.deleteKeyByType(key,keyType)
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
