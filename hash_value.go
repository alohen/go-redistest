package go_redistest

import (
	"fmt"
	"go/types"
)

type Hash = map[string]string

type RedisHashValue struct {
	Value Hash
	Expirable
}

func (db *RedisDB) HDEL(key string, hashes ...string) (int64, error) {
	keyType := db.locateKey(key)
	if keyType == "" {
		return 0, nil
	}

	if keyType != HashKeyType {
		return 0, fmt.Errorf("wrong key type")
	}

	storedValue := db.HashKeys[key].Value
	var hashesRemoved int64 = 0

	for _, hash := range hashes {
		if _, exists := storedValue[hash]; exists {
			delete(storedValue, hash)
			hashesRemoved++
		}
	}

	return hashesRemoved, nil
}

func (db *RedisDB) HEXISTS(key string, field string) (int64, error) {
	keyType := db.locateKey(key)
	if keyType == "" {
		return 0, nil
	}

	if keyType != HashKeyType {
		return 0, fmt.Errorf("wrong key type")
	}

	if _, exists := db.HashKeys[key].Value[field]; !exists {
		return 0, nil
	}

	return 1, nil
}

func (db *RedisDB) HSET(key, field, value string) (int, error) {
	returnValue := 0
	keyType := db.locateKey(key)
	if keyType != HashKeyType && keyType != "" {
		return 0, fmt.Errorf("wrong key type")
	}

	if storedValue, keyExists := db.HashKeys[key]; keyExists {
		if _, fieldExists := storedValue.Value[field]; fieldExists {
			returnValue = 1
		}
	}
	returnValue = 0

	db.HashKeys[key].Value[field] = value
	return returnValue, nil
}

func (db *RedisDB) HGET(key, field, value string) (string, error) {
	keyType := db.locateKey(key)
	if keyType != HashKeyType && keyType != "" {
		return "(nil)", fmt.Errorf("wrong key type")
	}

	storedValue, keyExists := db.HashKeys[key]
	if!keyExists {
		return "(nil)", nil
	}

	fieldValue, fieldExists := storedValue.Value[field]
	if !fieldExists {
		return "(nil)", nil
	}
	return fieldValue, nil
}
