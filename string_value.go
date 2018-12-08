package go_redistest

import (
	"fmt"
)

type String = string

type RedisStringValue struct {
	Value string
	Expirable
}

func (db *RedisDB) GET(key string) (string, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	isStringKey, err := db.validateKeyType(key, StringKeyType)
	if err != nil {
		return "", err
	}

	if !isStringKey {
		return "", fmt.Errorf("wrong key type")
	}

	value := db.StringKeys[key]
	if value == nil {
		return "", fmt.Errorf("key not found")
	}

	if value.IsExpired() {
		// db.RemoveKey(key)
		return "", fmt.Errorf("key not found")
	}

	return value.Value, nil
}

func (db *RedisDB) SET(key string, value string) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	keyType := db.locateKey(key)
	// if the key exists and is not string
	if keyType != "" && keyType != StringKeyType {
		return fmt.Errorf("wrong key type")
	}

	db.StringKeys[key] = &RedisStringValue{
		Value:value,
	}

	return nil
}
