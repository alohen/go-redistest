package go_redistest

type Set = []string

type RedisSetValue struct {
	Value Set
	Expirable
}
