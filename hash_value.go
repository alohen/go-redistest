package go_redistest

type Hash = map[string]string

type RedisHashValue struct {
	Value Hash
	Expirable
}
