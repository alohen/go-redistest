package go_redistest

type List = []string

type RedisListValue struct {
	Value List
	Expirable
}
