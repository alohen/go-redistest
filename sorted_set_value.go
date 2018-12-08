package go_redistest

type SortedSet = []SetKey

type SetKey struct {
	Key   string
	Score int
}

type RedisSortedSetValue struct {
	Value SortedSet
	Expirable
}
