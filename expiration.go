package go_redistest

import "time"

type Expirable interface{
	IsExpired() bool
	TTL() int64
	SetTTL(time.Duration)
}

type expirationHandler struct {
	ExpirationTime time.Time
}

// Checks whether the value is expired
func (this *expirationHandler) IsExpired() bool {
	// if the expiration time wasn't set, they key can't be expired
	if value.ExpirationTime.IsZero() {
		return false
	}

	if time.Now().After(value.ExpirationTime) {
		return true
	}

	return false
}

// TTL return the amount of milliseconds left until expiration time
func (this *expirationHandler) TTL() int64 {
	timeLeft := time.Until(this.ExpirationTime)
	millisecondsLeft := timeLeft.Nanoseconds() / 1000000
	return millisecondsLeft
}

func (this *expirationHandler) SetTTL(duration time.Duration) {
	this.ExpirationTime = time.Now().Add(duration)
}
