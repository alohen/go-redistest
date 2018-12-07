package go_redistest

import "time"

type Expirable interface{
	IsExpired() bool
	TTL() int64
	PTTL() int64
	SetTTL(time.Duration)
	SetExpirationTime(time.Time)
	RemoveTTL()
}

type expirationHandler struct {
	ExpirationTime time.Time
}

// Checks whether the value is expired
func (this *expirationHandler) IsExpired() bool {
	// if the expiration time wasn't set, they key can't be expired
	if this.ExpirationTime.IsZero() {
		return false
	}

	if time.Now().After(this.ExpirationTime) {
		return true
	}

	return false
}

// GetTTLInSeconds return the amount of milliseconds left until expiration time
func (this *expirationHandler) GetTTLInSeconds() int64 {
	timeLeft := time.Until(this.ExpirationTime)
	return int64(timeLeft.Seconds())
}

func (this *expirationHandler) GetTTLInMilliseconds() int64 {
	timeLeft := time.Until(this.ExpirationTime)
	return timeLeft.Nanoseconds() / 1000000
}

func (this *expirationHandler) SetTTL(duration time.Duration) {
	this.ExpirationTime = time.Now().Add(duration)
}

func (this *expirationHandler) SetExpirationTime(expirationTime time.Time) {
	this.ExpirationTime = expirationTime
}

func (this *expirationHandler) RemoveTTL() {
	this.ExpirationTime = time.Time{}
}
