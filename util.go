package remhelp

import (
	"crypto/rand"
	"encoding/hex"
)

func sel[T any](b bool, x, y T) T {
	if b {
		return x
	}
	return y
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(b), nil
}
