//nolint:gosec
package internal

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"

	"github.com/dgryski/go-farm"
)

func FARM_FINGERPRINT(v []byte) (Value, error) {
	return IntValue(farm.Fingerprint64(v)), nil
}

func MD5(v []byte) (Value, error) {
	sum := md5.Sum(v)
	return BytesValue(sum[:]), nil
}

func SHA1(v []byte) (Value, error) {
	sum := sha1.Sum(v)
	return BytesValue(sum[:]), nil
}

func SHA256(v []byte) (Value, error) {
	sum := sha256.Sum256(v)
	return BytesValue(sum[:]), nil
}

func SHA512(v []byte) (Value, error) {
	sum := sha512.Sum512(v)
	return BytesValue(sum[:]), nil
}
