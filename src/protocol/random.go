package protocol

import (
	"crypto/rand"
	"encoding/binary"
)

// SecureUint32 generates a cryptographically secure random 32-bit unsigned integer.
// crypto/rand.Read never returns an error; on unrecoverable failure it panics.
func SecureUint32() uint32 {
	var b [4]byte
	rand.Read(b[:])
	return binary.BigEndian.Uint32(b[:])
}

// SecureSessionID generates a cryptographically secure non-zero SessionID.
// SessionID 0 is a protocol sentinel value (means "close entire tunnel"),
// so it must never be assigned to a real session.
func SecureSessionID() uint32 {
	id := SecureUint32()
	if id == 0 {
		id = 1
	}
	return id
}