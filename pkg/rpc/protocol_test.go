package rpc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeRequest(t *testing.T) {
	msg := Message{
		ReqId:   1,
		Kind:    KindRequest,
		Method:  "/fizzbuzz",
		Payload: []byte("{\"fizz\": \"buzz\"}"),
	}

	encoded := Encode(msg)
	decoded, err := Decode(encoded)
	require.NoError(t, err)

	assert.Equal(t, msg, decoded)
}

func TestEncodeDecodeResponse(t *testing.T) {
	msg := Message{
		ReqId:   1,
		Kind:    KindResponse,
		Method:  "/fizzbuzz",
		Payload: []byte("{\"fizz\": \"buzz\"}"),
	}

	encoded := Encode(msg)
	decoded, err := Decode(encoded)
	require.NoError(t, err)

	assert.Equal(t, msg, decoded)
}

func TestDecodingMalformedBytesErrors(t *testing.T) {
	msg := Message{
		ReqId:   1,
		Kind:    KindResponse,
		Method:  "/fizzbuzz",
		Payload: []byte("{\"fizz\": \"buzz\"}"),
	}

	encoded := Encode(msg)
	encoded = encoded[:15]
	_, err := Decode(encoded)
	require.Error(t, err)
}

func TestEncodeDecodeEmptyMethod(t *testing.T) {
	msg := Message{
		ReqId:   1,
		Kind:    KindResponse,
		Method:  "",
		Payload: []byte("{\"fizz\": \"buzz\"}"),
	}

	encoded := Encode(msg)
	decoded, err := Decode(encoded)
	require.NoError(t, err)

	assert.Equal(t, msg, decoded)
}

func TestEncodeDecodeLargePayload(t *testing.T) {
	msg := Message{
		ReqId:   1,
		Kind:    KindResponse,
		Method:  "",
		Payload: []byte(largeText),
	}

	encoded := Encode(msg)
	decoded, err := Decode(encoded)
	require.NoError(t, err)

	assert.Equal(t, msg, decoded)
}

const largeText = `’Twas brillig, and the slithy toves Did gyre and gimble in the wabe: All mimsy were the borogoves, And the mome raths outgrabe.

“Beware the Jabberwock, my son! The jaws that bite, the claws that catch! Beware the Jubjub bird, and shun The frumious Bandersnatch!”

He took his vorpal sword in hand; Long time the manxome foe he sought— So rested he by the Tumtum tree And stood awhile in thought.

And, as in uffish thought he stood, The Jabberwock, with eyes of flame, Came whiffling through the tulgey wood, And burbled as it came!

One, two! One, two! And through and through The vorpal blade went snicker-snack! He left it dead, and with its head He went galumphing back.

“And hast thou slain the Jabberwock? Come to my arms, my beamish boy! O frabjous day! Callooh! Callay!” He chortled in his joy.

’Twas brillig, and the slithy toves Did gyre and gimble in the wabe: All mimsy were the borogoves, And the mome raths outgrabe.`
