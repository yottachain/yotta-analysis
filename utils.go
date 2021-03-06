package ytanalysis

import (
	"bytes"
	"fmt"
	"reflect"

	cyp "github.com/libp2p/go-libp2p-core/crypto"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/mr-tron/base58"
)

//DBName create DB name by miner ID
func DBName(snCount int64, minerID int32) string {
	snID := minerID % int32(snCount)
	return fmt.Sprintf("%s%d", YottaDB, snID)
}

// EqualSorted check if two arrays are equal
func EqualSorted(listA, listB interface{}) (ok bool) {
	if listA == nil || listB == nil {
		return listA == listB
	}
	aKind := reflect.TypeOf(listA).Kind()
	bKind := reflect.TypeOf(listB).Kind()
	if aKind != reflect.Array && aKind != reflect.Slice {
		return false
	}
	if bKind != reflect.Array && bKind != reflect.Slice {
		return false
	}
	aValue := reflect.ValueOf(listA)
	bValue := reflect.ValueOf(listB)
	if aValue.Len() != bValue.Len() {
		return false
	}
	// Mark indexes in bValue that we already used
	visited := make([]bool, bValue.Len())
	for i := 0; i < aValue.Len(); i++ {
		element := aValue.Index(i).Interface()
		found := false
		for j := 0; j < bValue.Len(); j++ {
			if visited[j] {
				continue
			}

			if ObjectsAreEqual(bValue.Index(j).Interface(), element) {
				visited[j] = true
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// ObjectsAreEqual check if two objects are equal
func ObjectsAreEqual(expected, actual interface{}) bool {
	if expected == nil || actual == nil {
		return expected == actual
	}
	if exp, ok := expected.([]byte); ok {
		act, ok := actual.([]byte)
		if !ok {
			return false
		} else if exp == nil || act == nil {
			return exp == nil && act == nil
		}
		return bytes.Equal(exp, act)
	}
	return reflect.DeepEqual(expected, actual)

}

// Max select maximum value
func Max(num ...int64) int64 {
	max := num[0]
	for _, v := range num {
		if v > max {
			max = v
		}
	}
	return max
}

// Min select minimum value
func Min(num ...int64) int64 {
	min := num[0]
	for _, v := range num {
		if v < min {
			min = v
		}
	}
	return min
}

// IDFromPublicKey generate node ID from publickey
func IDFromPublicKey(publicKey string) (string, error) {
	bytes, err := base58.Decode(publicKey)
	if err != nil {
		return "", err
	}
	rawpk, err := cyp.UnmarshalSecp256k1PublicKey(bytes[0:33])
	if err != nil {
		return "", err
	}
	id, err := peer.IDFromPublicKey(rawpk)
	return id.Pretty(), nil
}
