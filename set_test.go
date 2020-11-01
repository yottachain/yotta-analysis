package ytanalysis

import (
	"fmt"
	"testing"
)

func TestAdd(t *testing.T) {
	iset := new(Int32Set)
	iset.Add(3)
	iset.Add(1)
	iset.Add(9)
	iset.Add(9)
	fmt.Println(iset.Slice)

	n := iset.RandomDelete()
	fmt.Println(n)
	n = iset.RandomDelete()
	fmt.Println(n)
	n = iset.RandomDelete()
	fmt.Println(n)
}
