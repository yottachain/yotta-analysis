package ytanalysis

import (
	"math/rand"
	"sort"
	"sync"
	"time"
)

//Int32Set set with int64 elements
type Int32Set struct {
	Slice []int32
	lock  sync.RWMutex
}

//Add sorted insert an element
func (set *Int32Set) Add(n int32) bool {
	set.lock.Lock()
	defer set.lock.Unlock()
	size := len(set.Slice)
	index := sort.Search(size, func(i int) bool { return set.Slice[i] > n })
	if index != 0 && set.Slice[index-1] == n {
		return false
	}
	set.Slice = append(set.Slice, n)
	copy(set.Slice[index+1:], set.Slice[index:])
	set.Slice[index] = n
	return true
}

//Delete delete an element
func (set *Int32Set) Delete(n int32) bool {
	set.lock.Lock()
	defer set.lock.Unlock()
	size := len(set.Slice)
	index := sort.Search(size, func(i int) bool { return set.Slice[i] == n })
	if index == size {
		return false
	}
	copy(set.Slice[index:], set.Slice[index+1:])
	set.Slice = set.Slice[0 : size-1]
	return true
}

//RandomDelete random delete an element
func (set *Int32Set) RandomDelete() int32 {
	set.lock.Lock()
	defer set.lock.Unlock()
	size := len(set.Slice)
	if size == 0 {
		return -1
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	index := r.Int31n(int32(size))
	n := set.Slice[index]
	copy(set.Slice[index:], set.Slice[index+1:])
	set.Slice = set.Slice[0 : size-1]
	return n
}
