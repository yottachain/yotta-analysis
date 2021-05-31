package ytanalysis

import (
	"math/rand"
	"sort"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

//Int32Set set with int64 elements
type Int32Set struct {
	Slice []int32
	lock  sync.RWMutex
}

//Add sorted insert an element
func (set *Int32Set) Add(n int32) bool {
	entry := log.WithFields(log.Fields{Function: "Add", Element: n})
	set.lock.Lock()
	defer set.lock.Unlock()
	size := len(set.Slice)
	index := sort.Search(size, func(i int) bool { return set.Slice[i] > n })
	if index != 0 && set.Slice[index-1] == n {
		entry.Debugf("duplicate element, size: %d", size)
		return false
	}
	set.Slice = append(set.Slice, n)
	copy(set.Slice[index+1:], set.Slice[index:])
	set.Slice[index] = n
	entry.Debugf("add element, size: %d", size+1)
	return true
}

//Delete delete an element
func (set *Int32Set) Delete(n int32) bool {
	entry := log.WithFields(log.Fields{Function: "Delete", Element: n})
	set.lock.Lock()
	defer set.lock.Unlock()
	size := len(set.Slice)
	index := sort.Search(size, func(i int) bool { return set.Slice[i] == n })
	if index == size {
		entry.Debugf("no element, size: %d", size)
		return false
	}
	copy(set.Slice[index:], set.Slice[index+1:])
	set.Slice = set.Slice[0 : size-1]
	entry.Debugf("delete element, size: %d", size-1)
	return true
}

//RandomDelete random delete an element
func (set *Int32Set) RandomDelete() int32 {
	entry := log.WithFields(log.Fields{Function: "RandomDelete"})
	set.lock.Lock()
	defer set.lock.Unlock()
	size := len(set.Slice)
	if size == 0 {
		return -1
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	index := r.Int31n(int32(size))
	n := set.Slice[index]
	entry.Debugf("random delete element %d, size: %d", n, size-1)
	copy(set.Slice[index:], set.Slice[index+1:])
	set.Slice = set.Slice[0 : size-1]
	return n
}
