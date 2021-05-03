package pworker

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

//Test with capacity 1 and 1 worker: all of the work items are returned
func TestMakePWorkerSingleAllReceivedInOrder(t *testing.T) {
	in, out := MakePWorker(1, 1, func(arg interface{}) (interface{}, error) {
		i := arg.(int)
		if i%2 == 0 {
			return arg, nil
		} else {
			if i%3 == 0 {
				panic("panic")
			}
			return -1, errors.New("Some odd error")
		}
	})
	lastVal := -1
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for result := range out {
			input := result.input.(int)
			if input%2 == 0 {
				if input != result.output {
					t.Errorf("Unexpected output value; expected %d, got %v", input, result.output)
				}
			} else {
				if result.err == nil {
					t.Errorf("Error expected")
				}
			}
			if lastVal+1 != input {
				t.Errorf("Unexpected value; expected %d, got %d", lastVal+1, input)
			}
			lastVal = input
		}
		wg.Done()
		fmt.Println("finished reading")
	}()
	for i := 0; i < 10000; i++ {
		in <- Work{partitionId: 0, input: i}
	}
	close(in)
	fmt.Println("finished writing")
	wg.Wait()

	if lastVal != 9999 {
		t.Errorf("Didn't get all values, last one received was %d", lastVal)
	}
}

//Test with capacity and num of workers > 1: all of the work items are returned
func TestMakePWorkerMultipleAllReceivedInOrder(t *testing.T) {
	numPartitions := 100
	lastVals := make(map[int]int, numPartitions)
	in, out := MakePWorker(3, 30, func(arg interface{}) (interface{}, error) {
		i := arg.(int)
		if i%2 == 0 {
			return arg, nil
		} else {
			if i%3 == 0 {
				panic("panic")
			}
			return -1, errors.New("Some odd error")
		}
	})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for result := range out {
			lastVal, ok := lastVals[result.partitionId]
			if !ok {
				lastVal = -1
			}
			input := result.input.(int)
			if input%2 == 0 {
				if input != result.output {
					t.Errorf("Unexpected output value; expected %d, got %v", input, result.output)
				}
			} else {
				if result.err == nil {
					t.Errorf("Error expected")
				}
			}
			if lastVal+1 != input {
				t.Errorf("Unexpected value; expected %d, got %d", lastVal+1, input)
			}
			lastVals[result.partitionId] = input
		}
		wg.Done()
		fmt.Println("finished reading")
	}()
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	nextVals := make(map[int]int, numPartitions)
	for i := 0; i < 100000; i++ {
		pId := r1.Intn(numPartitions)
		v, ok := nextVals[pId]
		if !ok {
			v = 0
		}
		in <- Work{partitionId: pId, input: v}
		nextVals[pId] = v + 1
	}
	close(in)
	fmt.Println("finished writing")
	wg.Wait()
	for pId, lastVal := range lastVals {
		if lastVal != nextVals[pId]-1 {
			t.Errorf("Didn't get all values, last one received was %d instead of %d", lastVal, nextVals[pId]-1)
		}
	}
}

//Test that for a given partition work items are processed serially
type noConcurrencyInput struct {
	partitionId int
	value       int
}

func TestMakePWorkerMultipleNoConcurrencyInPartition(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	numPartitions := 2
	lastVals := make(map[int]int, numPartitions)
	//Pairs of mutexes per partition
	aLocks := sync.Map{}
	bLocks := sync.Map{}
	in, out := MakePWorker(numPartitions, 30,
		func(arg interface{}) (interface{}, error) {
			i := arg.(noConcurrencyInput)
			//If we replaced "i.partitionId" with a fixed index (e.g. 0) we could see this test hanging for deadlock:
			//items from different partitions are processed concurrently
			aLock, _ := aLocks.Load(i.partitionId)
			bLock, _ := bLocks.Load(i.partitionId)
			afirst := rand.Intn(2) == 0
			//For the same partition some items aquire A and then B, some others B and then A
			//If processing is done concurrently this most likely causes deadlock
			if afirst {
				aLock.(*sync.Mutex).Lock()
			} else {
				bLock.(*sync.Mutex).Lock()
			}
			//sleep average about 25 ms
			time.Sleep(time.Duration(12+rand.Intn(25)) * time.Millisecond)
			if afirst {
				bLock.(*sync.Mutex).Lock()
			} else {
				aLock.(*sync.Mutex).Lock()
			}
			aLock.(*sync.Mutex).Unlock()
			bLock.(*sync.Mutex).Unlock()
			if i.value%2 == 0 {
				return arg.(noConcurrencyInput).value, nil
			} else {
				if i.value%3 == 0 {
					panic("panic")
				}
				return -1, errors.New("Some odd error")
			}
		})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for result := range out {
			lastVal, ok := lastVals[result.partitionId]
			if !ok {
				lastVal = -1
			}
			input := result.input.(noConcurrencyInput)
			if input.value%2 == 0 {
				if input.value != result.output {
					t.Errorf("Unexpected output value; expected %d, got %v", input.value, result.output)
				}
			} else {
				if result.err == nil {
					t.Errorf("Error expected")
				}
			}
			if lastVal+1 != input.value {
				t.Errorf("Unexpected value; expected %d, got %d", lastVal+1, input.value)
			}
			lastVals[result.partitionId] = input.value
		}
		wg.Done()
		fmt.Println("finished reading")
	}()

	nextVals := make(map[int]int, numPartitions)
	for i := 0; i < 100; i++ {
		pId := rand.Intn(numPartitions)
		v, ok := nextVals[pId]
		if !ok {
			v = 0
			//Init mutexes
			aLocks.Store(pId, &sync.Mutex{})
			bLocks.Store(pId, &sync.Mutex{})
		}
		in <- Work{partitionId: pId, input: noConcurrencyInput{partitionId: pId, value: v}}
		nextVals[pId] = v + 1
	}
	close(in)
	fmt.Println("finished writing")
	wg.Wait()
	for pId, lastVal := range lastVals {
		if lastVal != nextVals[pId]-1 {
			t.Errorf("Didn't get all values, last one received was %d instead of %d", lastVal, nextVals[pId]-1)
		}
	}
}
