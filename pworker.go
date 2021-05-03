// pWorker is a worker pool with fixed buffer size where submitted work items are assigned a partition.
// Work items of the same partition are processed serially in order of arrival.
package pworker

import (
	"container/list"
	"fmt"
)

// Input of worker pool, contains a partition id and the input which will be passed to the WorkerFunc.
type Work struct {
	partitionId int
	input       interface{}
}

// Output of the worker pool.
type Result struct {
	partitionId int
	input       interface{}
	err         error
	output      interface{}
}

// Function applied by the workers to the input (see Work.input).
type WorkerFunc func(arg interface{}) (ret interface{}, err error)

const minCapacity = 1
const minNumWorkers = 1

// Builds a pWorker with a given number of worker goroutines, a buffer with a given capacity and
// a function which the workers apply to work items in order to produce results.
//
// Closing the input channel gracefully shuts down the worker pool: all work items are processed before stopping.
//
// TODO add done channel for abrupt shutdown.
// TODO handle case of nil workerFunction.
func MakePWorker(numWorkers int, capacity int, workerFunction WorkerFunc) (chan<- Work, <-chan Result) {
	if capacity < minCapacity {
		capacity = minCapacity
	}
	if numWorkers < minNumWorkers {
		numWorkers = minNumWorkers
	}
	in := make(chan Work)
	out := make(chan Result)
	buf := newBuffer(capacity)
	pool := newWorkerPool(numWorkers, workerFunction)
	//Return worker input channel if buffer is not full, nil otherwise
	getInMaybe := func() <-chan Work {
		if buf.isFull() {
			return nil
		}
		return in
	}
	//Return worker pool input channel if buffer has work to submit, nil otherwise
	getPoolInMaybe := func() chan<- *Work {
		if buf.getNext() == nil {
			return nil
		}
		return pool.getIn()
	}
	//Control loop
	go func() {
		closed := false
		//Run until buffer has no more pending work and worker is closed
		for buf.hasPendingWork() || !closed {
			select {
			case w, ok := <-getInMaybe():
				if !ok {
					//If client closes worker input channel then graceful shutdown: wait for all work
					closed = true
					continue
				}
				//Work unit received
				//Add to buffer
				buf.appendWork(&w)
			case result := <-pool.getOut():
				//Tere is work ready to be delivered
				out <- *result
				//Once delivered remove from buffer
				buf.removeWork(result.partitionId)
			case getPoolInMaybe() <- buf.getNext():
				//Work submitted to the pool
				//Remove from buffer backlog
				buf.removeNext()
			}

		}
		//Input channel is closed and no pending work
		//Close worker pool input channel
		close(pool.in)
		//Close output channel
		close(out)
	}()
	return in, out
}

type buffer struct {
	//list *partition
	partitions *list.List
	//list *Work
	workBacklog *list.List
	capacity    int
	size        int
}

func newBuffer(capacity int) *buffer {
	return &buffer{
		partitions:  list.New(),
		workBacklog: list.New(),
		capacity:    capacity}
}

func (b *buffer) isFull() bool {
	return b.size >= b.capacity
}

func (b *buffer) hasPendingWork() bool {
	//a) A partition exists -> work in progress (be in buffer or pool) for that partition exists
	return b.partitions.Len() > 0
}
func (b *buffer) appendWork(work *Work) {
	//Search for partition
	var part *partition
	for el := b.partitions.Front(); el != nil; el = el.Next() {
		curPartition := el.Value.(*partition)
		if curPartition.id == work.partitionId {
			part = curPartition
			break
		}
	}
	if part == nil {
		//Create partition
		part = newPartition(work.partitionId)
		b.partitions.PushBack(part)
		//Add work to backlog
		b.workBacklog.PushBack(work)
	}
	//Add work
	part.addWork(work)
	//Increment buffer size
	b.size++
}
func (b *buffer) getNext() *Work {
	next := b.workBacklog.Front()
	if next == nil {
		return nil
	}
	p := next.Value.(*Work)
	return p
}
func (b *buffer) removeNext() {
	b.workBacklog.Remove(b.workBacklog.Front())
}

func (b *buffer) removeWork(partitionId int) {
	//Search for partition
	var part *partition
	var listElement *list.Element
	for el := b.partitions.Front(); el != nil; el = el.Next() {
		curPartition := el.Value.(*partition)
		if curPartition.id == partitionId {
			part = curPartition
			listElement = el
			break
		}
	}
	//Partition must exist and with at least one work element
	part.removeWork()
	if part.hasWork() {
		//Schedule next work item
		b.workBacklog.PushBack(part.peekWork())
	} else {
		//Remove partition
		b.partitions.Remove(listElement)
	}
	//Decrement buffer size
	b.size--
}

type partition struct {
	id int
	//TODO optimize with a queue-friendly data structure (linked list, ring buffer,...)
	workItems []*Work
}

func (p *partition) addWork(work *Work) {
	p.workItems = append(p.workItems, work)
}

func (p *partition) removeWork() {
	//Remove work from partition
	p.workItems[0] = nil
	p.workItems = p.workItems[1:]
}

func (p *partition) peekWork() *Work {
	return p.workItems[0]
}

func (p *partition) hasWork() bool {
	return len(p.workItems) > 0
}

func newPartition(id int) *partition {
	return &partition{
		id:        id,
		workItems: make([]*Work, 0, 1)}
}

type workerpool struct {
	in  chan *Work
	out chan *Result
}

func newWorkerPool(numWorkers int, function WorkerFunc) *workerpool {
	wp := &workerpool{
		in:  make(chan *Work),
		out: make(chan *Result),
	}
	for i := 0; i < numWorkers; i++ {
		go worker(i, wp.in, wp.out, function)
	}
	return wp
}

func (wp *workerpool) getIn() chan<- *Work {
	return wp.in
}

func (wp *workerpool) getOut() <-chan *Result {
	return wp.out
}

//TODO add done channel
func worker(workerId int, in <-chan *Work, out chan<- *Result, function WorkerFunc) {
	run := true
	for run {
		select {
		case w, ok := <-in:
			if !ok {
				//Stop
				run = false
				continue
			}
			ret, err := safeExecute(function, w.input)
			out <- &Result{
				partitionId: w.partitionId,
				input:       w.input,
				err:         err,
				output:      ret,
			}
		}
	}
}

func safeExecute(function WorkerFunc, arg interface{}) (ret interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			ret = nil
			err = fmt.Errorf("panic executing provided worker function: %v", r)
		}
	}()
	ret, err = function(arg)
	return ret, err
}
