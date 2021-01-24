package Broker

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
)

const minQueueLen = 32

type Queue struct {
	items             map[int64]*Message
	ids               map[interface{}]int64
	buf               []int64
	head, tail, count int
	mutex             *sync.Mutex
	notEmpty          *sync.Cond
	// You can subscribe to this channel to know whether queue is not empty
	NotEmpty chan struct{}
}

func newQueue() *Queue {
	q := &Queue{
		items:    make(map[int64]*Message),
		ids:      make(map[interface{}]int64),
		buf:      make([]int64, minQueueLen),
		mutex:    &sync.Mutex{},
		NotEmpty: make(chan struct{}, 1),
	}

	q.notEmpty = sync.NewCond(q.mutex)

	return q
}

// Removes all elements from queue
func (q *Queue) Clean() {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	q.items = make(map[int64]*Message)
	q.ids = make(map[interface{}]int64)
	q.buf = make([]int64, minQueueLen)
}

// Returns the number of elements in queue
func (q *Queue) Length() int {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	return len(q.items)
}

func (q *Queue) PrintElem() {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	for _, v := range q.items {
		fmt.Print("[seq:", v.Seq, ", visibility:", strconv.Itoa(v.Visibility), "]")
	}
	println()
	println()
}

// resizes the queue to fit exactly twice its current contents
// this can result in shrinking if the queue is less than half-full
func (q *Queue) resize() {
	newCount := q.count << 1

	if q.count < 2<<18 {
		newCount = newCount << 2
	}

	newBuf := make([]int64, newCount)

	if q.tail > q.head {
		copy(newBuf, q.buf[q.head:q.tail])
	} else {
		n := copy(newBuf, q.buf[q.head:])
		copy(newBuf[n:], q.buf[:q.tail])
	}

	q.head = 0
	q.tail = q.count
	q.buf = newBuf
}

func (q *Queue) notify() {
	if len(q.items) > 0 {
		select {
		case q.NotEmpty <- struct{}{}:
		default:
		}
	}
}

// Adds one element at the back of the queue
func (q *Queue) Append(elem *Message) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.count == len(q.buf) {
		q.resize()
	}

	id := q.newId()
	q.items[id] = elem
	q.ids[elem] = id
	q.buf[q.tail] = id
	// bitwise modulus
	q.tail = (q.tail + 1) & (len(q.buf) - 1)
	q.count++

	q.notify()

	if q.count == 1 {
		q.notEmpty.Broadcast()
	}
}

func (q *Queue) newId() int64 {
	for {
		id := rand.Int63()
		_, ok := q.items[id]
		if id != 0 && !ok {
			return id
		}
	}
}

// Adds one element at the front of queue
func (q *Queue) Prepend(elem *Message) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.count == len(q.buf) {
		q.resize()
	}

	q.head = (q.head - 1) & (len(q.buf) - 1)
	id := q.newId()
	q.items[id] = elem
	q.ids[elem] = id
	q.buf[q.head] = id
	// bitwise modulus
	q.count++

	q.notify()

	if q.count == 1 {
		q.notEmpty.Broadcast()
	}
}

func (q *Queue) pop() int64 {
	for {
		if q.count <= 0 {
			q.notEmpty.Wait()
		}

		// I have no idea why, but sometimes it's less than 0
		if q.count > 0 {
			break
		}
	}

	id := q.buf[q.head]
	q.buf[q.head] = 0

	// bitwise modulus
	q.head = (q.head + 1) & (len(q.buf) - 1)
	q.count--
	if len(q.buf) > minQueueLen && (q.count<<1) == len(q.buf) {
		q.resize()
	}

	return id
}

func (q *Queue) Head() *Message {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	item := q.items[q.buf[q.head]]
	return item
}

// Pop removes and returns the element from the front of the queue.
// If the queue is empty, it will block
func (q *Queue) Pop() *Message {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	for {
		id := q.pop()

		item, ok := q.items[id]

		if ok {
			delete(q.ids, item)
			delete(q.items, id)
			q.notify()
			return item
		}
	}
}

// Removes one element from the queue
func (q *Queue) Remove(elem *Message) bool {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	id, ok := q.ids[elem]
	if !ok {
		return false
	}
	delete(q.ids, elem)
	delete(q.items, id)

	return true
}
