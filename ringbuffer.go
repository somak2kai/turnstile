package turnstile

import "iter"

// ringbuffer is a fixed bounded data structure.
type ringbuffer[T any] struct {
	queue []T
	head  int // points to front of buffer.
	count int // current number of elements stored.
}

func newringbuffer[T any](cap int) *ringbuffer[T] {

	return &ringbuffer[T]{
		queue: make([]T, cap),
	}
}

func (r *ringbuffer[T]) size() int {
	return r.count
}

// add provided element to the back of the buffer.
// false response indicates push failed since buffer is full.
func (r *ringbuffer[T]) push(t T) bool {
	size := len(r.queue)
	if r.count == size {
		return false
	}
	r.queue[(r.head+r.count)%size] = t
	r.count++
	return true
}

// remove and return front of the buffer.
func (r *ringbuffer[T]) pop() (T, bool) {

	var zero T
	if r.count == 0 { // no elements in yet or buffer empty.
		return zero, false
	}
	t := r.queue[r.head]
	r.queue[r.head] = zero
	r.count--
	r.head = (r.head + 1) % len(r.queue)
	return t, true
}

// remove and return the ith element from the buffer.
func (r *ringbuffer[T]) removeAt(indx int) (T, bool) {
	var zero T
	if r.count == 0 {
		return zero, false // buffer is empty.
	}
	size := len(r.queue)
	idx := (r.head + indx) % size
	t := r.queue[idx]

	//everything after idx needs to move a step ahead.
	for i := idx; i < r.count-1; i++ {
		r.queue[(r.head+i)%size] = r.queue[(r.head+i+1)%size]
	}

	// zeroise the final element, since thats a hole after previous loop ran
	finalIdx := (r.head + (r.count - 1)) % size
	r.queue[finalIdx] = zero

	r.count--
	return t, true
}

func (r *ringbuffer[T]) all() iter.Seq2[int, T] {
	return func(yield func(int, T) bool) {
		for i := 0; i < r.count; i++ {
			if !yield(i, r.queue[(r.head+i)%len(r.queue)]) {
				return
			}
		}
	}
}
