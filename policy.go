package turnstile

import "math"

type fairnesspolicy[T any] interface {
	// Next takes one element from q. score(t) is the caller's measure of
	// how much t has consumed. Caller must hold q's lock.
	Next(q *ringbuffer[T], score func(T) float64) (T, bool)
}

// round robin fairness.
type roundrobin[T any] struct{}

func (roundrobin[T]) Next(q *ringbuffer[T], _ func(T) float64) (T, bool) {
	return q.pop() // ignores score
}

type leastserved[T any] struct{}

func (leastserved[T]) Next(q *ringbuffer[T], score func(T) float64) (T, bool) {
	best, bestScore := -1, math.MaxFloat64
	for i, t := range q.all() {
		if s := score(t); s < bestScore { // lower usage higher chance.
			best, bestScore = i, s
		}
	}
	if best < 0 {
		var zero T
		return zero, false
	}
	return q.removeAt(best)
}
