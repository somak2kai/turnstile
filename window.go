package turnstile

import "time"

const (
	// each window holds data for that interval of 10secs
	_windowWidth = 10 * time.Second
	// 60s timeframe, we in total calculate and/or age out for 1min worth of data. so 6*10 slots = 60s.
	_windowSize = 6
)

type bucket struct {
	slotNumber int64
	value      float64 // can be ints, since we are basing this on tokens count but for now taking it as float.
}

// window captures values against timestamp by placing in _windowSize number of buckets.
type window [_windowSize]bucket

// divide our clock into 6 slots of 10 seconds each.
// w[0] covers 0-10s and val represents total cumulative value over that duration.
func (w *window) add(val float64) {
	slot := time.Now().UnixNano() / int64(_windowWidth)
	idx := slot % _windowSize

	// eg 10:00:01 and 10:01:01 are not same slot numbers.
	// when we receive 10:01:01, we wipe off earlier data.
	if w[idx].slotNumber != slot {
		w[idx].slotNumber, w[idx].value = slot, 0
	}

	w[idx].value += val
}

func (w *window) usageOverLastMinute() float64 {
	slot := time.Now().UnixNano() / int64(_windowWidth)
	sum := 0.0
	for _, i := range w {

		// over last minute , so last 6 slots.
		if slot-i.slotNumber < _windowSize {
			sum += i.value
		}
	}

	return sum
}
