package streamaggr

import (
	"sync"
)

// rateAggrState calculates output=rate, e.g. the counter per-second change.
type rateAggrState struct {
	m      sync.Map
	suffix string
}

type rateStateValue struct {
	mu sync.Mutex

	// prevTimestamp stores timestamp of the last registered value
	// in the previous aggregation interval
	prevTimestamp  map[string]int64
	state          [aggrStateSize]rateState
	deleted        bool
	deleteDeadline int64
}

type rateState struct {
	lastValues map[string]rateLastValueState
}

type rateLastValueState struct {
	value          float64
	timestamp      int64
	deleteDeadline int64

	// total stores cumulative difference between registered values
	// in the aggregation interval
	total float64
}

func newRateAggrState(suffix string) *rateAggrState {
	return &rateAggrState{
		suffix: suffix,
	}
}

func (as *rateAggrState) pushSamples(samples []pushSample, deleteDeadline int64, idx int) {
	for i := range samples {
		s := &samples[i]
		inputKey, outputKey := getInputOutputKey(s.key)

	again:
		v, ok := as.m.Load(outputKey)
		if !ok {
			// The entry is missing in the map. Try creating it.
			rsv := &rateStateValue{
				prevTimestamp: make(map[string]int64),
			}
			for r := range rsv.state {
				rsv.state[r] = rateState{
					lastValues: make(map[string]rateLastValueState),
				}
			}
			v = rsv
			vNew, loaded := as.m.LoadOrStore(outputKey, v)
			if loaded {
				// Use the entry created by a concurrent goroutine.
				v = vNew
			}
		}
		sv := v.(*rateStateValue)
		sv.mu.Lock()
		deleted := sv.deleted
		if !deleted {
			lv, ok := sv.state[idx].lastValues[inputKey]
			if ok {
				if s.timestamp < lv.timestamp {
					// Skip out of order sample
					sv.mu.Unlock()
					continue
				}
				if _, ok = sv.prevTimestamp[inputKey]; !ok {
					sv.prevTimestamp[inputKey] = lv.timestamp
				}
				if s.value >= lv.value {
					lv.total += s.value - lv.value
				} else {
					// counter reset
					lv.total += s.value
				}
			}
			lv.value = s.value
			lv.timestamp = s.timestamp
			lv.deleteDeadline = deleteDeadline
			sv.state[idx].lastValues[inputKey] = lv
			sv.deleteDeadline = deleteDeadline
		}
		sv.mu.Unlock()
		if deleted {
			// The entry has been deleted by the concurrent call to flushState
			// Try obtaining and updating the entry again.
			goto again
		}
	}
}

func (as *rateAggrState) flushState(ctx *flushCtx, flushTimestamp int64, idx int) {
	m := &as.m
	m.Range(func(k, v interface{}) bool {
		sv := v.(*rateStateValue)
		sv.mu.Lock()

		// check for stale entries
		deleted := flushTimestamp > sv.deleteDeadline
		if deleted {
			// Mark the current entry as deleted
			sv.deleted = deleted
			sv.mu.Unlock()
			m.Delete(k)
			return true
		}

		// Delete outdated entries in state
		var rate float64
		state := sv.state[idx]
		for k1, v1 := range state.lastValues {
			if flushTimestamp > v1.deleteDeadline {
				delete(state.lastValues, k1)
				continue
			} else if prevTimestamp, ok := sv.prevTimestamp[k1]; ok && prevTimestamp > 0 {
				rate += v1.total * 1000 / float64(v1.timestamp-prevTimestamp)
			}
			sv.prevTimestamp[k1] = v1.timestamp
			v1.total = 0
		}
		if as.suffix == "rate_avg" && len(state.lastValues) > 0 {
			// note: capture m length after deleted items were removed
			rate /= float64(len(state.lastValues))
		}
		sv.mu.Unlock()
		if rate > 0 {
			key := k.(string)
			ctx.appendSeries(key, as.suffix, flushTimestamp, rate)
		}
		return true
	})
}
