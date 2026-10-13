package turnstile

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// scheduler responsible for scheduling jobs and ensuring fairness policies are applied for execution sequence.
type scheduler struct {
	// represents requests/jobs in queue per model per tenant.
	buffer map[model]*ringbuffer[*tenant]

	// represents current tenants whose jobs are enqueued.
	tenants map[model]map[string]*tenant

	// represents tokens used in the last minute per model, across all tenants.
	// this is what gets checked against the model's TPM before dispatching.
	usage map[model]*window

	// represents tokens used in the last minute per model per tenant name.
	// only used as the fairness score (eg leastserved), never as a limit.
	tenantUsage map[model]map[string]*window

	// per model lock.
	lock map[model]*sync.Mutex

	// wake signal per model when element has landed in buffer.
	wake map[model]chan struct{}

	llm llmprovider

	fairness fairnesspolicy[*tenant]
}

type llmprovider interface {
	doCall(ctx context.Context, req request) (*response, error)
}

type tenant struct {
	name string
	jobs []*job
}
type job struct {
	ctx        context.Context
	req        request
	res        chan *response
	tenantName string
}

// minimal type for the purpose of the exercise.
type response struct {
	// using tokens per limit as an usage check and rate limit factor.
	tokens float64
	err    error
}

// minimal type for the purpose of the exercise.
type request struct {
	model model
}

func newscheduler(bufSize int, pol fairnesspolicy[*tenant]) *scheduler {
	mCfg := getWorkerConfig()
	s := &scheduler{
		buffer:      make(map[model]*ringbuffer[*tenant], len(mCfg)),
		usage:       make(map[model]*window, len(mCfg)),
		tenantUsage: make(map[model]map[string]*window, len(mCfg)),
		tenants:     make(map[model]map[string]*tenant, len(mCfg)),
		lock:        make(map[model]*sync.Mutex, len(mCfg)),
		wake:        make(map[model]chan struct{}, len(mCfg)),
		fairness:    pol,
	}
	for m := range mCfg {
		// counts should be as per model
		s.buffer[m] = newringbuffer[*tenant](bufSize)
		s.usage[m] = &window{}
		s.tenantUsage[m] = make(map[string]*window)
		s.lock[m] = &sync.Mutex{}
		s.wake[m] = make(chan struct{}, 1)
		s.tenants[m] = make(map[string]*tenant)
	}
	return s
}

// clients are expected to call the schedule method for required tenants and llm requests.
func (s *scheduler) schedule(ctx context.Context, tenantName string, req request) (*response, error) {
	// calls enqueue
	// returns when response available.
	j, err := s.enqueue(ctx, tenantName, req)
	if err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-j.res:
		return r, nil

	}

}

func (s *scheduler) enqueue(ctx context.Context, tenantName string, req request) (*job, error) {

	model := req.model
	lock := s.lock[model]
	lock.Lock()
	defer lock.Unlock()

	j := &job{ctx: ctx, req: req, res: make(chan *response, 1), tenantName: tenantName}
	// check to see if ringbuffer already has tenant . if yes append new job
	tnt := s.tenants[model][tenantName]
	if tnt != nil {
		tnt.jobs = append(tnt.jobs, j)
		return j, nil
	}
	tnt = &tenant{name: tenantName, jobs: []*job{j}}
	s.tenants[model][tenantName] = tnt
	ok := s.buffer[model].push(tnt)
	if !ok {
		// buffer is full, return error and let clients decide on retry.
		return nil, fmt.Errorf("ring buffer full capacity")
	}

	select {
	case s.wake[model] <- struct{}{}: //non-blocking: safe to send while holding the lock
	default:
	}

	return j, nil
}

func (s *scheduler) run(ctx context.Context) {
	var wg sync.WaitGroup
	for model, limit := range getWorkerConfig() {
		wg.Go(func() {
			s.dispatch(ctx, model, limit)
		})
	}
	wg.Wait()
}

func (s *scheduler) dispatch(ctx context.Context, m model, limit int) {

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(limit)
	defer g.Wait() //nolint:errcheck
	for {

		j, ok := s.nextOrWait(m)
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-s.wake[m]:
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		g.Go(func() error {

			// call llm
			res, err := s.llm.doCall(j.ctx, j.req)
			if err != nil {
				j.res <- &response{err: err}
				return nil
			}
			j.res <- res
			s.recordUsage(m, j.tenantName, res.tokens)
			return nil
		})
	}

}

// apply fairness policy to take element off the ring buffer.
func (s *scheduler) nextOrWait(m model) (*job, bool) {

	s.lock[m].Lock()
	defer s.lock[m].Unlock()

	// the model's TPM is used up for the last minute: nobody can go, whoever's turn it is.
	// we also keep a soft limit target of 95% so we escape earlier than overshooting consumption.
	if !s.isWithinUsageLimit(m) {
		return nil, false
	}

	q := s.buffer[m]
	size := q.size()

	for i := 0; i < size; i++ {

		score := func(t *tenant) float64 {
			w := s.tenantUsage[m][t.name]
			if w == nil {
				return 0 // no usage yet: lightest possible
			}
			return w.usageOverLastMinute()
		}
		t, _ := s.fairness.Next(q, score)

		// if any of the job scheduled for the model+tenant combination has timed out, we take it out and consider only next one.
		for len(t.jobs) > 0 && t.jobs[0].ctx.Err() != nil {
			t.jobs[0] = nil
			t.jobs = t.jobs[1:]
		}

		if len(t.jobs) == 0 { // nothing live left: leave the rotation
			delete(s.tenants[m], t.name)
			continue // we take one lap to see if there are any other tenants job we could probably pick up.
		}

		j := t.jobs[0]
		t.jobs[0] = nil
		t.jobs = t.jobs[1:]
		if len(t.jobs) > 0 {
			q.push(t) // once we have scheduled a tenants calls to be picked up, we remove from front of queue and push it back.
		} else {
			// t.queued = false
			delete(s.tenants[m], t.name)
		}
		return j, true

	}
	return nil, false
}

// record the tokens consumed for tenant and model.
func (s *scheduler) recordUsage(m model, tenant string, tokens float64) {

	s.lock[m].Lock()
	defer s.lock[m].Unlock()

	s.usage[m].add(tokens)

	w, ok := s.tenantUsage[m][tenant]
	if !ok {
		w = &window{}
		s.tenantUsage[m][tenant] = w
	}
	w.add(tokens)

}

// returns true if tokens per minute is left over for the model in question.
func (s *scheduler) isWithinUsageLimit(m model) bool {
	return s.usage[m].usageOverLastMinute() < modelLimits[m].TPM*tpmGate
}
