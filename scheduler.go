package turnstile

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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

	log *slog.Logger

	metrics *metrics
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
	enqueuedAt time.Time
	inflight   atomic.Bool // set just before the LLM call starts
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

// newscheduler builds a scheduler with a ring buffer of bufSize tenants per model.
func newscheduler(bufSize int, pol fairnesspolicy[*tenant], logger *slog.Logger, reg prometheus.Registerer) (*scheduler, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	mCfg := getWorkerConfig()
	s := &scheduler{
		buffer:      make(map[model]*ringbuffer[*tenant], len(mCfg)),
		usage:       make(map[model]*window, len(mCfg)),
		tenantUsage: make(map[model]map[string]*window, len(mCfg)),
		tenants:     make(map[model]map[string]*tenant, len(mCfg)),
		lock:        make(map[model]*sync.Mutex, len(mCfg)),
		wake:        make(map[model]chan struct{}, len(mCfg)),
		fairness:    pol,
		log:         logger,
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
	var err error
	if s.metrics, err = newMetrics(reg, pol.name()); err != nil {
		return nil, err
	}
	if reg != nil {
		if err := reg.Register(s); err != nil { // s itself is the state collector; see metrics.go
			return nil, err
		}
	}
	return s, nil
}

// clients are expected to call the schedule method for required tenants and llm requests.
func (s *scheduler) schedule(ctx context.Context, tenantName string, req request) (*response, error) {
	// calls enqueue
	// returns when response available.
	start := time.Now()
	j, err := s.enqueue(ctx, tenantName, req)
	if err != nil {
		return nil, err
	}

	record := func(outcome string) { // records outcome + duration once, right before each return
		s.metrics.recordOutcome(string(req.model), tenantName, outcome)
		s.metrics.observeDuration(string(req.model), outcome, time.Since(start))
	}

	select {
	case <-ctx.Done():
		err := ctx.Err()
		outcome := "canceled"
		if err == context.DeadlineExceeded {
			outcome = "timeout_queued"
			if j.inflight.Load() {
				outcome = "timeout_inflight"
			}
		}
		record(outcome)
		return nil, err
	case r := <-j.res:
		outcome := "served"
		if r.err != nil {
			outcome = "llm_error"
		}
		record(outcome)
		return r, nil
	}
}

func (s *scheduler) enqueue(ctx context.Context, tenantName string, req request) (*job, error) {

	model := req.model
	lock := s.lock[model]
	lock.Lock()
	defer lock.Unlock()

	j := &job{ctx: ctx, req: req, res: make(chan *response, 1), tenantName: tenantName, enqueuedAt: time.Now()}
	// check to see if ringbuffer already has tenant . if yes append new job
	tnt := s.tenants[model][tenantName]
	if tnt != nil {
		// yes our jobs to a tenant can keep growing, right now at phase 1 this is not capped.
		// you may want to cap it at production , could be a default or sensible numbers based off your tenants usage ratios.
		tnt.jobs = append(tnt.jobs, j)
		s.log.Debug("enqueued", "model", model, "tenant", tenantName, "queued_jobs", len(tnt.jobs))
		return j, nil
	}

	if s.buffer[model].isFull() {
		s.log.Warn("ring buffer full, rejected", "model", model, "tenant", tenantName)
		s.metrics.recordOutcome(string(model), tenantName, "rejected_queue_full")
		return nil, fmt.Errorf("ring buffer full capacity")
	}

	tnt = &tenant{name: tenantName, jobs: []*job{j}}
	s.tenants[model][tenantName] = tnt
	s.buffer[model].push(tnt)
	s.log.Debug("enqueued", "model", model, "tenant", tenantName, "queued_jobs", 1, "new_in_rotation", true)

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
			j.inflight.Store(true)
			s.metrics.inflightInc(string(m))
			defer s.metrics.inflightDec(string(m))

			// call llm
			res, err := s.llm.doCall(j.ctx, j.req)
			if err != nil {
				s.log.Debug("llm call failed", "model", m, "tenant", j.tenantName, "err", err)
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
		s.log.Debug("model TPM gate closed", "model", m, "used_last_min", int(s.usage[m].usageOverLastMinute()))
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
		t, _ := s.fairness.next(q, score)

		// if any of the job scheduled for the model+tenant combination has timed out, we take it out and consider only next one.
		dropped := 0
		for len(t.jobs) > 0 && t.jobs[0].ctx.Err() != nil {
			s.metrics.observeQueueWait(string(m), "false", time.Since(t.jobs[0].enqueuedAt))
			t.jobs[0] = nil
			t.jobs = t.jobs[1:]
			dropped++
		}
		if dropped > 0 {
			s.metrics.jobsAbandoned(string(m), dropped)
			s.log.Debug("dropped abandoned jobs", "model", m, "tenant", t.name, "count", dropped, "queued_jobs", len(t.jobs))
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
		s.metrics.observeQueueWait(string(m), "true", time.Since(j.enqueuedAt))
		s.log.Debug("dispatched", "model", m, "tenant", t.name, "queued_jobs", len(t.jobs))
		return j, true

	}
	return nil, false
}

// record the tokens consumed for tenant and model.
func (s *scheduler) recordUsage(m model, tenant string, tokens float64) {

	s.lock[m].Lock()
	defer s.lock[m].Unlock()

	before := s.usage[m].usageOverLastMinute()
	s.usage[m].add(tokens)
	limit := modelLimits[m].TPM * tpmGate
	s.log.Debug("completed", "model", m, "tenant", tenant, "tokens", tokens, "model_used_last_min", int(before+tokens))
	if before < limit && before+tokens >= limit { // log once, at the moment the gate closes
		s.log.Warn("rate limit crossed: model TPM gate closed", "model", m, "used_last_min", int(before+tokens), "limit", int(limit))
		s.metrics.gateClosed(string(m))
	}

	w, ok := s.tenantUsage[m][tenant]
	if !ok {
		w = &window{}
		s.tenantUsage[m][tenant] = w
	}
	w.add(tokens)
	s.metrics.tokensAdd(string(m), tenant, tokens)
}

// returns true if tokens per minute is left over for the model in question.
func (s *scheduler) isWithinUsageLimit(m model) bool {
	return s.usage[m].usageOverLastMinute() < modelLimits[m].TPM*tpmGate
}
