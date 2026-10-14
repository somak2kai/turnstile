package turnstile

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var latencyBuckets = []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30, 60}

// metrics holds every turnstile_* series for one scheduler. tenant never labels a histogram, to keep cardinality bounded.
type metrics struct {
	requests        *prometheus.CounterVec
	tokens          *prometheus.CounterVec
	abandoned       *prometheus.CounterVec
	gateClosures    *prometheus.CounterVec
	queueWait       *prometheus.HistogramVec
	requestDuration *prometheus.HistogramVec
	inflight        *prometheus.GaugeVec

	queueTenants, queueCapacity, queueJobs, oldestJobAge,
	tpmUsed, tpmLimit, rpmLimit, gateOpen, workersLimit,
	tenantsTracked, fairnessIndex *prometheus.Desc
}

func newMetrics(reg prometheus.Registerer, policy string) (*metrics, error) {
	cl := prometheus.Labels{"policy": policy}
	m := &metrics{
		requests:        prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "turnstile", Name: "requests_total", Help: "LLM requests by terminal outcome.", ConstLabels: cl}, []string{"model", "tenant", "outcome"}),
		tokens:          prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "turnstile", Name: "tokens_total", Help: "Tokens consumed, per tenant.", ConstLabels: cl}, []string{"model", "tenant"}),
		abandoned:       prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "turnstile", Name: "jobs_abandoned_total", Help: "Jobs dropped because their context expired before dispatch.", ConstLabels: cl}, []string{"model"}),
		gateClosures:    prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "turnstile", Name: "gate_closures_total", Help: "Times a model's soft TPM gate closed.", ConstLabels: cl}, []string{"model"}),
		queueWait:       prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "turnstile", Name: "queue_wait_seconds", Help: "Time a job spent in the ring buffer.", ConstLabels: cl, Buckets: latencyBuckets}, []string{"model", "dispatched"}),
		requestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "turnstile", Name: "request_duration_seconds", Help: "End-to-end schedule() latency.", ConstLabels: cl, Buckets: latencyBuckets}, []string{"model", "outcome"}),
		inflight:        prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "turnstile", Name: "inflight_requests", Help: "LLM calls currently at the provider.", ConstLabels: cl}, []string{"model"}),

		queueTenants:   prometheus.NewDesc("turnstile_queue_tenants", "Tenants currently in a model's rotation.", []string{"model"}, cl),
		queueCapacity:  prometheus.NewDesc("turnstile_queue_capacity", "Ring buffer capacity for a model.", []string{"model"}, cl),
		queueJobs:      prometheus.NewDesc("turnstile_queue_jobs", "Jobs queued across all tenants for a model.", []string{"model"}, cl),
		oldestJobAge:   prometheus.NewDesc("turnstile_queue_oldest_job_age_seconds", "Age of the oldest job still queued.", []string{"model"}, cl),
		tpmUsed:        prometheus.NewDesc("turnstile_model_tpm_used_tokens", "Tokens used by a model in the last 60s.", []string{"model"}, cl),
		tpmLimit:       prometheus.NewDesc("turnstile_model_tpm_limit_tokens", "Provider TPM limit.", []string{"model"}, cl),
		rpmLimit:       prometheus.NewDesc("turnstile_model_rpm_limit", "Provider RPM limit.", []string{"model"}, cl),
		gateOpen:       prometheus.NewDesc("turnstile_model_gate_open", "1 if the model's TPM gate is open.", []string{"model"}, cl),
		workersLimit:   prometheus.NewDesc("turnstile_workers_limit", "Concurrent worker slots for a model.", []string{"model"}, cl),
		tenantsTracked: prometheus.NewDesc("turnstile_tenants_tracked", "Distinct tenants with a usage window, ever seen.", []string{"model"}, cl),
		fairnessIndex:  prometheus.NewDesc("turnstile_fairness_index", "Jain's fairness index over tenants' tokens in the last 60s.", []string{"model"}, cl),
	}
	if reg == nil {
		return m, nil
	}
	for _, c := range []prometheus.Collector{m.requests, m.tokens, m.abandoned, m.gateClosures, m.queueWait, m.requestDuration, m.inflight} {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (m *metrics) recordOutcome(model, tenant, outcome string) {
	m.requests.WithLabelValues(model, tenant, outcome).Inc()
}
func (m *metrics) tokensAdd(model, tenant string, n float64) {
	m.tokens.WithLabelValues(model, tenant).Add(n)
}
func (m *metrics) jobsAbandoned(model string, n int) {
	m.abandoned.WithLabelValues(model).Add(float64(n))
}
func (m *metrics) gateClosed(model string) { m.gateClosures.WithLabelValues(model).Inc() }
func (m *metrics) observeQueueWait(model, dispatched string, d time.Duration) {
	m.queueWait.WithLabelValues(model, dispatched).Observe(d.Seconds())
}
func (m *metrics) observeDuration(model, outcome string, d time.Duration) {
	m.requestDuration.WithLabelValues(model, outcome).Observe(d.Seconds())
}
func (m *metrics) inflightInc(model string) { m.inflight.WithLabelValues(model).Inc() }
func (m *metrics) inflightDec(model string) { m.inflight.WithLabelValues(model).Dec() }

func (s *scheduler) Describe(ch chan<- *prometheus.Desc) {
	m := s.metrics
	ch <- m.queueTenants
	ch <- m.queueCapacity
	ch <- m.queueJobs
	ch <- m.oldestJobAge
	ch <- m.tpmUsed
	ch <- m.tpmLimit
	ch <- m.rpmLimit
	ch <- m.gateOpen
	ch <- m.workersLimit
	ch <- m.tenantsTracked
	ch <- m.fairnessIndex
}

// Collect snapshots each model's state under its own lock, then sends it; it never holds a lock while sending.
func (s *scheduler) Collect(ch chan<- prometheus.Metric) {
	m := s.metrics
	limits := getWorkerConfig()
	for mo, lock := range s.lock {
		lock.Lock()
		tenants := s.buffer[mo].size()
		capacity := len(s.buffer[mo].queue)
		jobs, oldest := 0, time.Time{}
		for _, t := range s.tenants[mo] {
			jobs += len(t.jobs)
			if len(t.jobs) > 0 && (oldest.IsZero() || t.jobs[0].enqueuedAt.Before(oldest)) {
				oldest = t.jobs[0].enqueuedAt
			}
		}
		used := s.usage[mo].usageOverLastMinute()
		limit := modelLimits[mo]
		fairness := jainFairness(s.tenantUsage[mo])
		tracked := len(s.tenantUsage[mo])
		lock.Unlock()

		name := string(mo)
		gateOpen := 0.0
		if used < limit.TPM*tpmGate {
			gateOpen = 1
		}
		ch <- prometheus.MustNewConstMetric(m.queueTenants, prometheus.GaugeValue, float64(tenants), name)
		ch <- prometheus.MustNewConstMetric(m.queueCapacity, prometheus.GaugeValue, float64(capacity), name)
		ch <- prometheus.MustNewConstMetric(m.queueJobs, prometheus.GaugeValue, float64(jobs), name)
		if !oldest.IsZero() {
			ch <- prometheus.MustNewConstMetric(m.oldestJobAge, prometheus.GaugeValue, time.Since(oldest).Seconds(), name)
		}
		ch <- prometheus.MustNewConstMetric(m.tpmUsed, prometheus.GaugeValue, used, name)
		ch <- prometheus.MustNewConstMetric(m.tpmLimit, prometheus.GaugeValue, limit.TPM, name)
		ch <- prometheus.MustNewConstMetric(m.rpmLimit, prometheus.GaugeValue, limit.RPM, name)
		ch <- prometheus.MustNewConstMetric(m.gateOpen, prometheus.GaugeValue, gateOpen, name)
		ch <- prometheus.MustNewConstMetric(m.workersLimit, prometheus.GaugeValue, float64(limits[mo]), name)
		ch <- prometheus.MustNewConstMetric(m.tenantsTracked, prometheus.GaugeValue, float64(tracked), name)
		ch <- prometheus.MustNewConstMetric(m.fairnessIndex, prometheus.GaugeValue, fairness, name)
	}
}

// jainFairness is (Σx)²/(n·Σx²) over each tenant's tokens in the last 60s: 1.0 is perfectly fair, →1/n means one tenant takes it all.
func jainFairness(byTenant map[string]*window) float64 {
	if len(byTenant) == 0 {
		return 1
	}
	var sum, sumSq float64
	for _, w := range byTenant {
		x := w.usageOverLastMinute()
		sum += x
		sumSq += x * x
	}
	if sumSq == 0 {
		return 1
	}
	return (sum * sum) / (float64(len(byTenant)) * sumSq)
}
