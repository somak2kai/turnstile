package turnstile

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof/* on http.DefaultServeMux
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The benchmark replays the load from pizzashop's main(): 35 customers in four
// tiers plus one noisy neighbour hammering c_haiku4_5, against a fake LLM.
//
// Time is compressed so a run takes seconds, not minutes. The real setup has
// LLM calls of 0.5–5s; the fake takes 1–10ms, about 500× faster. Everything
// that depends on call speed is scaled by the same factor:
//   - each model's TPM is multiplied by speedup, so the model gate closes at
//     the same point relative to the work done;
//   - the per-request deadline is 30s / speedup = 60ms.
//
// The 60s usage window is not scaled, so usage accounting behaves as in
// production within a run.
const (
	fakeMinLatency = 1 * time.Millisecond
	fakeMaxLatency = 10 * time.Millisecond
	speedup        = 500.0
	reqTimeout     = time.Duration(float64(30*time.Second) / speedup)
	noisyTenant    = "pebbleapp"
)

// TestMain starts a pprof endpoint for Parca to scrape when PPROF_ADDR is set:
//
//	PPROF_ADDR=:1122 go test -run xxx -bench Scheduler -benchtime 4m
//
// PPROF_ADDR=:1122 go test -run xxx -bench 'Scheduler/leastserved/noisy=true' -benchtime 4m -timeout 30m to run specific cases.
//
// Without PPROF_ADDR the tests and benchmarks run as normal.
func TestMain(m *testing.M) {
	if addr := os.Getenv("PPROF_ADDR"); addr != "" {
		runtime.SetBlockProfileRate(10_000)  // record blocking on channels, select, wg (sampled below 10µs)
		runtime.SetMutexProfileFraction(100) // record 1 in 100 contended lock events
		go func() {
			if err := http.ListenAndServe(addr, nil); err != nil {
				fmt.Fprintln(os.Stderr, "pprof server:", err)
			}
		}()
		fmt.Fprintln(os.Stderr, "pprof on", addr+"/debug/pprof/")
	}
	os.Exit(m.Run())
}

// ---------------------------------------------------------------------------
// fake LLM
// ---------------------------------------------------------------------------

type fakeLLM struct{}

func (fakeLLM) doCall(ctx context.Context, _ request) (*response, error) {
	d := fakeMinLatency + time.Duration(rand.Int64N(int64(fakeMaxLatency-fakeMinLatency)))
	select {
	case <-time.After(d):
		in, out := 200+rand.IntN(2000), 50+rand.IntN(1500) // same token spread as pizzashop's callLLM
		return &response{tokens: float64(in + out)}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ---------------------------------------------------------------------------
// customers, same tiers as pizzashop
// ---------------------------------------------------------------------------

var (
	cheapModels = []model{g_gemini3_flash, g_gemini3_1_flashlit, g_gemini3_5_flash, g_gemini3_8_flash,
		c_haiku4_5, o_gpt4_1_mini, o_gpt5_4_mini}
	midModels = append(slices.Clone(cheapModels), c_sonnet4_6, c_sonnet5, o_gpt4_1, o_gpt5_6_sol)
	allModels = func() []model {
		out := make([]model, 0, len(modelLimits))
		for m := range modelLimits {
			out = append(out, m)
		}
		return out
	}()
)

type tier struct {
	name    string
	rate    float64 // requests per second per customer in pizzashop (1/interval)
	models  []model
	members []string
}

var tiers = []tier{
	{"enterprise", 100, allModels, []string{"northwind", "stellarforge", "bluepeak", "quantaro", "ironleaf"}},
	{"business", 40, allModels, []string{"copperline", "vantagehq", "lumenworks", "driftwood", "kestrel",
		"orbitalis", "pinegrove", "halcyon", "meridianlabs", "tidewater"}},
	{"growth", 20, midModels, []string{"sparkfern", "nimbusly", "brightcart", "tallowtech", "fernhollow",
		"zephyrly", "mosaicai", "quillbox", "roamly", "cobaltbay"}},
	{"starter", 5, cheapModels, []string{"pebbleapp", "tinyloop", "acornio", "wispwave", "dotmatter",
		"plumcraft", "jotpad", "minnowly", "sprigly", "nookbase"}},
}

// sender is one source of requests. The noisy neighbour is one more sender
// under an existing customer's name, on one model only.
type sender struct {
	name   string
	tier   string
	rate   float64
	models []model

	sent, served atomic.Int64
}

func newSenders(noisy bool) []*sender {
	var out []*sender
	for _, tr := range tiers {
		for _, n := range tr.members {
			out = append(out, &sender{name: n, tier: tr.name, rate: tr.rate, models: tr.models})
		}
	}
	if noisy {
		out = append(out, &sender{name: noisyTenant, tier: "noisy", rate: 1000, models: []model{c_haiku4_5}})
	}
	return out
}

// picker chooses a sender in proportion to its request rate, so the mix of
// requests matches pizzashop's tickers (enterprise every 10ms, noisy every 1ms, ...).
type picker struct {
	senders []*sender
	cum     []float64
}

func newPicker(ss []*sender) *picker {
	p := &picker{senders: ss}
	total := 0.0
	for _, s := range ss {
		total += s.rate
		p.cum = append(p.cum, total)
	}
	return p
}

func (p *picker) pick() *sender {
	x := rand.Float64() * p.cum[len(p.cum)-1]
	i, _ := slices.BinarySearch(p.cum, x)
	return p.senders[min(i, len(p.senders)-1)]
}

// scaleLimits multiplies every model's TPM by speedup (see top of file) and
// restores the real limits afterwards.
func scaleLimits(b *testing.B) {
	prev := modelLimits
	scaled := make(map[model]modelLimit, len(prev))
	for m, l := range prev {
		scaled[m] = modelLimit{RPM: l.RPM, TPM: l.TPM * speedup}
	}
	modelLimits = scaled
	b.Cleanup(func() { modelLimits = prev })
}

// ---------------------------------------------------------------------------
// benchmark
// ---------------------------------------------------------------------------

// BenchmarkScheduler: each op is one schedule() call from a randomly chosen
// sender, issued by many concurrent clients (closed loop: a client sends its
// next request once the previous one is answered, times out or is rejected).
//
//	go test -bench Scheduler -benchtime 5s
func BenchmarkScheduler(b *testing.B) {
	policies := []struct {
		name string
		pol  fairnesspolicy[*tenant]
	}{
		{"roundrobin", roundrobin[*tenant]{}},
		{"leastserved", leastserved[*tenant]{}},
	}
	for _, p := range policies {
		for _, noisy := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/noisy=%v", p.name, noisy), func(b *testing.B) {
				runLoad(b, p.pol, noisy)
			})
		}
	}
}

func runLoad(b *testing.B, pol fairnesspolicy[*tenant], noisy bool) {
	scaleLimits(b)

	s := newscheduler(64, pol) // 64 > 35 customers: one ring slot per customer per model
	s.llm = fakeLLM{}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { s.run(ctx); close(runDone) }()
	defer func() { cancel(); <-runDone }()

	senders := newSenders(noisy)
	p := newPicker(senders)

	var latMu sync.Mutex
	var latencies []time.Duration // served requests only

	b.SetParallelism(64) // 64 × GOMAXPROCS concurrent clients
	b.ResetTimer()
	began := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		var local []time.Duration
		for pb.Next() {
			sd := p.pick()
			m := sd.models[rand.IntN(len(sd.models))]
			sd.sent.Add(1)

			rctx, rcancel := context.WithTimeout(ctx, reqTimeout)
			start := time.Now()
			res, err := s.schedule(rctx, sd.name, request{model: m})
			if err == nil && res.err != nil {
				err = res.err
			}
			if err == nil {
				sd.served.Add(1)
				local = append(local, time.Since(start))
			}
			rcancel()
		}
		latMu.Lock()
		latencies = append(latencies, local...)
		latMu.Unlock()
	})
	b.StopTimer()

	report(b, senders, latencies, time.Since(began))
}

// ---------------------------------------------------------------------------
// reporting
// ---------------------------------------------------------------------------

// perMinReal converts a count over a compressed run into requests per minute
// of real time: the fake LLM is speedup× faster, so divide by speedup.
func perMinReal(n int64, elapsed time.Duration) float64 {
	return float64(n) / elapsed.Seconds() * 60 / speedup
}

// report adds three numbers to the benchmark line:
//   - served%:    share of requests answered before their deadline
//   - p99-ms:     99th percentile latency of served requests (timed-out ones have no latency)
//   - served/min: requests served per minute, converted to real time
func report(b *testing.B, senders []*sender, latencies []time.Duration, elapsed time.Duration) {
	var sent, served int64
	for _, sd := range senders {
		sent += sd.sent.Load()
		served += sd.served.Load()
	}
	b.ReportMetric(100*float64(served)/float64(max(sent, 1)), "served%")
	if len(latencies) > 0 {
		slices.Sort(latencies)
		p99 := latencies[int(0.99*float64(len(latencies)-1))]
		b.ReportMetric(float64(p99)/float64(time.Millisecond), "p99-ms")
	}
	b.ReportMetric(perMinReal(served, elapsed), "served/min")
}
