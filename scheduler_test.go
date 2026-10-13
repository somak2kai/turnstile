package turnstile

import (
	"bufio"
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof/* on http.DefaultServeMux
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
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
//   - tenant shares (tokens per minute, from testdata/shares.env) are multiplied by speedup, so tenants
//     cross their share at the same point relative to the work done;
//   - the per-request deadline is 30s / speedup = 60ms.
//
// The 60s usage window is not scaled, so share accounting behaves as in
// production within a run.
const (
	fakeMinLatency = 1 * time.Millisecond
	fakeMaxLatency = 10 * time.Millisecond
	speedup        = 500.0
	reqTimeout     = time.Duration(float64(30*time.Second) / speedup)
	noisyTenant    = "pebbleapp"
	sharesFile     = "testdata/shares.env" // copy of pizzashop/.env
)

// TestMain starts a pprof endpoint for Parca to scrape when PPROF_ADDR is set:
//
//	PPROF_ADDR=:1122 go test -run xxx -bench Scheduler -benchtime 4m
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

	sent, served, timedOut, rejected atomic.Int64
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

// loadShares reads <model>__<tenant>=<tokens per minute> lines, the same
// format and parsing as pizzashop's loadShares. A plain line parser instead of
// godotenv keeps the package free of a dependency that only the test needs.
func loadShares(path string) (map[model]map[string]float64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	shares := map[model]map[string]float64{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("bad line %q: want <model>__<tenant>=<tpm>", line)
		}
		m, tenant, ok := strings.Cut(key, "__") // "c_haiku4_5", "northwind"
		if !ok {
			return nil, fmt.Errorf("bad key %q: want <model>__<tenant>", key)
		}
		n, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", key, err)
		}
		if shares[model(m)] == nil {
			shares[model(m)] = map[string]float64{}
		}
		shares[model(m)][tenant] = n
	}
	return shares, sc.Err()
}

// setShares loads the .env shares into the package-level config that shareOf
// reads, scaled by speedup (see top of file), and restores it afterwards.
func setShares(b *testing.B) {
	shares, err := loadShares(sharesFile)
	if err != nil {
		b.Fatalf("loading %s: %v", sharesFile, err)
	}
	for _, tenants := range shares {
		for t := range tenants {
			tenants[t] *= speedup
		}
	}
	prev := modelTenantShare
	modelTenantShare = shares
	b.Cleanup(func() { modelTenantShare = prev })
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
	setShares(b)

	s := newscheduler(64, pol) // 64 > 35 customers: one ring slot per customer per model
	s.llm = fakeLLM{}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { s.run(ctx); close(runDone) }()
	defer func() { cancel(); <-runDone }()

	senders := newSenders(noisy)
	p := newPicker(senders)

	var latMu sync.Mutex
	var latencies []time.Duration

	b.SetParallelism(64) // 64 × GOMAXPROCS concurrent clients
	b.ResetTimer()
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
			switch {
			case err == nil:
				sd.served.Add(1)
				local = append(local, time.Since(start))
			case rctx.Err() != nil:
				sd.timedOut.Add(1)
			default:
				sd.rejected.Add(1) // ring buffer full
			}
			rcancel()
		}
		latMu.Lock()
		latencies = append(latencies, local...)
		latMu.Unlock()
	})
	b.StopTimer()

	report(b, senders, latencies)
}

// report adds per-tier served % and latency percentiles to the benchmark output.
func report(b *testing.B, senders []*sender, latencies []time.Duration) {
	type agg struct{ sent, served, timedOut, rejected int64 }
	byTier := map[string]*agg{}
	var all agg
	for _, sd := range senders {
		a := byTier[sd.tier]
		if a == nil {
			a = &agg{}
			byTier[sd.tier] = a
		}
		for _, x := range []*agg{a, &all} {
			x.sent += sd.sent.Load()
			x.served += sd.served.Load()
			x.timedOut += sd.timedOut.Load()
			x.rejected += sd.rejected.Load()
		}
	}
	pct := func(n, d int64) float64 { return 100 * float64(n) / float64(max(d, 1)) }

	b.ReportMetric(pct(all.served, all.sent), "served%")
	b.ReportMetric(pct(all.timedOut, all.sent), "timeout%")
	b.ReportMetric(pct(all.rejected, all.sent), "rejected%")
	for _, t := range []string{"enterprise", "business", "growth", "starter", "noisy"} {
		if a := byTier[t]; a != nil {
			b.ReportMetric(pct(a.served, a.sent), t+"-served%")
		}
	}
	if len(latencies) > 0 {
		slices.Sort(latencies)
		at := func(q float64) float64 {
			return float64(latencies[int(q*float64(len(latencies)-1))]) / float64(time.Millisecond)
		}
		b.ReportMetric(at(0.50), "p50-ms")
		b.ReportMetric(at(0.99), "p99-ms")
	}
}
