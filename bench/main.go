// Command bench measures the footprint of N whatsmeow clients, laid out either
// as N clients inside one process or as N processes holding one client each.
//
// Usage: go run . [-ladder 1,10,100,1000] [-modes idle,ws] [-topos inproc,proc]
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

func main() {
	var (
		isChild = flag.Bool("child", false, "run as a benchmarked child process")
		n       = flag.Int("n", 1, "child: number of clients to hold")
		mode    = flag.String("mode", "idle", "child: idle|ws")
		wsURL   = flag.String("ws", "", "child: fake server websocket URL")
		db      = flag.String("db", "", "child: sqlite path")

		ladder = flag.String("ladder", "1,10,100,1000", "client counts to test")
		modes  = flag.String("modes", "idle,ws", "idle,ws")
		topos  = flag.String("topos", "inproc,proc", "inproc,proc")
		budget = flag.Float64("rss-budget-gb", 10, "skip a case whose projected RSS exceeds this")
		out    = flag.String("out", "results.csv", "csv output path")
	)
	flag.Parse()

	if *isChild {
		c := &Child{N: *n, Mode: *mode, WSURL: *wsURL, DB: *db}
		if err := c.run(); err != nil {
			log.Fatalf("child: %v", err)
		}
		return
	}

	r := &Runner{
		Ladder:    parseInts(*ladder),
		Modes:     strings.Split(*modes, ","),
		Topos:     strings.Split(*topos, ","),
		BudgetGB:  *budget,
		OutPath:   *out,
		Settle:    1500 * time.Millisecond,
		Window:    3 * time.Second,
		ReadyWait: 180 * time.Second,
	}
	if err := r.run(); err != nil {
		log.Fatalf("runner: %v", err)
	}
}

// ---------------------------------------------------------------- the runner

type Runner struct {
	Ladder    []int
	Modes     []string
	Topos     []string
	BudgetGB  float64
	OutPath   string
	Settle    time.Duration
	Window    time.Duration
	ReadyWait time.Duration

	server  *FakeWA
	self    string
	tmp     string
	results []Runner_Result
}

// Result is one measured cell of the matrix.
type Runner_Result struct {
	Topology string
	Mode     string
	Clients  int
	Procs    int
	SpinupMS float64
	RSSTotMB float64
	RSSPerKB float64
	CPUPct   float64
	SysMB    float64
	Goros    int
	Threads  int
	HeapMB   float64
	Alive    int
	Skipped  string
}

func (r *Runner) run() error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	r.self = exe
	r.tmp, err = os.MkdirTemp("", "bench-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(r.tmp)

	r.server = &FakeWA{}
	if err := r.server.start(); err != nil {
		return err
	}
	defer r.server.stop()
	fmt.Printf("fake WhatsApp ws server on %s\n", r.server.url)

	for _, topo := range r.Topos {
		for _, mode := range r.Modes {
			for _, n := range r.Ladder {
				res := r.runCase(topo, mode, n)
				r.results = append(r.results, res)
				r.printRow(res)
			}
		}
	}
	if err := r.writeCSV(); err != nil {
		return err
	}
	r.printTable()
	return nil
}

func (r *Runner) runCase(topo, mode string, n int) Runner_Result {
	res := Runner_Result{Topology: topo, Mode: mode, Clients: n}
	procs, perProc := 1, n
	if topo == "proc" {
		procs, perProc = n, 1
	}
	res.Procs = procs

	if why := r.overBudget(topo, mode, procs); why != "" {
		res.Skipped = why
		return res
	}

	dir := filepath.Join(r.tmp, fmt.Sprintf("%s-%s-%d", topo, mode, n))
	os.MkdirAll(dir, 0o755)
	defer os.RemoveAll(dir)

	r.server.reset()
	start := time.Now()
	kids, err := r.spawn(procs, perProc, mode, dir)
	defer kids.kill()
	if err != nil {
		res.Skipped = "spawn: " + err.Error()
		return res
	}
	if err := kids.waitReady(r.ReadyWait); err != nil {
		res.Skipped = "ready: " + err.Error()
		return res
	}
	if mode == "ws" {
		if err := r.server.waitConns(n, r.ReadyWait); err != nil {
			res.Skipped = "conns: " + err.Error()
			return res
		}
	}
	res.SpinupMS = float64(time.Since(start).Microseconds()) / 1000

	time.Sleep(r.Settle)
	a := kids.snapshot()
	rssA := rssKB(kids.pids())
	time.Sleep(r.Window)
	b := kids.snapshot()
	rssB := rssKB(kids.pids())

	wall := float64(b.tsUS-a.tsUS) / 1e6
	if wall <= 0 {
		wall = r.Window.Seconds()
	}
	res.CPUPct = float64((b.cpuUS - a.cpuUS)) / 1e6 / wall * 100
	res.SysMB = float64(b.gosys) / 1024 / 1024
	res.RSSTotMB = float64(rssA+rssB) / 2 / 1024
	res.RSSPerKB = float64(rssA+rssB) / 2 / float64(n)
	res.Goros, res.Threads = b.goros, b.threads
	res.HeapMB = float64(b.heap) / 1024 / 1024
	res.Alive = int(r.server.conns.Load()) // live sockets seen by the server
	return res
}

// overBudget projects total RSS for a process-heavy case from the largest
// already-measured case with the same shape, so a 1000-process run cannot
// quietly push the machine into swap.
func (r *Runner) overBudget(topo, mode string, procs int) string {
	if procs < 50 {
		return ""
	}
	var perProcKB float64
	for _, p := range r.results {
		if p.Topology == topo && p.Mode == mode && p.Procs > 0 && p.Skipped == "" {
			mb := p.SysMB
			if mb <= 0 {
				mb = p.RSSTotMB
			}
			if v := mb * 1024 / float64(p.Procs); v > perProcKB {
				perProcKB = v
			}
		}
	}
	if perProcKB == 0 {
		return ""
	}
	projGB := perProcKB * float64(procs) / 1024 / 1024
	if projGB > r.BudgetGB {
		return fmt.Sprintf("projected %.1f GB > %.0f GB budget", projGB, r.BudgetGB)
	}
	return ""
}

func (r *Runner) spawn(procs, perProc int, mode, dir string) (*Kids, error) {
	kids := &Kids{}
	for i := 0; i < procs; i++ {
		args := []string{
			"-child",
			"-n", strconv.Itoa(perProc),
			"-mode", mode,
			"-ws", r.server.url,
			"-db", filepath.Join(dir, fmt.Sprintf("s%d.db", i)),
		}
		cmd := exec.Command(r.self, args...)
		if os.Getenv("BENCH_DEBUG") != "" {
			cmd.Stderr = os.Stderr
		}
		pipe, err := cmd.StdoutPipe()
		if err != nil {
			return kids, err
		}
		if err := cmd.Start(); err != nil {
			return kids, fmt.Errorf("proc %d: %w", i, err)
		}
		k := &Kid{cmd: cmd}
		kids.list = append(kids.list, k)
		go k.consume(pipe)
	}
	return kids, nil
}

func (r *Runner) printRow(res Runner_Result) {
	if res.Skipped != "" {
		fmt.Printf("  %-6s %-4s n=%-5d SKIPPED: %s\n", res.Topology, res.Mode, res.Clients, res.Skipped)
		return
	}
	fmt.Printf("  %-6s %-4s n=%-5d rss=%8.1f MB  sys=%8.1f MB  cpu=%5.1f%%  goro=%-6d spinup=%7.1f ms  alive=%d\n",
		res.Topology, res.Mode, res.Clients, res.RSSTotMB, res.SysMB, res.CPUPct, res.Goros, res.SpinupMS, res.Alive)
}

func (r *Runner) writeCSV() error {
	var b strings.Builder
	b.WriteString("topology,mode,clients,procs,spinup_ms,rss_total_mb,rss_per_client_kb,go_runtime_sys_mb,cpu_pct,goroutines,threads,heap_alloc_mb,alive,skipped\n")
	for _, x := range r.results {
		fmt.Fprintf(&b, "%s,%s,%d,%d,%.1f,%.2f,%.1f,%.2f,%.2f,%d,%d,%.2f,%d,%s\n",
			x.Topology, x.Mode, x.Clients, x.Procs, x.SpinupMS, x.RSSTotMB, x.RSSPerKB,
			x.SysMB, x.CPUPct, x.Goros, x.Threads, x.HeapMB, x.Alive, strings.ReplaceAll(x.Skipped, ",", ";"))
	}
	return os.WriteFile(r.OutPath, []byte(b.String()), 0o644)
}

func (r *Runner) printTable() {
	fmt.Println()
	fmt.Println("topo    mode  clients  procs      RSS      per-client   go heap    CPU%   goroutines   spinup   alive")
	fmt.Println("──────  ────  ───────  ─────  ─────────  ───────────  ─────────  ──────  ──────────  ────────  ─────")
	for _, x := range r.results {
		if x.Skipped != "" {
			fmt.Printf("%-6s  %-4s  %7d  %5d  %s\n", x.Topology, x.Mode, x.Clients, x.Procs, x.Skipped)
			continue
		}
		fmt.Printf("%-6s  %-4s  %7d  %5d  %6.1f MB  %7.1f KB  %6.1f MB  %6.1f  %10d  %6.0f ms  %5d\n",
			x.Topology, x.Mode, x.Clients, x.Procs, x.RSSTotMB, x.RSSPerKB, x.SysMB, x.CPUPct, x.Goros, x.SpinupMS, x.Alive)
	}
	fmt.Printf("\nwrote %s\n", r.OutPath)
}

// ------------------------------------------------------------ child handling

type Kid struct {
	cmd  *exec.Cmd
	mu   sync.Mutex
	last Child_Sample
}

func (k *Kid) consume(pipe interface{ Read([]byte) (int, error) }) {
	sc := bufio.NewScanner(pipe)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		var s Child_Sample
		if json.Unmarshal(sc.Bytes(), &s) != nil {
			continue
		}
		k.mu.Lock()
		k.last = s
		k.mu.Unlock()
	}
}

func (k *Kid) get() Child_Sample {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.last
}

type Kids struct{ list []*Kid }

type Kids_Snapshot struct {
	tsUS    int64
	cpuUS   int64
	goros   int
	threads int
	heap    uint64
	gosys   uint64
}

func (ks *Kids) pids() []int {
	out := make([]int, 0, len(ks.list))
	for _, k := range ks.list {
		if k.cmd.Process != nil {
			out = append(out, k.cmd.Process.Pid)
		}
	}
	return out
}

func (ks *Kids) waitReady(limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		ready := 0
		for _, k := range ks.list {
			if k.get().Ready {
				ready++
			}
		}
		if ready == len(ks.list) {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %d children", len(ks.list))
}

func (ks *Kids) snapshot() Kids_Snapshot {
	var s Kids_Snapshot
	for _, k := range ks.list {
		c := k.get()
		if c.TSUnixUS > s.tsUS {
			s.tsUS = c.TSUnixUS
		}
		s.cpuUS += c.CPUUserUS + c.CPUSysUS
		s.goros += c.Goroutines
		s.threads += c.Threads
		s.heap += c.HeapAllocB
		s.gosys += c.GoSysB
	}
	return s
}

func (ks *Kids) kill() {
	for _, k := range ks.list {
		if k.cmd.Process != nil {
			k.cmd.Process.Kill()
		}
	}
	for _, k := range ks.list {
		k.cmd.Wait()
	}
}

// rssKB sums resident set size over the given pids, in kilobytes.
func rssKB(pids []int) float64 {
	if len(pids) == 0 {
		return 0
	}
	var total float64
	for i := 0; i < len(pids); i += 400 { // ps argv is not infinite
		end := min(i+400, len(pids))
		strs := make([]string, 0, end-i)
		for _, p := range pids[i:end] {
			strs = append(strs, strconv.Itoa(p))
		}
		out, err := exec.Command("ps", "-o", "rss=", "-p", strings.Join(strs, ",")).Output()
		if err != nil {
			continue
		}
		for _, line := range strings.Fields(string(out)) {
			v, err := strconv.ParseFloat(line, 64)
			if err == nil {
				total += v
			}
		}
	}
	return total
}

// ------------------------------------------------------- the fake WA server

// FakeWA accepts the websocket, reads the client hello and never answers, so
// every client stays in the noise handshake with a live socket and read pump.
type FakeWA struct {
	url    string
	srv    *http.Server
	ln     net.Listener
	conns  atomic.Int64
	closed chan struct{}
}

func (f *FakeWA) start() error {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	f.ln = ln
	f.closed = make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		f.conns.Add(1)
		defer f.conns.Add(-1)
		ctx := r.Context()
		for {
			if _, _, err := c.Read(ctx); err != nil {
				return
			}
		}
	})
	f.srv = &http.Server{Handler: mux}
	go f.srv.Serve(ln)
	f.url = "ws://" + ln.Addr().String() + "/ws"
	return nil
}

func (f *FakeWA) reset() {}

func (f *FakeWA) waitConns(n int, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	debug := os.Getenv("BENCH_DEBUG") != ""
	var peak int64
	for time.Now().Before(deadline) {
		cur := f.conns.Load()
		if cur > peak {
			peak = cur
			if debug {
				fmt.Fprintf(os.Stderr, "[srv] conns=%d/%d\n", cur, n)
			}
		}
		if int(cur) >= n {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("only %d/%d websockets connected (peak %d)", f.conns.Load(), n, peak)
}

func (f *FakeWA) stop() {
	if f.srv != nil {
		f.srv.Shutdown(context.Background())
	}
}

// ------------------------------------------------------------------ helpers

func parseInts(s string) []int {
	var out []int
	for _, part := range strings.Split(s, ",") {
		if v, err := strconv.Atoi(strings.TrimSpace(part)); err == nil {
			out = append(out, v)
		}
	}
	sort.Ints(out)
	return out
}
