package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// Child is one benchmarked process: it holds N whatsmeow clients alive and
// prints a Sample line to stdout every tick so the parent can measure it.
type Child struct {
	N     int
	Mode  string // "idle" (constructed only) or "ws" (connected to the fake server)
	WSURL string
	DB    string

	clients []*whatsmeow.Client
	ready   bool
}

// Sample is one JSON line on the child's stdout.
type Child_Sample struct {
	TSUnixUS   int64  `json:"ts_us"`
	Ready      bool   `json:"ready"`
	Clients    int    `json:"clients"`
	Goroutines int    `json:"goroutines"`
	Threads    int    `json:"threads"`
	HeapAllocB uint64 `json:"heap_alloc_b"`
	HeapSysB   uint64 `json:"heap_sys_b"`
	GoSysB     uint64 `json:"go_sys_b"`
	CPUUserUS  int64  `json:"cpu_user_us"`
	CPUSysUS   int64  `json:"cpu_sys_us"`
}

func (c *Child) run() error {
	if err := c.build(); err != nil {
		return err
	}
	c.ready = true
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	enc := json.NewEncoder(os.Stdout)
	for {
		if err := enc.Encode(c.sample()); err != nil {
			return err
		}
		<-tick.C
	}
}

// build creates the store, the devices and the clients. In ws mode Connect is
// launched in a goroutine because the noise handshake blocks until the server
// answers, and the fake server deliberately never does.
func (c *Child) build() error {
	ctx := context.Background()
	dsn := fmt.Sprintf("file:%s?_foreign_keys=on&_journal_mode=WAL", c.DB)
	container, err := sqlstore.New(ctx, "sqlite3", dsn, waLog.Noop)
	if err != nil {
		return fmt.Errorf("sqlstore: %w", err)
	}
	c.clients = make([]*whatsmeow.Client, 0, c.N)
	for i := 0; i < c.N; i++ {
		cli := whatsmeow.NewClient(container.NewDevice(), waLog.Noop)
		cli.EnableAutoReconnect = false
		cli.AddEventHandler(func(evt any) {})
		if c.Mode == "ws" {
			cli.MessengerConfig = &whatsmeow.MessengerConfig{
				UserAgent:    "bench",
				BaseURL:      "http://127.0.0.1",
				WebsocketURL: c.WSURL,
			}
			go func(i int) {
				if err := cli.Connect(); err != nil {
					fmt.Fprintf(os.Stderr, "connect %d: %v\n", i, err)
				}
			}(i)
		}
		c.clients = append(c.clients, cli)
	}
	return nil
}

func (c *Child) sample() Child_Sample {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return Child_Sample{
		TSUnixUS:   time.Now().UnixMicro(),
		Ready:      c.ready,
		Clients:    len(c.clients),
		Goroutines: runtime.NumGoroutine(),
		Threads:    pprof.Lookup("threadcreate").Count(),
		HeapAllocB: mem.HeapAlloc,
		HeapSysB:   mem.HeapSys,
		GoSysB:     mem.Sys,
		CPUUserUS:  ru.Utime.Sec*1e6 + int64(ru.Utime.Usec),
		CPUSysUS:   ru.Stime.Sec*1e6 + int64(ru.Stime.Usec),
	}
}
