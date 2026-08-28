# bench

## What this measures

**It measures the resident cost of holding N whatsmeow clients alive, in the two
layouts you can actually deploy: N clients inside one process, or N processes
holding one client each.** Without this, "multi-número é leve" is a claim nobody
can reproduce.

Two connection modes, because the difference is most of the answer:

- `idle` — client constructed over a real sqlstore device, never connected. The floor.
- `ws` — client dialed into a local fake WhatsApp server, socket live, ClientHello sent.

Run it with `go run . -ladder 1,10,100,1000`. Results land in `results.csv`.

## What the fake server does, and what that costs the number

**The fake server accepts the websocket and never answers the Noise
ClientHello,** so each client holds a live socket and its read pump while parked
in `doHandshake`. That is as close to "connected" as you get without pairing N
real phone numbers.

The gap: a fully logged-in client also runs `keepAliveLoop` and
`handlerQueueLoop` (2 more goroutines) and holds decrypted session state per
chat. So `ws` is a floor for a connected client, not a ceiling for a busy one.

`whatsmeow.NoiseHandshakeResponseTimeout` is 20s, so every socket dies 20s after
it opens. The `alive` column is the server's live socket count at sampling time —
if it is below `clients`, the window slipped past the timeout and that row's
memory number is measuring a disconnected client.

## How the numbers are taken

**RSS comes from `ps` in the parent; CPU and Go heap come from `getrusage` and
`runtime.MemStats` inside each child**, sampled twice, `settle` apart then
`window` apart, and differenced. CPU% is `Δ(utime+stime) / Δwall`.

Summed RSS over many processes double-counts the shared binary text, so it
overstates the process layout. `go_runtime_sys_mb` is the honest companion: it
is memory the Go runtime itself took from the OS, with no shared pages in it.

The sampler must never touch a client lock. `Client.Connect` holds `socketLock`
for the entire 20s handshake, so calling `IsConnected()` from the sampler
deadlocks the child and the parent then waits out its full readiness timeout.

## The budget guard

**A process case is skipped when its projected RSS exceeds `-rss-budget-gb`
(default 10),** projected from the largest already-measured case of the same
shape. 1000 Go processes on a 16 GB machine is a swap storm, and a benchmark
that swaps measures the swap.

Force it with `-rss-budget-gb 999` only if the machine can hold it.
