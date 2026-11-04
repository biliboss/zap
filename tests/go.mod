// The test suite is its own module so it can live outside the binary it
// exercises: a package in tests/ cannot be imported by zap, which is exactly
// the dependency direction a test suite should have.
module tests

go 1.26.4

require (
	github.com/nats-io/nats-server/v2 v2.15.2
	github.com/nats-io/nats.go v1.55.0
	zap v0.0.0
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/BurntSushi/toml v1.6.0 // indirect
	github.com/antithesishq/antithesis-sdk-go v0.8.0-default-no-op // indirect
	github.com/beeper/argo-go v1.1.2 // indirect
	github.com/coder/websocket v1.8.15 // indirect
	github.com/elliotchance/orderedmap/v3 v3.1.0 // indirect
	github.com/google/go-tpm v0.9.8 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-sqlite3 v1.14.50 // indirect
	github.com/mdp/qrterminal/v3 v3.2.1 // indirect
	github.com/minio/highwayhash v1.0.4 // indirect
	github.com/nats-io/jwt/v2 v2.8.2 // indirect
	github.com/nats-io/nkeys v0.4.16 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/petermattis/goid v0.0.0-20260816044145-ed329add6b1b // indirect
	github.com/rs/zerolog v1.35.1 // indirect
	github.com/vektah/gqlparser/v2 v2.5.27 // indirect
	go.mau.fi/libsignal v0.2.2 // indirect
	go.mau.fi/util v0.10.1-0.20260820140024-eb612d936fde // indirect
	go.mau.fi/whatsmeow v0.0.0-20260821141805-33cfac511629 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/exp v0.0.0-20260813180055-c1d0aacb2297 // indirect
	golang.org/x/net v0.60.0 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/term v0.47.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	golang.org/x/time v0.16.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
	rsc.io/qr v0.2.0 // indirect
)

replace zap => ../zap
