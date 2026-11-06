# zap — the tasks worth a name.
# `just` lists them; `just <recipe>` runs one.

set shell := ["bash", "-uc"]

sandbox := "/tmp/zap-sandbox"

_default:
    @just --list --unsorted

# ---------------------------------------------------------------- the code

# build every module and install zap on PATH
build:
    cd zap && go build -o zap . && go install .

# the whole check: format, vet, build, suite
check:
    cd zap && gofmt -l . && go vet ./... && go build -o zap .
    cd tests && go test ./...

# the suite alone, verbose
test:
    cd tests && go test -v ./...

# regenerate the json schema from the Go structs
schema: build
    cd zap && ./zap schema > schema.json && git diff --stat schema.json

# the footprint benchmark: N clients in one process vs N processes
bench ladder="1,10,100,1000":
    cd bench && go build -o bench . && ./bench -ladder {{ladder}}

# the extension: unit test of update.xml/policy, then the fake WhatsApp page in Chromium
ext-test:
    bash -n skills/zap/scripts/extension.sh
    python3 -I tests/extension/test_policy.py
    PLAYWRIGHT_BROWSERS_PATH=${ZAP_PW_BROWSERS:-$HOME/.cache/zap-ext-pw} uv run --with playwright python -I tests/extension/test_extension.py

# ------------------------------------------------------------- the daemon

# a sandbox daemon: fake numbers, no phone, no ban risk
fake: build
    @mkdir -p {{sandbox}}
    ZAP_HOME={{sandbox}} zap serve --fake

# seed the sandbox with a contact and one inbound message
fake-seed:
    ZAP_HOME={{sandbox}} zap fake contact +5511900000001 "Ana"
    ZAP_HOME={{sandbox}} zap fake inbound --from +5511900000001 "oi, tudo bem?"
    ZAP_HOME={{sandbox}} zap messages --plain-text

# what the real daemon is doing right now
status:
    @zap accounts
    @zap queue || true

# ------------------------------------------------------------- the graph

# build the knowledge graph of this repo (writes graphify-out/)
graph:
    graphify . --no-viz

# rebuild only what changed
graph-update:
    graphify . --update

# build with the interactive HTML visualisation
graph-viz:
    graphify .

# ask the graph a question — just ask "where does an event become a message"
ask question:
    graphify query "{{question}}"

# how two things in this repo connect — just link Bus Store
link a b:
    graphify path "{{a}}" "{{b}}"

# plain-language explanation of one node — just explain Transcriber
explain node:
    graphify explain "{{node}}"

# the report the graph wrote about this repo
graph-report:
    @cat graphify-out/GRAPH_REPORT.md

# ---------------------------------------------------------------- reading

# the outline of every Go file: the design, before the details
outline:
    @for f in zap/main.go zap/core/*.go; do \
        echo "── $f"; \
        grep -nE '^(func|type|const|var) ' "$f" | sed 's/ {$//' | sed 's/^/   /'; \
    done

# every verb and flag zap has
verbs:
    @cd zap && ./zap -h

# the event catalogue, straight from the binary
events:
    @zap events
