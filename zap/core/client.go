package core

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"

	"github.com/mdp/qrterminal/v3"
)

// Client is the thin side of zap: every verb that is not `serve` talks to the
// daemon over the unix socket with plain HTTP.
type Client struct {
	socket string
	http   *http.Client
}

func NewClient(socket string) *Client {
	dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}
	return &Client{
		socket: socket,
		http:   &http.Client{Transport: &http.Transport{DialContext: dial}},
	}
}

func (c *Client) JSON(ctx context.Context, method, path string, body, out any) error {
	resp, err := c.do(ctx, method, path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// stream reads an ndjson response line by line until the server closes it.
func (c *Client) Stream(ctx context.Context, method, path string, body any, onLine func([]byte) error) error {
	resp, err := c.do(ctx, method, path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 4<<20)
	for sc.Scan() {
		if len(bytes.TrimSpace(sc.Bytes())) == 0 {
			continue
		}
		if err := onLine(sc.Bytes()); err != nil {
			return err
		}
	}
	if err := sc.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func (c *Client) do(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var payload io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		payload = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://zap"+path, payload)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		if isDialError(err) {
			return nil, fmt.Errorf("no daemon on %s — run `zap serve`", c.socket)
		}
		return nil, err
	}
	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		var e struct {
			Error string `json:"error"`
		}
		json.NewDecoder(resp.Body).Decode(&e)
		if e.Error == "" {
			e.Error = resp.Status
		}
		return nil, errors.New(e.Error)
	}
	return resp, nil
}

func isDialError(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "dial"
}

func Decode(line []byte, v any) error { return json.Unmarshal(line, v) }

// qrPainter redraws the QR in place. WhatsApp rotates the code every ~20s, and
// printing each one below the last scrolls the scannable one off the screen.
type QRPainter struct {
	w     io.Writer
	tty   bool
	lines int
}

func NewQRPainter() *QRPainter {
	return &QRPainter{w: os.Stderr, tty: IsTTY(os.Stderr)}
}

func (p *QRPainter) Paint(code string) {
	var buf bytes.Buffer
	qrterminal.GenerateWithConfig(code, qrterminal.Config{
		Level:      qrterminal.L,
		Writer:     &buf,
		HalfBlocks: true,
		BlackChar:  qrterminal.BLACK_BLACK,
		WhiteChar:  qrterminal.WHITE_WHITE,
		QuietZone:  1,
	})
	fmt.Fprintln(&buf, "scan it: WhatsApp → Linked devices → Link a device")

	if p.tty && p.lines > 0 {
		fmt.Fprintf(p.w, "\033[%dA\033[0J", p.lines) // up N lines, clear below
	}
	p.lines = bytes.Count(buf.Bytes(), []byte("\n"))
	p.w.Write(buf.Bytes())
}

func IsTTY(f *os.File) bool {
	info, err := f.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}
