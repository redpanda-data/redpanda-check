package k8s

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/client-go/kubernetes/fake"
)

// fakeStream is one end of an in-memory pipe.
type fakeStream struct {
	net.Conn
	headers http.Header
}

func (s *fakeStream) Reset() error         { return s.Close() }
func (s *fakeStream) Headers() http.Header { return s.headers }
func (s *fakeStream) Identifier() uint32   { return 0 }

// fakeConn hands out data streams whose other end echoes, and error streams
// that stay empty. block makes CreateStream hang until the conn closes.
type fakeConn struct {
	mu      sync.Mutex
	closed  chan bool
	once    sync.Once
	streams int
	block   bool
	fail    bool
}

func newFakeConn() *fakeConn { return &fakeConn{closed: make(chan bool)} }

func (c *fakeConn) CreateStream(h http.Header) (httpstream.Stream, error) {
	if c.block {
		<-c.closed
		return nil, errors.New("connection closed")
	}
	if c.fail {
		return nil, errors.New("stream refused")
	}
	select {
	case <-c.closed:
		return nil, errors.New("connection closed")
	default:
	}
	c.mu.Lock()
	c.streams++
	c.mu.Unlock()
	a, b := net.Pipe()
	if h.Get(corev1.StreamType) == corev1.StreamTypeData {
		go io.Copy(b, b) //nolint:errcheck // echo
	} else {
		go func() { <-c.closed; b.Close() }()
	}
	return &fakeStream{Conn: a, headers: h.Clone()}, nil
}

func (c *fakeConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}
func (c *fakeConn) CloseChan() <-chan bool                     { return c.closed }
func (c *fakeConn) SetIdleTimeout(time.Duration)               {}
func (c *fakeConn) RemoveStreams(streams ...httpstream.Stream) {}

func testDialer(conns ...*fakeConn) (*PodDialer, *int) {
	b := Broker{Pod: *brokerPod("rp-0", nil), Address: "rp-0.rp.redpanda.svc.cluster.local:9644", AdminPort: 9644}
	d := NewPodDialer(&Client{Kube: fake.NewSimpleClientset()}, ns, []Broker{b})
	connects := 0
	d.connectFn = func(context.Context, Broker) (httpstream.Connection, error) {
		if connects >= len(conns) {
			return nil, errors.New("no more connections")
		}
		c := conns[connects]
		connects++
		return c, nil
	}
	return d, &connects
}

func echo(t *testing.T, c net.Conn) {
	t.Helper()
	if _, err := c.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("got %q, %v", buf, err)
	}
}

func TestPodDialer_ReusesConnectionPerPod(t *testing.T) {
	fc := newFakeConn()
	d, connects := testDialer(fc)
	defer d.Close()
	for i := 0; i < 3; i++ {
		c, err := d.DialContext(context.Background(), "tcp", "rp-0.rp.redpanda.svc.cluster.local.:9644")
		if err != nil {
			t.Fatal(err)
		}
		echo(t, c)
		c.Close()
	}
	if *connects != 1 {
		t.Errorf("opened %d connections for 3 dials, want 1", *connects)
	}
	if fc.streams != 6 {
		t.Errorf("opened %d streams, want an error and a data stream per dial (6)", fc.streams)
	}
}

func TestPodDialer_ReconnectsAfterClose(t *testing.T) {
	first, second := newFakeConn(), newFakeConn()
	d, connects := testDialer(first, second)
	defer d.Close()
	c, err := d.DialContext(context.Background(), "tcp", "rp-0.rp.redpanda.svc.cluster.local:9644")
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	first.Close()
	c, err = d.DialContext(context.Background(), "tcp", "rp-0.rp.redpanda.svc.cluster.local:9644")
	if err != nil {
		t.Fatal(err)
	}
	echo(t, c)
	if *connects != 2 {
		t.Errorf("opened %d connections, want a new one after the first closed", *connects)
	}
}

func TestPodDialer_StreamFailureKeepsLiveSharedConnection(t *testing.T) {
	shared, own := newFakeConn(), newFakeConn()
	d, _ := testDialer(shared, own)
	defer d.Close()
	busy, err := d.DialContext(context.Background(), "tcp", "rp-0.rp.redpanda.svc.cluster.local:9644")
	if err != nil {
		t.Fatal(err)
	}

	shared.fail = true
	c, err := d.DialContext(context.Background(), "tcp", "rp-0.rp.redpanda.svc.cluster.local:9644")
	if err != nil {
		t.Fatal(err)
	}
	echo(t, c)
	c.Close()
	select {
	case <-own.CloseChan():
	default:
		t.Error("the dial's own connection wasn't closed with it")
	}
	select {
	case <-shared.CloseChan():
		t.Fatal("closed the shared connection, which still had a request in flight")
	default:
	}
	echo(t, busy)
}

func TestPodDialer_StreamOpenHonorsContext(t *testing.T) {
	fc := newFakeConn()
	fc.block = true
	d, _ := testDialer(fc)
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := d.DialContext(ctx, "tcp", "rp-0.rp.redpanda.svc.cluster.local:9644")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want the context deadline", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Errorf("took %v to give up", time.Since(start))
	}
}
