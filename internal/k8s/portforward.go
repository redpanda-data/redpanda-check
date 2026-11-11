package k8s

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

// DialFunc matches net.Dialer.DialContext.
type DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// PodDialer dials broker admin addresses through the Kubernetes API's
// pods/portforward subresource, so in-cluster FQDNs work from outside the
// cluster. The caller keeps using the FQDNs in its URLs, which keeps TLS
// server-name checks matching the broker certificates. Addresses that don't
// belong to a known broker are dialed directly.
//
// Like kubectl port-forward, it keeps one port-forward connection per pod
// and opens a new stream pair on it for each dial. It tries WebSockets first
// and falls back to SPDY.
type PodDialer struct {
	kube      kubernetes.Interface
	config    *rest.Config
	namespace string
	targets   map[string]Broker
	direct    net.Dialer

	mu        sync.Mutex
	conns     map[string]httpstream.Connection
	requestID int

	// connectFn opens a port-forward connection to a pod. Tests replace it.
	connectFn func(ctx context.Context, b Broker) (httpstream.Connection, error)
}

// NewPodDialer returns a dialer that port-forwards to the given brokers.
func NewPodDialer(c *Client, namespace string, brokers []Broker) *PodDialer {
	targets := make(map[string]Broker, len(brokers))
	for _, b := range brokers {
		targets[normalizeAddr(b.Address)] = b
	}
	d := &PodDialer{
		kube:      c.Kube,
		config:    c.Config,
		namespace: namespace,
		targets:   targets,
		direct:    net.Dialer{Timeout: 10 * time.Second},
		conns:     map[string]httpstream.Connection{},
	}
	d.connectFn = d.connect
	return d
}

// normalizeAddr drops a trailing dot from the host, so a redirect to
// "<fqdn>.:9644" still maps to its broker.
func normalizeAddr(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return net.JoinHostPort(strings.TrimSuffix(host, "."), port)
}

// DialContext implements DialFunc.
func (d *PodDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	b, ok := d.targets[normalizeAddr(addr)]
	if !ok {
		return d.direct.DialContext(ctx, network, addr)
	}
	conn, err := d.podConnection(ctx, b)
	if err != nil {
		return nil, err
	}
	pc, err := d.openStreamsCtx(ctx, conn, b)
	if err == nil {
		return pc, nil
	}
	if ctx.Err() != nil {
		return nil, err
	}
	// Forget the shared connection only if it's dead: closing a live one
	// would reset every other request in flight on it. Either way, use a
	// connection of its own for this dial.
	select {
	case <-conn.CloseChan():
		d.forget(b.Pod.Name, conn)
	default:
	}
	own, err := d.connectFn(ctx, b)
	if err != nil {
		return nil, err
	}
	pc, err = d.openStreamsCtx(ctx, own, b)
	if err != nil {
		own.Close()
		return nil, err
	}
	pc.ownsConn = true
	return pc, nil
}

// Close closes every port-forward connection.
func (d *PodDialer) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for name, c := range d.conns {
		c.Close()
		delete(d.conns, name)
	}
}

func (d *PodDialer) forget(pod string, c httpstream.Connection) {
	d.mu.Lock()
	if d.conns[pod] == c {
		delete(d.conns, pod)
	}
	d.mu.Unlock()
}

// podConnection returns the cached port-forward connection to the pod, or
// opens one.
func (d *PodDialer) podConnection(ctx context.Context, b Broker) (httpstream.Connection, error) {
	d.mu.Lock()
	if c, ok := d.conns[b.Pod.Name]; ok {
		select {
		case <-c.CloseChan():
			delete(d.conns, b.Pod.Name)
		default:
			d.mu.Unlock()
			return c, nil
		}
	}
	d.mu.Unlock()

	c, err := d.connectFn(ctx, b)
	if err != nil {
		return nil, err
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if existing, ok := d.conns[b.Pod.Name]; ok {
		// Another dial won the race; use its connection.
		c.Close()
		return existing, nil
	}
	d.conns[b.Pod.Name] = c
	return c, nil
}

func (d *PodDialer) connect(ctx context.Context, b Broker) (httpstream.Connection, error) {
	u := d.kube.CoreV1().RESTClient().Post().
		Resource("pods").Namespace(d.namespace).Name(b.Pod.Name).
		SubResource("portforward").URL()

	transport, upgrader, err := spdy.RoundTripperFor(d.config)
	if err != nil {
		return nil, fmt.Errorf("unable to set up port-forward to pod %s: %v", b.Pod.Name, err)
	}
	var dialer httpstream.Dialer = spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, u)
	if ws, err := portforward.NewSPDYOverWebsocketDialer(u, d.config); err == nil {
		dialer = portforward.NewFallbackDialer(ws, dialer, func(err error) bool {
			return httpstream.IsUpgradeFailure(err) || httpstream.IsHTTPSProxyError(err)
		})
	}

	type result struct {
		conn httpstream.Connection
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		conn, _, err := dialer.Dial(portforward.PortForwardProtocolV1Name)
		ch <- result{conn, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			return nil, portForwardError(b.Pod.Name, r.err)
		}
		return r.conn, nil
	case <-ctx.Done():
		go func() {
			if r := <-ch; r.conn != nil {
				r.conn.Close()
			}
		}()
		return nil, ctx.Err()
	}
}

// openStreamsCtx is openStreams, bounded by ctx. Creating a stream waits up
// to 30s for the server's reply, which on a half-open connection would
// outlast the caller's timeout.
func (d *PodDialer) openStreamsCtx(ctx context.Context, conn httpstream.Connection, b Broker) (*podConn, error) {
	type result struct {
		pc  *podConn
		err error
	}
	ch := make(chan result, 1)
	go func() {
		pc, err := d.openStreams(conn, b)
		ch <- result{pc, err}
	}()
	select {
	case r := <-ch:
		return r.pc, r.err
	case <-ctx.Done():
		go func() {
			if r := <-ch; r.pc != nil {
				r.pc.Close()
			}
		}()
		return nil, ctx.Err()
	}
}

// openStreams opens an error and a data stream for one forwarded connection.
func (d *PodDialer) openStreams(conn httpstream.Connection, b Broker) (*podConn, error) {
	d.mu.Lock()
	d.requestID++
	id := d.requestID
	d.mu.Unlock()

	headers := http.Header{}
	headers.Set(corev1.StreamType, corev1.StreamTypeError)
	headers.Set(corev1.PortHeader, strconv.Itoa(int(b.AdminPort)))
	headers.Set(corev1.PortForwardRequestIDHeader, strconv.Itoa(id))
	errStream, err := conn.CreateStream(headers)
	if err != nil {
		return nil, fmt.Errorf("unable to open port-forward error stream to pod %s: %v", b.Pod.Name, err)
	}
	// Only the server writes to the error stream.
	errStream.Close()

	headers.Set(corev1.StreamType, corev1.StreamTypeData)
	dataStream, err := conn.CreateStream(headers)
	if err != nil {
		conn.RemoveStreams(errStream)
		return nil, fmt.Errorf("unable to open port-forward data stream to pod %s: %v", b.Pod.Name, err)
	}

	pc := &podConn{
		conn:      conn,
		stream:    dataStream,
		errStream: errStream,
		local:     podAddr("port-forward"),
		remote:    podAddr(fmt.Sprintf("%s/%s:%d", d.namespace, b.Pod.Name, b.AdminPort)),
	}
	go pc.watchErrors()
	return pc, nil
}

func portForwardError(pod string, err error) error {
	msg := err.Error()
	if strings.Contains(msg, "forbidden") || strings.Contains(msg, "Forbidden") {
		return fmt.Errorf("port-forward to pod %s was denied; the admin API checks need 'create' on pods/portforward: %v", pod, err)
	}
	return fmt.Errorf("unable to port-forward to pod %s: %v", pod, err)
}

// podConn adapts one port-forward stream pair to net.Conn. Closing it closes
// only its streams; the pod's connection stays open for other dials.
// Deadlines aren't supported by the stream; the HTTP client cancels by
// closing the conn.
type podConn struct {
	conn      httpstream.Connection
	stream    httpstream.Stream
	errStream httpstream.Stream
	local     net.Addr
	remote    net.Addr
	// ownsConn is set when conn was opened for this podConn alone.
	ownsConn bool

	mu      sync.Mutex
	err     error
	closeMu sync.Once
}

func (c *podConn) watchErrors() {
	msg, err := io.ReadAll(c.errStream)
	if err == nil && len(msg) > 0 {
		c.mu.Lock()
		c.err = fmt.Errorf("port-forward to %s failed: %s", c.remote, msg)
		c.mu.Unlock()
		c.Close()
	}
}

func (c *podConn) streamErr(err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	return err
}

func (c *podConn) Read(p []byte) (int, error) {
	n, err := c.stream.Read(p)
	if err != nil && err != io.EOF {
		err = c.streamErr(err)
	}
	return n, err
}

func (c *podConn) Write(p []byte) (int, error) {
	n, err := c.stream.Write(p)
	if err != nil {
		err = c.streamErr(err)
	}
	return n, err
}

func (c *podConn) Close() error {
	c.closeMu.Do(func() {
		c.stream.Close()
		c.stream.Reset()
		c.conn.RemoveStreams(c.stream, c.errStream)
		if c.ownsConn {
			c.conn.Close()
		}
	})
	return nil
}

func (c *podConn) LocalAddr() net.Addr              { return c.local }
func (c *podConn) RemoteAddr() net.Addr             { return c.remote }
func (c *podConn) SetDeadline(time.Time) error      { return nil }
func (c *podConn) SetReadDeadline(time.Time) error  { return nil }
func (c *podConn) SetWriteDeadline(time.Time) error { return nil }

type podAddr string

func (a podAddr) Network() string { return "portforward" }
func (a podAddr) String() string  { return string(a) }
