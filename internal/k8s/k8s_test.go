package k8s

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

const ns = "redpanda"

func headless(name string, selector, lbls map[string]string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: lbls},
		Spec:       corev1.ServiceSpec{ClusterIP: corev1.ClusterIPNone, Selector: selector},
	}
}

func brokerPod(name string, lbls map[string]string, volumes ...corev1.Volume) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: lbls},
		Spec: corev1.PodSpec{
			Hostname: name,
			Containers: []corev1.Container{{
				Name:  "redpanda",
				Ports: []corev1.ContainerPort{{Name: "kafka", ContainerPort: 9093}, {Name: "admin", ContainerPort: 9644}},
			}},
			Volumes: volumes,
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.1"},
	}
}

func TestDiscoverBrokers_SkipsSelectorlessHeadless(t *testing.T) {
	rp := map[string]string{"app.kubernetes.io/name": "redpanda", "app.kubernetes.io/instance": "rp"}
	objs := []runtime.Object{
		// Flat-network StretchCluster per-pod Service: headless, no
		// selector. Sorts before "rp" so the old code picked it.
		headless("a-per-pod-0", nil, rp),
		headless("rp", map[string]string{"app.kubernetes.io/instance": "rp"}, rp),
		brokerPod("rp-central-0", rp),
		brokerPod("rp-central-1", rp),
		brokerPod("operator-abc", map[string]string{"app": "operator"}),
	}
	cl := fake.NewSimpleClientset(objs...)

	brokers, err := DiscoverBrokers(context.Background(), cl, ns)
	if err != nil {
		t.Fatal(err)
	}
	if len(brokers) != 2 {
		t.Fatalf("got %d brokers, want 2: %+v", len(brokers), brokers)
	}
	for _, b := range brokers {
		if !strings.HasPrefix(b.Address, b.Pod.Name+".rp.redpanda.svc.") || !strings.HasSuffix(b.Address, ":9644") {
			t.Errorf("unexpected address %q", b.Address)
		}
		if strings.Contains(b.Address, ".:") {
			t.Errorf("address %q keeps the trailing dot of the cluster domain", b.Address)
		}
		if b.AdminPort != 9644 {
			t.Errorf("admin port %d, want 9644", b.AdminPort)
		}
	}
}

func TestHeadlessService_PrefersRedpandaLabel(t *testing.T) {
	cl := fake.NewSimpleClientset(
		headless("aaa-other", map[string]string{"app": "other"}, nil),
		headless("redpanda", map[string]string{"app.kubernetes.io/name": "redpanda"}, map[string]string{"app.kubernetes.io/name": "redpanda"}),
	)
	svc, err := headlessService(context.Background(), cl, ns)
	if err != nil {
		t.Fatal(err)
	}
	if svc.Name != "redpanda" {
		t.Fatalf("picked %q, want redpanda", svc.Name)
	}
}

func TestDiscoverBrokers_SkipsPodsThatCantServe(t *testing.T) {
	rp := map[string]string{"app.kubernetes.io/name": "redpanda"}
	pending := brokerPod("rp-1", rp)
	pending.Status = corev1.PodStatus{Phase: corev1.PodPending}
	deleting := brokerPod("rp-2", rp)
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	deleting.Finalizers = []string{"test"}
	cl := fake.NewSimpleClientset(headless("rp", rp, rp), brokerPod("rp-0", rp), pending, deleting)

	brokers, err := DiscoverBrokers(context.Background(), cl, ns)
	if err != nil {
		t.Fatal(err)
	}
	if len(brokers) != 1 || brokers[0].Pod.Name != "rp-0" {
		t.Fatalf("got %d brokers, want only rp-0", len(brokers))
	}
}

func TestNormalizeAddr(t *testing.T) {
	for in, want := range map[string]string{
		"rp-0.rp.redpanda.svc.cluster.local.:9644": "rp-0.rp.redpanda.svc.cluster.local:9644",
		"rp-0.rp.redpanda.svc.cluster.local:9644":  "rp-0.rp.redpanda.svc.cluster.local:9644",
		"not-an-address": "not-an-address",
	} {
		if got := normalizeAddr(in); got != want {
			t.Errorf("normalizeAddr(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestHeadlessService_NoneUsable(t *testing.T) {
	cl := fake.NewSimpleClientset(headless("per-pod", nil, nil))
	if _, err := headlessService(context.Background(), cl, ns); err == nil {
		t.Fatal("expected an error when the only headless service has no selector")
	}
}

// tlsBroker returns a broker whose address is the TLS test server and whose
// Host matches a SAN in the test server's certificate.
func tlsBroker(srv *httptest.Server, volumes ...corev1.Volume) (Broker, DialFunc) {
	addr := srv.Listener.Addr().String()
	b := Broker{Pod: *brokerPod("rp-0", nil, volumes...), Address: "example.com:9644", Host: "example.com", AdminPort: 9644}
	dial := func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}
	return b, dial
}

func secretVolume(name string) corev1.Volume {
	return corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: name}}}
}

// mount adds a volume mount at path to the pod's redpanda container.
func mount(p *corev1.Pod, volume, path string) {
	p.Spec.Containers[0].VolumeMounts = append(p.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: volume, MountPath: path})
}

// secretGets counts Secret reads on a fake clientset.
func secretGets(cl *fake.Clientset) []string {
	var names []string
	for _, a := range cl.Actions() {
		if a.GetVerb() == "get" && a.GetResource().Resource == "secrets" {
			names = append(names, a.(interface{ GetName() string }).GetName())
		}
	}
	return names
}

func TestDetectAdminTLS_Plaintext(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	b, dial := tlsBroker(srv)
	res, err := DetectAdminTLS(context.Background(), fake.NewSimpleClientset(), b, dial, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if res.UsesTLS {
		t.Fatal("plaintext listener reported as TLS")
	}
}

func TestDetectAdminTLS_CAFromMountedSecret(t *testing.T) {
	srv := httptest.NewTLSServer(http.NotFoundHandler())
	defer srv.Close()
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})

	cl := fake.NewSimpleClientset(
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "unrelated", Namespace: ns}, Data: map[string][]byte{"ca.crt": []byte("not a cert")}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "rp-default-cert", Namespace: ns}, Data: map[string][]byte{"ca.crt": caPEM}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "rp-superusers", Namespace: ns}, Data: map[string][]byte{"users.txt": []byte("admin:secret")}},
	)
	b, dial := tlsBroker(srv, secretVolume("unrelated"), secretVolume("rp-default-cert"), secretVolume("rp-superusers"))
	mount(&b.Pod, "unrelated", "/etc/tls/certs/unrelated")
	mount(&b.Pod, "rp-default-cert", "/etc/tls/certs/default")
	mount(&b.Pod, "rp-superusers", "/etc/secrets/users")

	res, err := DetectAdminTLS(context.Background(), cl, b, dial, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if !res.UsesTLS || res.RootCAs == nil {
		t.Fatalf("got %+v, want TLS with a CA", res)
	}
	for _, n := range secretGets(cl) {
		if n == "rp-superusers" {
			t.Error("read the superusers Secret, which isn't mounted under /etc/tls/")
		}
	}
}

func TestDetectAdminTLS_ProjectedSecret(t *testing.T) {
	srv := httptest.NewTLSServer(http.NotFoundHandler())
	defer srv.Close()
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	cl := fake.NewSimpleClientset(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "rp-ca", Namespace: ns}, Data: map[string][]byte{"ca.crt": caPEM}})
	vol := corev1.Volume{Name: "certs", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
		Sources: []corev1.VolumeProjection{{Secret: &corev1.SecretProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "rp-ca"}}}},
	}}}
	b, dial := tlsBroker(srv, vol)
	mount(&b.Pod, "certs", "/etc/tls/certs/default")

	res, err := DetectAdminTLS(context.Background(), cl, b, dial, nil, false)
	if err != nil || !res.UsesTLS || res.RootCAs == nil {
		t.Fatalf("got %+v, %v; want TLS with the projected CA", res, err)
	}
}

func TestDetectAdminTLS_CandidateSkipsSecretReads(t *testing.T) {
	srv := httptest.NewTLSServer(http.NotFoundHandler())
	defer srv.Close()
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	cl := fake.NewSimpleClientset()
	b, dial := tlsBroker(srv, secretVolume("rp-default-cert"))
	mount(&b.Pod, "rp-default-cert", "/etc/tls/certs/default")

	res, err := DetectAdminTLS(context.Background(), cl, b, dial, []CACandidate{{Source: "--admin-tls-ca", Pool: pool}}, false)
	if err != nil || res.RootCAs != pool {
		t.Fatalf("got %+v, %v; want the candidate CA", res, err)
	}
	if got := secretGets(cl); len(got) != 0 {
		t.Errorf("read Secrets %v although --admin-tls-ca verified", got)
	}
}

func TestDetectAdminTLS_NoVerifyingCA(t *testing.T) {
	srv := httptest.NewTLSServer(http.NotFoundHandler())
	defer srv.Close()
	b, dial := tlsBroker(srv)
	_, err := DetectAdminTLS(context.Background(), fake.NewSimpleClientset(), b, dial, nil, false)
	if err == nil || !strings.Contains(err.Error(), "--admin-tls-ca") {
		t.Fatalf("got %v, want an error that mentions --admin-tls-ca", err)
	}
}

func TestDetectAdminTLS_SkipVerify(t *testing.T) {
	srv := httptest.NewTLSServer(http.NotFoundHandler())
	defer srv.Close()
	b, dial := tlsBroker(srv)
	res, err := DetectAdminTLS(context.Background(), fake.NewSimpleClientset(), b, dial, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if !res.UsesTLS || res.RootCAs != nil {
		t.Fatalf("got %+v, want TLS without a CA", res)
	}
}
