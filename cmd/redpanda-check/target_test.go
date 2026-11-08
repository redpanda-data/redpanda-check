package main

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/vuldin/redpanda-check/internal/admin"
	"github.com/vuldin/redpanda-check/internal/k8s"
)

func target(t *testing.T, source string, cfg admin.Config, addrs ...string) *adminTarget {
	t.Helper()
	cfg.Addresses = addrs
	tg, err := newTarget(cfg, source, source == "disc", nil)
	if err != nil {
		t.Fatal(err)
	}
	return tg
}

func TestChooseAdmin(t *testing.T) {
	const ns = "redpanda"
	type uuids map[string]string // source -> uuid; missing means unreachable

	creds := admin.Config{SASLUser: "admin", SASLPassword: "secret"}
	tests := []struct {
		name        string
		flag, disc  bool
		discErr     error
		nsRequested bool
		uuids       uuids
		wantSource  string
		wantWarn    string
		wantCreds   bool
		wantErr     bool
	}{
		{name: "nothing", wantErr: true},
		{name: "flag only, no namespace", flag: true, wantSource: "flag", wantCreds: true},
		{name: "flag only, discovery failed", flag: true, nsRequested: true, discErr: errors.New("forbidden"), wantSource: "flag", wantWarn: "may be a different cluster", wantCreds: true},
		{name: "discovered only", disc: true, nsRequested: true, wantSource: "disc", wantCreds: true},
		{name: "same cluster keeps flag", flag: true, disc: true, nsRequested: true, uuids: uuids{"flag": "u1", "disc": "u1"}, wantSource: "flag", wantCreds: true},
		{name: "different cluster drops its credentials", flag: true, disc: true, nsRequested: true, uuids: uuids{"flag": "u2", "disc": "u1"}, wantSource: "disc", wantWarn: "Not sending the --admin-url credentials", wantCreds: false},
		{name: "unreachable flag keeps credentials", flag: true, disc: true, nsRequested: true, uuids: uuids{"disc": "u1"}, wantSource: "disc", wantWarn: "unreachable", wantCreds: true},
		{name: "discovered unreachable keeps flag", flag: true, disc: true, nsRequested: true, uuids: uuids{"flag": "u1"}, wantSource: "flag", wantWarn: "Unable to reach", wantCreds: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var flag, disc *adminTarget
			if tt.flag {
				flag = target(t, "flag", creds, "127.0.0.1:19644")
			}
			if tt.disc {
				disc = target(t, "disc", creds, "rp-0.rp.redpanda.svc.cluster.local:9644")
			}
			uuid := func(_ context.Context, a *adminTarget) (string, error) {
				if u, ok := tt.uuids[a.source]; ok {
					return u, nil
				}
				return "", errors.New("connection refused")
			}
			got, err := chooseAdmin(context.Background(), flag, disc, tt.discErr, tt.nsRequested, ns, uuid)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.source != tt.wantSource {
				t.Errorf("source %q, want %q", got.source, tt.wantSource)
			}
			if got.discovered != (tt.wantSource == "disc") {
				t.Errorf("discovered = %v for source %q", got.discovered, got.source)
			}
			if got.hasCredentials() != tt.wantCreds {
				t.Errorf("credentials kept = %v, want %v", got.hasCredentials(), tt.wantCreds)
			}
			warn := strings.Join(got.warnings, "\n")
			if tt.wantWarn == "" && warn != "" {
				t.Errorf("unexpected warning %q", warn)
			}
			if tt.wantWarn != "" && !strings.Contains(warn, tt.wantWarn) {
				t.Errorf("warning %q, want it to contain %q", warn, tt.wantWarn)
			}
		})
	}
}

// uuidServer answers /v1/cluster/uuid and records whether a request carried
// credentials.
func uuidServer(t *testing.T, uuid string, sawAuth *bool) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/cluster/uuid" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "" && sawAuth != nil {
			*sawAuth = true
		}
		json.NewEncoder(w).Encode(map[string]string{"cluster_uuid": uuid})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestClusterUUID_DeadFirstAddressDoesNotUseUpTheBudget(t *testing.T) {
	var sawAuth bool
	srv := uuidServer(t, "u1", &sawAuth)
	tg := target(t, "flag", admin.Config{SASLUser: "admin", SASLPassword: "secret"},
		"127.0.0.1:1", strings.TrimPrefix(srv.URL, "http://"))

	ctx, cancel := context.WithTimeout(context.Background(), flagProbeTimeout)
	defer cancel()
	start := time.Now()
	u, err := clusterUUID(ctx, tg)
	if err != nil {
		t.Fatalf("got %v, want the second address to answer", err)
	}
	if u != "u1" {
		t.Errorf("uuid %q, want u1", u)
	}
	if time.Since(start) > 2*time.Second {
		t.Errorf("took %v; the dead address held up the probe", time.Since(start))
	}
	if sawAuth {
		t.Error("the UUID probe sent credentials")
	}
}

func TestClusterUUID_EmptyIsAnError(t *testing.T) {
	srv := uuidServer(t, "", nil)
	tg := target(t, "flag", admin.Config{}, strings.TrimPrefix(srv.URL, "http://"))
	if _, err := clusterUUID(context.Background(), tg); err == nil {
		t.Fatal("an unset cluster UUID must not count as a match")
	}
}

func inClusterBroker(srv *httptest.Server) (*k8s.Client, []k8s.Broker) {
	addr := strings.TrimPrefix(strings.TrimPrefix(srv.URL, "https://"), "http://")
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "rp-0", Namespace: "redpanda"}}
	kc := &k8s.Client{Kube: fake.NewSimpleClientset(), InCluster: true}
	return kc, []k8s.Broker{{Pod: pod, Address: addr, Host: "127.0.0.1", AdminPort: 9644}}
}

func TestDiscoveredTarget_PlaintextDropsProfileTLS(t *testing.T) {
	srv := uuidServer(t, "u1", nil)
	kc, brokers := inClusterBroker(srv)
	// A profile's client cert that doesn't exist here: if it weren't
	// dropped for a plaintext listener, building the client would fail.
	base := admin.Config{TLSCert: "/nonexistent/tls.crt", TLSKey: "/nonexistent/tls.key"}
	tg, err := discoveredTarget(context.Background(), kc, "redpanda", brokers, base)
	if err != nil {
		t.Fatal(err)
	}
	defer tg.Close()
	if u, err := clusterUUID(context.Background(), tg); err != nil || u != "u1" {
		t.Fatalf("got %q, %v", u, err)
	}
}

func TestDiscoveredTarget_UsesAdminTLSCA(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"cluster_uuid": "u1"})
	}))
	defer srv.Close()
	caFile := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	kc, brokers := inClusterBroker(srv)
	tg, err := discoveredTarget(context.Background(), kc, "redpanda", brokers, admin.Config{TLSCa: caFile})
	if err != nil {
		t.Fatal(err)
	}
	defer tg.Close()
	if u, err := clusterUUID(context.Background(), tg); err != nil || u != "u1" {
		t.Fatalf("got %q, %v", u, err)
	}
}
