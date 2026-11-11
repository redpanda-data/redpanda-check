package k8s

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// CACandidate is a CA bundle to try against a broker's certificate.
type CACandidate struct {
	Source string
	Pool   *x509.CertPool
}

// TLSResult describes a broker's admin listener.
type TLSResult struct {
	UsesTLS bool
	// RootCAs verifies the broker's certificate. It's nil for a plaintext
	// listener, and when skipVerify was requested.
	RootCAs *x509.CertPool
}

// DetectAdminTLS reports whether the broker's admin listener uses TLS and,
// if so, finds a CA that verifies the broker's certificate for its FQDN:
// first from candidates, in order, then from the Secrets the broker pod
// mounts. With skipVerify, no CA is looked for.
func DetectAdminTLS(ctx context.Context, kube kubernetes.Interface, b Broker, dial DialFunc, candidates []CACandidate, skipVerify bool) (TLSResult, error) {
	conn, err := dial(ctx, "tcp", b.Address)
	if err != nil {
		return TLSResult{}, err
	}
	defer conn.Close()

	tc := tls.Client(conn, &tls.Config{
		ServerName: b.Host,
		MinVersion: tls.VersionTLS12,
		// Only used to read the certificate chain, which is verified
		// against each candidate CA below.
		InsecureSkipVerify: true, //nolint:gosec // verified manually below
	})
	if err := tc.HandshakeContext(ctx); err != nil {
		var rh tls.RecordHeaderError
		if errors.As(err, &rh) {
			return TLSResult{}, nil
		}
		return TLSResult{}, fmt.Errorf("TLS handshake with pod %s failed: %v", b.Pod.Name, err)
	}
	if skipVerify {
		return TLSResult{UsesTLS: true}, nil
	}
	chain := tc.ConnectionState().PeerCertificates
	if len(chain) == 0 {
		return TLSResult{}, fmt.Errorf("pod %s sent no TLS certificate", b.Pod.Name)
	}

	for _, c := range candidates {
		if verifies(chain, c.Pool, b.Host) {
			return TLSResult{UsesTLS: true, RootCAs: c.Pool}, nil
		}
	}
	// Only read Secrets when no CA the caller gave us works.
	mounted, secretErr := mountedCAs(ctx, kube, b.Pod)
	for _, c := range mounted {
		if verifies(chain, c.Pool, b.Host) {
			return TLSResult{UsesTLS: true, RootCAs: c.Pool}, nil
		}
	}
	if secretErr != nil {
		return TLSResult{}, fmt.Errorf("the admin API on pod %s uses TLS, and its CA couldn't be read from the pod's TLS Secrets (%v); pass --admin-tls-ca", b.Pod.Name, secretErr)
	}
	return TLSResult{}, fmt.Errorf("the admin API on pod %s uses TLS, and no CA mounted in the pod verifies its certificate for %s; pass --admin-tls-ca", b.Pod.Name, b.Host)
}

func verifies(chain []*x509.Certificate, roots *x509.CertPool, host string) bool {
	inter := x509.NewCertPool()
	for _, c := range chain[1:] {
		inter.AddCert(c)
	}
	_, err := chain[0].Verify(x509.VerifyOptions{
		DNSName:       host,
		Roots:         roots,
		Intermediates: inter,
	})
	return err == nil
}

// tlsMountPrefix is where the Helm chart and the operator (including
// StretchCluster pools) mount the broker certificates.
const tlsMountPrefix = "/etc/tls/"

// mountedCAs returns the ca.crt of each Secret the pod mounts under
// /etc/tls/, directly or through a projected volume. Other mounted Secrets,
// such as the superuser credentials, are never read. The error is the first
// Secret read failure, so callers can explain a missing 'get secrets'
// permission.
func mountedCAs(ctx context.Context, kube kubernetes.Interface, pod corev1.Pod) ([]CACandidate, error) {
	tlsVolumes := map[string]bool{}
	for _, c := range pod.Spec.Containers {
		for _, m := range c.VolumeMounts {
			if strings.HasPrefix(m.MountPath, tlsMountPrefix) {
				tlsVolumes[m.Name] = true
			}
		}
	}

	var names []string
	seen := map[string]bool{}
	add := func(n string) {
		if n != "" && !seen[n] {
			seen[n] = true
			names = append(names, n)
		}
	}
	for _, v := range pod.Spec.Volumes {
		if !tlsVolumes[v.Name] {
			continue
		}
		if v.Secret != nil {
			add(v.Secret.SecretName)
		}
		if v.Projected != nil {
			for _, s := range v.Projected.Sources {
				if s.Secret != nil {
					add(s.Secret.Name)
				}
			}
		}
	}

	var out []CACandidate
	var firstErr error
	for _, n := range names {
		s, err := kube.CoreV1().Secrets(pod.Namespace).Get(ctx, n, metav1.GetOptions{})
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		pem, ok := s.Data["ca.crt"]
		if !ok {
			continue
		}
		pool := x509.NewCertPool()
		if pool.AppendCertsFromPEM(pem) {
			out = append(out, CACandidate{Source: "secret " + n, Pool: pool})
		}
	}
	return out, firstErr
}
