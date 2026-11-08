package admin

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"

	"github.com/redpanda-data/common-go/rpadmin"
)

// Config holds the parameters needed to construct an admin API client.
type Config struct {
	Addresses     []string
	TLSCa        string
	TLSCert      string
	TLSKey       string
	TLSSkipVerify bool
	SASLUser      string
	SASLPassword  string
	// RootCAs, when set, enables TLS and is used instead of TLSCa.
	RootCAs *x509.CertPool
	// Dialer, when set, opens every connection (e.g. a Kubernetes
	// port-forward).
	Dialer func(ctx context.Context, network, addr string) (net.Conn, error)
}

// NewClient builds an rpadmin.AdminAPI from the given Config.
func NewClient(cfg Config) (*rpadmin.AdminAPI, error) {
	if len(cfg.Addresses) == 0 {
		return nil, fmt.Errorf("at least one admin API address is required")
	}

	var tc *tls.Config
	if cfg.TLSCa != "" || cfg.TLSCert != "" || cfg.TLSSkipVerify || cfg.RootCAs != nil {
		var err error
		tc, err = buildTLSConfig(cfg.TLSCa, cfg.TLSCert, cfg.TLSKey)
		if err != nil {
			return nil, fmt.Errorf("unable to build TLS config: %v", err)
		}
		if cfg.RootCAs != nil {
			tc.RootCAs = cfg.RootCAs
		}
		if cfg.TLSSkipVerify {
			tc.InsecureSkipVerify = true
		}
	}

	var auth rpadmin.Auth
	if cfg.SASLUser != "" {
		auth = &rpadmin.BasicAuth{Username: cfg.SASLUser, Password: cfg.SASLPassword}
	} else {
		auth = &rpadmin.NopAuth{}
	}

	if cfg.Dialer != nil {
		return rpadmin.NewAdminAPIWithDialer(cfg.Addresses, auth, tc, cfg.Dialer)
	}
	return rpadmin.NewAdminAPI(cfg.Addresses, auth, tc)
}

// LoadCA reads a PEM CA bundle from path.
func LoadCA(path string) (*x509.CertPool, error) {
	caPEM, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("unable to read CA file %q: %v", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("unable to parse CA certificate from %q", path)
	}
	return pool, nil
}

func buildTLSConfig(caPath, certPath, keyPath string) (*tls.Config, error) {
	tc := &tls.Config{MinVersion: tls.VersionTLS12}

	if caPath != "" {
		pool, err := LoadCA(caPath)
		if err != nil {
			return nil, err
		}
		tc.RootCAs = pool
	}

	if certPath != "" && keyPath != "" {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, fmt.Errorf("unable to load client certificate: %v", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}

	return tc, nil
}
