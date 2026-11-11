package main

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/vuldin/redpanda-check/internal/admin"
	"github.com/vuldin/redpanda-check/internal/k8s"
)

const (
	// discoveredProbeTimeout covers port-forward setup plus a TLS handshake.
	discoveredProbeTimeout = 15 * time.Second
	// flagProbeTimeout is short: under rpk check, --admin-url often points
	// at a profile address with nothing listening.
	flagProbeTimeout = 3 * time.Second
)

// adminTarget is one way to reach an admin API.
type adminTarget struct {
	cfg        admin.Config
	client     *rpadmin.AdminAPI
	addresses  []string
	source     string
	discovered bool
	// closer releases resources beyond the client, e.g. port-forwards.
	closer func()
}

func newTarget(cfg admin.Config, source string, discovered bool, closer func()) (*adminTarget, error) {
	client, err := admin.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return &adminTarget{cfg: cfg, client: client, addresses: cfg.Addresses, source: source, discovered: discovered, closer: closer}, nil
}

// Close releases the client and anything else the target holds.
func (t *adminTarget) Close() {
	t.client.Close()
	if t.closer != nil {
		t.closer()
	}
}

func (t *adminTarget) hasCredentials() bool {
	return t.cfg.SASLUser != "" || t.cfg.TLSCert != ""
}

// withoutCredentials returns the same target with no SASL user and no client
// certificate. It takes over t's resources.
func (t *adminTarget) withoutCredentials() (*adminTarget, error) {
	cfg := t.cfg
	cfg.SASLUser, cfg.SASLPassword, cfg.TLSCert, cfg.TLSKey = "", "", "", ""
	client, err := admin.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	t.client.Close()
	n := *t
	n.cfg, n.client = cfg, client
	return &n, nil
}

// adminChoice is the admin API the checks will use, plus anything the user
// should be told about how it was chosen.
type adminChoice struct {
	*adminTarget
	warnings []string
}

// uuidFunc reads a cluster UUID through t. It's a variable so tests can
// replace it.
type uuidFunc func(ctx context.Context, t *adminTarget) (string, error)

// chooseAdmin decides which admin API to check, and closes the target it
// doesn't use.
//
// flag is the --admin-url target, which rpk check always fills in from the
// active profile, so it may belong to a different cluster. disc is the
// target discovered in the namespace (discErr says why there isn't one).
// When both exist, flag is kept only if it reports the same cluster UUID as
// disc, so a profile that already points at this cluster keeps its
// listener and TLS settings. If flag answers for a different cluster, its
// credentials belong to that cluster and aren't sent to disc.
func chooseAdmin(ctx context.Context, flag, disc *adminTarget, discErr error, nsRequested bool, ns string, uuid uuidFunc) (adminChoice, error) {
	switch {
	case flag == nil && disc == nil:
		if discErr != nil {
			return adminChoice{}, fmt.Errorf("no --admin-url provided and Kubernetes discovery failed (%v); specify --admin-url", discErr)
		}
		return adminChoice{}, fmt.Errorf("no --admin-url provided and Kubernetes discovery failed; specify --admin-url")

	case disc == nil:
		c := adminChoice{adminTarget: flag}
		if nsRequested && discErr != nil {
			c.warnings = append(c.warnings, fmt.Sprintf(
				"--namespace %s was given, but the admin API there couldn't be used (%v). The admin API checks use --admin-url %s, which may be a different cluster.",
				ns, discErr, joinAddrs(flag.addresses)))
		}
		return c, nil

	case flag == nil:
		return adminChoice{adminTarget: disc}, nil
	}

	dctx, cancel := context.WithTimeout(ctx, discoveredProbeTimeout)
	discUUID, err := uuid(dctx, disc)
	cancel()
	if err != nil {
		disc.Close()
		return adminChoice{
			adminTarget: flag,
			warnings: []string{fmt.Sprintf(
				"Unable to reach the admin API of the brokers in namespace %s (%v). The admin API checks use --admin-url %s, which may be a different cluster.",
				ns, err, joinAddrs(flag.addresses))},
		}, nil
	}

	fctx, cancel := context.WithTimeout(ctx, flagProbeTimeout)
	flagUUID, flagErr := uuid(fctx, flag)
	cancel()
	if flagErr == nil && flagUUID == discUUID {
		disc.Close()
		return adminChoice{adminTarget: flag}, nil
	}
	flag.Close()

	reason := "a different cluster"
	if flagErr != nil {
		reason = fmt.Sprintf("unreachable: %v", flagErr)
	}
	warnings := []string{fmt.Sprintf(
		"--admin-url %s isn't the cluster in namespace %s (%s); using the %d brokers found in %s.",
		joinAddrs(flag.addresses), ns, reason, len(disc.addresses), ns)}

	if flagErr == nil && disc.hasCredentials() {
		stripped, err := disc.withoutCredentials()
		if err != nil {
			disc.Close()
			return adminChoice{}, fmt.Errorf("unable to create admin API client: %v", err)
		}
		disc = stripped
		warnings = append(warnings, fmt.Sprintf(
			"Not sending the --admin-url credentials (SASL user or client certificate) to the brokers in %s, because they belong to the other cluster. Checks that need admin API auth may fail; to pass credentials for this cluster, use --ignore-profile with --sasl-user and REDPANDA_SASL_PASSWORD, or pass --admin-url for this cluster.",
			ns))
	}
	if flagErr == nil && disc.cfg.TLSSkipVerify {
		warnings = append(warnings, fmt.Sprintf(
			"TLS certificate verification is still disabled for the brokers in %s, because --admin-tls-skip-verify was given, possibly by the other cluster's profile.",
			ns))
	}
	return adminChoice{adminTarget: disc, warnings: warnings}, nil
}

// clusterUUID asks every address at once for the cluster UUID, without
// SASL credentials (the endpoint is public), and returns the first answer.
// One dead or slow address can't use up the time budget for the others.
func clusterUUID(ctx context.Context, t *adminTarget) (string, error) {
	type result struct {
		uuid string
		err  error
	}
	ch := make(chan result, len(t.addresses))
	for _, addr := range t.addresses {
		go func(addr string) {
			cfg := t.cfg
			cfg.Addresses = []string{addr}
			cfg.SASLUser, cfg.SASLPassword = "", ""
			c, err := admin.NewClient(cfg)
			if err != nil {
				ch <- result{err: err}
				return
			}
			defer c.Close()
			u, err := c.ClusterUUID(ctx)
			if err == nil && u.UUID == "" {
				err = fmt.Errorf("%s has no cluster UUID yet", addr)
			}
			ch <- result{u.UUID, err}
		}(addr)
	}
	var lastErr error
	for range t.addresses {
		r := <-ch
		if r.err == nil {
			return r.uuid, nil
		}
		lastErr = r.err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no admin addresses")
	}
	return "", lastErr
}

// discoveredTarget builds a client for the brokers found in the namespace.
// Outside the cluster it reaches them through port-forwards. TLS is detected
// from the broker: a CA from --admin-tls-ca is tried first, then the CAs in
// the Secrets the broker pod mounts.
func discoveredTarget(ctx context.Context, kc *k8s.Client, ns string, brokers []k8s.Broker, base admin.Config) (*adminTarget, error) {
	addrs := make([]string, len(brokers))
	for i, b := range brokers {
		addrs[i] = b.Address
	}
	source := fmt.Sprintf("discovered in namespace %s", ns)

	cfg := base
	cfg.Addresses = addrs
	cfg.TLSCa = ""
	var dial k8s.DialFunc
	var closer func()
	if kc.InCluster {
		dial = (&net.Dialer{Timeout: 10 * time.Second}).DialContext
	} else {
		pd := k8s.NewPodDialer(kc, ns, brokers)
		dial, closer = pd.DialContext, pd.Close
		cfg.Dialer = dial
		source += " via port-forward"
	}

	var cands []k8s.CACandidate
	if base.TLSCa != "" {
		if pool, err := admin.LoadCA(base.TLSCa); err == nil {
			cands = append(cands, k8s.CACandidate{Source: "--admin-tls-ca", Pool: pool})
		}
	}

	pctx, cancel := context.WithTimeout(ctx, discoveredProbeTimeout)
	defer cancel()
	var res k8s.TLSResult
	var err error
	for _, b := range brokers {
		res, err = k8s.DetectAdminTLS(pctx, kc.Kube, b, dial, cands, base.TLSSkipVerify)
		if err == nil || pctx.Err() != nil {
			break
		}
	}
	if err != nil {
		if closer != nil {
			closer()
		}
		return nil, err
	}
	if res.UsesTLS {
		cfg.RootCAs = res.RootCAs
	} else {
		cfg.TLSCert, cfg.TLSKey, cfg.TLSSkipVerify, cfg.RootCAs = "", "", false, nil
	}

	t, err := newTarget(cfg, source, true, closer)
	if err != nil && closer != nil {
		closer()
	}
	return t, err
}

func joinAddrs(a []string) string { return strings.Join(a, ",") }
