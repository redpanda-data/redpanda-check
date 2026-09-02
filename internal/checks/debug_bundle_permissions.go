package checks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/vuldin/redpanda-check/internal/checker"
)

// probeResult mirrors the JSON shape returned by the broker's
// /v1/debug/bundle/check_permissions endpoint (which forwards the output of
// `rpk debug bundle --dry-run --format json`).
type probeResult struct {
	Category string `json:"category"`
	Resource string `json:"resource"`
	OK       bool   `json:"ok"`
	Error    string `json:"error,omitempty"`
	Hint     string `json:"hint,omitempty"`
}

type probeResponse struct {
	Probes []probeResult `json:"probes"`
}

// DebugBundlePermissions queries the broker for a dry-run of the debug bundle
// collection process and reports any permission or access issues that would
// cause the bundle to drop data. This is opportunistic: if the broker version
// doesn't expose the endpoint yet, the check SKIPs.
func DebugBundlePermissions(ctx context.Context, pc *checker.ProductionChecker) {
	r := checker.CheckResult{
		Name:        "debug_bundle_permissions",
		Description: "Debug bundle collection permissions",
		Level:       checker.LevelRecommended,
	}

	// SendOneStream requires exactly one configured admin URL and errors out
	// otherwise. Scope to a single broker via ForBroker regardless of how
	// many admin URLs are configured, so the real result (typically a 404,
	// since brokers on the same cluster ship the same version) isn't
	// replaced by rpadmin's generic "unable to issue a single-admin-endpoint
	// request to N admin endpoints" error -- which reads like a client/
	// profile misconfiguration rather than a missing endpoint
	// (redpanda-check#13). Falls back to the shared client if no broker is
	// resolvable.
	client := pc.AdminClient
	if brokers, err := pc.BrokerList(ctx); err == nil && len(brokers) > 0 {
		if bc, err := pc.AdminClient.ForBroker(ctx, brokers[0].NodeID); err == nil {
			defer bc.Close()
			client = bc
		}
	}

	resp, err := client.SendOneStream(ctx, http.MethodPost, "/v1/debug/bundle/check_permissions", nil, false)
	if err != nil {
		// rpadmin converts every non-2xx response into an *HTTPResponseError
		// and never returns a response alongside it (see sendReqAndReceive),
		// so the status code has to come off the error, not off resp -- a
		// resp.StatusCode switch here is unreachable dead code that made
		// the 404/403/401 branches below never actually run.
		var httpErr *rpadmin.HTTPResponseError
		if errors.As(err, &httpErr) && httpErr.Response != nil {
			switch httpErr.Response.StatusCode {
			case http.StatusNotFound:
				r.Status = checker.StatusSkip
				r.Details = "Endpoint /v1/debug/bundle/check_permissions not available on this broker version"
				pc.AddResult(r)
				return
			case http.StatusForbidden, http.StatusUnauthorized:
				r.Status = checker.StatusSkip
				r.Details = "Requires superuser credentials (pass --sasl-user/--sasl-password for a superuser)"
				pc.AddResult(r)
				return
			default:
				r.Status = checker.StatusWarn
				r.Details = fmt.Sprintf("Unexpected status %d from debug bundle permission probe", httpErr.Response.StatusCode)
				pc.AddResult(r)
				return
			}
		}
		r.Status = checker.StatusSkip
		r.Details = fmt.Sprintf("Unable to query debug bundle permissions: %v", err)
		pc.AddResult(r)
		return
	}
	defer resp.Body.Close()

	// err == nil guarantees a 2xx response (see sendReqAndReceive).
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		r.Status = checker.StatusWarn
		r.Details = fmt.Sprintf("Unable to read permission probe response: %v", err)
		pc.AddResult(r)
		return
	}

	var probe probeResponse
	if err := json.Unmarshal(body, &probe); err != nil {
		r.Status = checker.StatusWarn
		r.Details = fmt.Sprintf("Unable to parse permission probe response: %v", err)
		pc.AddResult(r)
		return
	}

	var issues []string
	for _, p := range probe.Probes {
		if p.OK {
			continue
		}
		msg := fmt.Sprintf("[%s] %s: %s", p.Category, p.Resource, p.Error)
		if p.Hint != "" {
			msg += " (" + p.Hint + ")"
		}
		issues = append(issues, msg)
	}

	if len(issues) == 0 {
		r.Status = checker.StatusPass
		r.Details = fmt.Sprintf("All %d debug bundle permission probes passed", len(probe.Probes))
	} else {
		r.Status = checker.StatusWarn
		r.Details = fmt.Sprintf("%d debug bundle permission issue(s):\n%s",
			len(issues), strings.Join(issues, "\n"))
	}
	pc.AddResult(r)
}
