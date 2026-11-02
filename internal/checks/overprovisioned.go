package checks

import (
	"context"
	"fmt"
	"strings"

	"github.com/vuldin/redpanda-check/internal/checker"
)

// Overprovisioned validates overprovisioned mode is disabled. overprovisioned
// is a node-level property (set in redpanda.yaml under rpk: or as a node
// property), so we check each broker's node config individually.
func Overprovisioned(ctx context.Context, pc *checker.ProductionChecker) {
	r := checker.CheckResult{
		Name:        "overprovisioned",
		Description: "Overprovisioned mode disabled",
		Level:       checker.LevelCritical,
	}

	configs, unreachable, err := pc.PerBrokerNodeConfig(ctx)
	if err != nil {
		r.Status = checker.StatusFail
		r.Details = fmt.Sprintf("Unable to get node config: %v", err)
		pc.AddResult(r)
		return
	}

	// GET /v1/node_config only returns the redpanda: section of redpanda.yaml.
	// overprovisioned lives under rpk:, which that endpoint never exposes, so
	// the key is always absent on every broker observed in the wild -- treat
	// that as unknown (SKIP) rather than reporting a false PASS on a critical
	// check (redpanda-check#11). If a broker ever does report the key
	// (e.g. a future admin API), honor its actual value as before.
	var enabled []string
	var present int
	for brokerID, nc := range configs {
		val, ok := nc["overprovisioned"]
		if !ok {
			continue
		}
		present++
		if b, isBool := val.(bool); isBool && b {
			enabled = append(enabled, brokerID)
		}
	}

	switch {
	case present == 0:
		r.Status = checker.StatusSkip
		r.Details = "Unable to determine overprovisioned state: /v1/node_config does not expose the rpk: config section on any broker checked"
	case len(enabled) > 0:
		r.Status = checker.StatusFail
		r.Details = fmt.Sprintf("Overprovisioned mode is enabled on brokers: %s", strings.Join(enabled, ", "))
	default:
		detail := "Overprovisioned mode is disabled"
		if unreachable > 0 {
			detail += fmt.Sprintf(" (%d brokers checked via single connection)", unreachable)
		}
		r.Status = checker.StatusPass
		r.Details = detail
	}
	pc.AddResult(r)
}
