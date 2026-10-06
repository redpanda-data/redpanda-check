package checks

import (
	"context"
	"fmt"
	"strings"

	"github.com/vuldin/redpanda-check/internal/checker"
)

// BallastFile validates that a ballast file is configured on all brokers.
// A ballast file provides emergency disk space recovery by being deletable
// when the disk is full.
func BallastFile(ctx context.Context, pc *checker.ProductionChecker) {
	r := checker.CheckResult{
		Name:        "ballast_file",
		Description: "Ballast file configured",
		Level:       checker.LevelRecommended,
	}

	brokers, err := pc.BrokerList(ctx)
	if err != nil {
		r.Status = checker.StatusFail
		r.Details = fmt.Sprintf("Unable to list brokers: %v", err)
		pc.AddResult(r)
		return
	}

	configs, unreachable, err := pc.PerBrokerNodeConfig(ctx)
	if err != nil {
		r.Status = checker.StatusFail
		r.Details = fmt.Sprintf("Unable to get node config: %v", err)
		pc.AddResult(r)
		return
	}

	// GET /v1/node_config only returns the redpanda: section of
	// redpanda.yaml. ballast_file_path/ballast_file_size live under rpk:,
	// which that endpoint never exposes, so both keys are always absent in
	// practice and this always reported a false WARN, even on brokers with
	// a real, correctly configured ballast file (redpanda-check#12).
	// Distinguish "keys absent" (can't determine -> SKIP that broker) from
	// "keys present but empty" (genuinely not configured -> WARN).
	var missing []string
	var checked int
	for nodeID, nc := range configs {
		_, pathOK := nc["ballast_file_path"]
		_, sizeOK := nc["ballast_file_size"]
		if !pathOK && !sizeOK {
			continue
		}
		checked++
		path, _ := nc["ballast_file_path"].(string)
		size, _ := nc["ballast_file_size"].(string)
		if path == "" && size == "" {
			missing = append(missing, nodeID)
		}
	}

	switch {
	case checked == 0:
		r.Status = checker.StatusSkip
		r.Details = "Unable to determine ballast file configuration: /v1/node_config does not expose the rpk: config section on any broker checked"
	case len(missing) > 0:
		r.Status = checker.StatusWarn
		r.Details = fmt.Sprintf("Ballast file not configured on brokers: %s", strings.Join(missing, ", "))
	default:
		detail := fmt.Sprintf("Ballast file configured on %d/%d brokers checked", checked, len(brokers))
		if unreachable > 0 {
			detail += fmt.Sprintf(" (%d brokers checked via single connection)", unreachable)
		}
		r.Status = checker.StatusPass
		r.Details = detail
	}
	pc.AddResult(r)
}
