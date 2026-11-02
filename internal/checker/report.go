package checker

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

// statusOrder defines the display order within each section: PASS first,
// then SKIP, WARN, FAIL last (closest to the summary).
func statusOrder(s CheckStatus) int {
	switch s {
	case StatusPass:
		return 0
	case StatusSkip:
		return 1
	case StatusWarn:
		return 2
	case StatusFail:
		return 3
	default:
		return 4
	}
}

// PrintText writes a human-readable report to w. When verbose is false, only
// non-passing checks are shown. When isK8s is true, the footer references the
// Kubernetes production readiness checklist; otherwise the bare-metal one.
func PrintText(w io.Writer, report Report, verbose bool, isK8s bool) {
	fmt.Fprintln(w, "Redpanda Production Readiness Check")
	fmt.Fprintln(w)

	// Split checks by level.
	var critical, recommended []CheckResult
	for _, c := range report.Checks {
		switch c.Level {
		case LevelRecommended:
			recommended = append(recommended, c)
		default:
			critical = append(critical, c)
		}
	}

	// Sort each group: PASS, SKIP, WARN, FAIL.
	sortByStatus := func(checks []CheckResult) {
		sort.SliceStable(checks, func(i, j int) bool {
			return statusOrder(checks[i].Status) < statusOrder(checks[j].Status)
		})
	}
	sortByStatus(critical)
	sortByStatus(recommended)

	printSection(w, "Recommended", recommended, verbose)
	printSection(w, "Critical", critical, verbose)

	fmt.Fprintf(w, "Summary: %d checks | %d passed | %d failed | %d warnings | %d skipped\n",
		report.Summary.Total,
		report.Summary.Passed,
		report.Summary.Failed,
		report.Summary.Warnings,
		report.Summary.Skipped,
	)
	fmt.Fprintf(w, "Overall: %s\n", report.OverallStatus)

	fmt.Fprintln(w)
	fmt.Fprintln(w, "Note: Some production readiness requirements (monitoring, backup/DR strategy,")
	fmt.Fprintln(w, "upgrade policy, etc.) cannot be verified programmatically.")
	fmt.Fprintln(w, "Review the full checklist for your deployment type:")
	if isK8s {
		fmt.Fprintln(w, "  https://docs.redpanda.com/current/deploy/redpanda/kubernetes/k-production-readiness/")
	} else {
		fmt.Fprintln(w, "  https://docs.redpanda.com/current/deploy/redpanda/manual/production/production-readiness/")
	}
}

func printSection(w io.Writer, title string, checks []CheckResult, verbose bool) {
	// Count visible checks to decide whether to print the header.
	visible := 0
	for _, c := range checks {
		if verbose || (c.Status != StatusPass && c.Status != StatusSkip) {
			visible++
		}
	}
	if visible == 0 && !verbose {
		return
	}

	fmt.Fprintf(w, "--- %s ---\n", title)
	for _, c := range checks {
		if !verbose && (c.Status == StatusPass || c.Status == StatusSkip) {
			continue
		}

		var tag string
		switch c.Status {
		case StatusPass:
			tag = "PASS"
		case StatusFail:
			tag = "FAIL"
		case StatusWarn:
			tag = "WARN"
		case StatusSkip:
			tag = "SKIP"
		}

		first := c.Description
		var lines []string
		if c.Details != "" {
			lines = strings.Split(c.Details, "\n")
			if lines[0] != "" {
				first = lines[0]
				lines = lines[1:]
			}
		}
		fmt.Fprintf(w, "%-4s  %s\n", tag, first)
		for _, line := range lines {
			fmt.Fprintf(w, "      %s\n", line)
		}
	}
	fmt.Fprintln(w)
}

// PrintJSON writes the report as indented JSON to w.
func PrintJSON(w io.Writer, report Report) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(report)
}
