package checker_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/vuldin/redpanda-check/internal/checker"
)

func TestNewReport(t *testing.T) {
	results := []checker.CheckResult{
		{Name: "a", Status: checker.StatusPass, Level: checker.LevelCritical},
		{Name: "b", Status: checker.StatusFail, Level: checker.LevelCritical},
		{Name: "c", Status: checker.StatusWarn, Level: checker.LevelRecommended},
		{Name: "d", Status: checker.StatusSkip, Level: checker.LevelCritical},
	}
	report := checker.NewReport(results)

	if report.OverallStatus != checker.StatusFail {
		t.Errorf("expected overall FAIL, got %s", report.OverallStatus)
	}
	if report.Summary.Total != 4 {
		t.Errorf("expected total 4, got %d", report.Summary.Total)
	}
	if report.Summary.Passed != 1 {
		t.Errorf("expected 1 passed, got %d", report.Summary.Passed)
	}
	if report.Summary.Failed != 1 {
		t.Errorf("expected 1 failed, got %d", report.Summary.Failed)
	}
}

func TestNewReport_AllPass(t *testing.T) {
	results := []checker.CheckResult{
		{Name: "a", Status: checker.StatusPass},
		{Name: "b", Status: checker.StatusPass},
	}
	report := checker.NewReport(results)

	if report.OverallStatus != checker.StatusPass {
		t.Errorf("expected overall PASS, got %s", report.OverallStatus)
	}
}

func TestPrintText_Default(t *testing.T) {
	results := []checker.CheckResult{
		{Name: "pass_check", Description: "Passes", Status: checker.StatusPass},
		{Name: "fail_check", Description: "Fails", Status: checker.StatusFail, Details: "something broke"},
	}
	report := checker.NewReport(results)

	var buf bytes.Buffer
	checker.PrintText(&buf, report, false, false)
	out := buf.String()

	// Non-verbose: should not show passing check.
	if strings.Contains(out, "Passes") {
		t.Error("non-verbose output should not contain passing check description")
	}
	// Details is non-empty, so the static Description should be dropped in
	// favor of the actual finding, not printed alongside it.
	if strings.Contains(out, "Fails") {
		t.Error("output should not contain the static description when Details is set")
	}
	if !strings.Contains(out, "something broke") {
		t.Error("output should contain failure details")
	}
	if !strings.Contains(out, "Overall: FAIL") {
		t.Error("output should contain overall status")
	}
}

func TestPrintText_Verbose(t *testing.T) {
	results := []checker.CheckResult{
		{Name: "pass_check", Description: "Passes", Status: checker.StatusPass},
	}
	report := checker.NewReport(results)

	var buf bytes.Buffer
	checker.PrintText(&buf, report, true, false)
	out := buf.String()

	if !strings.Contains(out, "Passes") {
		t.Error("verbose output should contain passing check")
	}
	// Empty Details must fall back to Description with no spurious blank line.
	if strings.Contains(out, "Passes\n      \n") {
		t.Error("empty Details should not produce a spurious blank indented line")
	}
}

func TestPrintText_DescriptionNotBackwards(t *testing.T) {
	// Regression test for the exact scenario in the issue: a check named as
	// the passing assertion ("Ballast file configured") must not have that
	// positive-sounding line printed ahead of a WARN/FAIL that contradicts it.
	results := []checker.CheckResult{
		{
			Name:        "ballast_file",
			Description: "Ballast file configured",
			Status:      checker.StatusWarn,
			Details:     "Ballast file not configured on brokers: 0",
		},
	}
	report := checker.NewReport(results)

	var buf bytes.Buffer
	checker.PrintText(&buf, report, false, false)
	out := buf.String()

	if strings.Contains(out, "Ballast file configured") {
		t.Error("output should not print the static, positive-sounding Description")
	}
	if !strings.Contains(out, "WARN  Ballast file not configured on brokers: 0") {
		t.Error("output should lead with the actual finding on the tag line")
	}
}

func TestPrintText_MultilineDetails(t *testing.T) {
	// Only the first line of Details replaces Description; remaining lines
	// stay indented as before.
	results := []checker.CheckResult{
		{
			Name:        "multi",
			Description: "Static description",
			Status:      checker.StatusFail,
			Details:     "first finding\nsecond finding",
		},
	}
	report := checker.NewReport(results)

	var buf bytes.Buffer
	checker.PrintText(&buf, report, false, false)
	out := buf.String()

	if !strings.Contains(out, "FAIL  first finding\n      second finding\n") {
		t.Errorf("expected first Details line on the tag line and remaining lines indented, got:\n%s", out)
	}
}

func TestPrintJSON(t *testing.T) {
	results := []checker.CheckResult{
		{Name: "a", Status: checker.StatusPass, Level: checker.LevelCritical},
	}
	report := checker.NewReport(results)

	var buf bytes.Buffer
	if err := checker.PrintJSON(&buf, report); err != nil {
		t.Fatalf("PrintJSON error: %v", err)
	}

	var decoded checker.Report
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatalf("unable to unmarshal JSON output: %v", err)
	}
	if decoded.OverallStatus != checker.StatusPass {
		t.Errorf("expected PASS in JSON, got %s", decoded.OverallStatus)
	}
	if len(decoded.Checks) != 1 {
		t.Errorf("expected 1 check in JSON, got %d", len(decoded.Checks))
	}
}
