package checker

import "time"

// CheckStatus represents the outcome of an individual check.
type CheckStatus string

const (
	StatusPass CheckStatus = "PASS"
	StatusFail CheckStatus = "FAIL"
	StatusWarn CheckStatus = "WARN"
	StatusSkip CheckStatus = "SKIP"
)

// CheckLevel represents the severity level of a check.
type CheckLevel string

const (
	LevelCritical    CheckLevel = "critical"
	LevelRecommended CheckLevel = "recommended"
)

// CheckResult represents the result of a single production readiness check.
type CheckResult struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Status      CheckStatus `json:"status"`
	Details     string      `json:"details,omitempty"`
	Level       CheckLevel  `json:"level"`
}

// Report represents the overall report of all checks.
type Report struct {
	Timestamp     string       `json:"timestamp"`
	OverallStatus CheckStatus  `json:"overall_status"`
	Summary       Summary      `json:"summary"`
	Checks        []CheckResult `json:"checks"`
	AdminAPI      *AdminAPIInfo `json:"admin_api,omitempty"`
}

// AdminAPIInfo records which admin API addresses the checks used and where
// they came from, so a report always says which cluster it describes.
type AdminAPIInfo struct {
	Addresses []string `json:"addresses"`
	Source    string   `json:"source"`
}

// Summary provides a count of check results by status.
type Summary struct {
	Total    int `json:"total"`
	Passed   int `json:"passed"`
	Failed   int `json:"failed"`
	Warnings int `json:"warnings"`
	Skipped  int `json:"skipped"`
}

// NewReport builds a Report from a slice of CheckResults.
func NewReport(results []CheckResult) Report {
	var s Summary
	s.Total = len(results)
	for _, r := range results {
		switch r.Status {
		case StatusPass:
			s.Passed++
		case StatusFail:
			s.Failed++
		case StatusWarn:
			s.Warnings++
		case StatusSkip:
			s.Skipped++
		}
	}

	overall := StatusPass
	if s.Failed > 0 {
		overall = StatusFail
	} else if s.Warnings > 0 {
		overall = StatusWarn
	}

	return Report{
		Timestamp:     time.Now().Format(time.RFC3339),
		OverallStatus: overall,
		Summary:       s,
		Checks:        results,
	}
}
