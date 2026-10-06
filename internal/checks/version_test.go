package checks

import (
	"fmt"
	"testing"
)

// TestLatestReleaseConstantsInSync guards against updating
// redpandaLatestMajor/redpandaLatestMinor without also prepending the new
// release to redpandaReleaseOrder (or vice versa) -- exactly the drift that
// produced redpanda-check#14, where 26.2 shipped and the table still listed
// 26.1 as latest, silently shifting every version's supported/EOL grading
// by one release.
func TestLatestReleaseConstantsInSync(t *testing.T) {
	if len(redpandaReleaseOrder) == 0 {
		t.Fatal("redpandaReleaseOrder is empty")
	}
	want := fmt.Sprintf("%d.%d", redpandaLatestMajor, redpandaLatestMinor)
	if got := redpandaReleaseOrder[0]; got != want {
		t.Errorf("redpandaReleaseOrder[0] = %q, want %q (redpandaLatestMajor/Minor is out of sync with the table)", got, want)
	}
}

func TestMinorOffsetFromLatest(t *testing.T) {
	cases := []struct {
		version string
		want    int
	}{
		{"v26.2.0", 0},
		{"v26.1.5", 1},
		{"v25.3.10", 2},
		{"v25.2.0", 3},
	}
	for _, tc := range cases {
		v, err := parseRedpandaVersion(tc.version)
		if err != nil {
			t.Fatalf("parseRedpandaVersion(%q): %v", tc.version, err)
		}
		if got := minorOffsetFromLatest(v); got != tc.want {
			t.Errorf("minorOffsetFromLatest(%s) = %d, want %d", tc.version, got, tc.want)
		}
	}
}

func TestMinorOffsetFromLatest_NewerThanKnown(t *testing.T) {
	// A release newer than anything in the table (we just haven't shipped
	// the constant bump yet) is treated as current rather than unknown.
	v, err := parseRedpandaVersion("v99.1.0")
	if err != nil {
		t.Fatalf("parseRedpandaVersion: %v", err)
	}
	if got := minorOffsetFromLatest(v); got != 0 {
		t.Errorf("expected offset 0 for a version newer than the known table, got %d", got)
	}
}
