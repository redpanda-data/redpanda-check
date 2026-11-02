package checks

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Latest known Redpanda release. Update this constant when a new major.minor
// ships. Versions >= this one are treated as current (PASS). Versions at N-1
// are also PASS. N-2 triggers the Recommended "approaching EOL" warning.
// Anything older than N-2 is treated as unsupported (Critical FAIL).
//
// version_test.go asserts redpandaReleaseOrder[0] matches this constant, so
// bumping one without the other fails the build (redpanda-check#14).
const redpandaLatestMajor = 26
const redpandaLatestMinor = 2

// Ordered sequence of Redpanda major.minor releases used to derive
// "how many minor versions back from latest" for a given broker version.
// Most recent first. When a new release ships, prepend it here and bump
// redpandaLatestMajor/redpandaLatestMinor.
var redpandaReleaseOrder = []string{
	"26.2",
	"26.1",
	"25.3",
	"25.2",
	"25.1",
	"24.3",
	"24.2",
	"24.1",
	"23.3",
	"23.2",
	"23.1",
	"22.3",
}

// redpandaVersion captures a parsed broker-reported Redpanda version.
type redpandaVersion struct {
	major int
	minor int
	patch int
}

func (v redpandaVersion) MajorMinor() string {
	return fmt.Sprintf("%d.%d", v.major, v.minor)
}

func (v redpandaVersion) String() string {
	return fmt.Sprintf("v%d.%d.%d", v.major, v.minor, v.patch)
}

// parseRedpandaVersion accepts strings like "v26.1.1 - <sha>" or "v26.1.1" and
// returns the parsed version. Returns an error for anything that doesn't
// match `v<major>.<minor>.<patch>` exactly (rejects `v0.0.0-dev` and similar
// non-production builds).
var redpandaVersionRegex = regexp.MustCompile(`^v(\d+)\.(\d+)\.(\d+)$`)

func parseRedpandaVersion(s string) (redpandaVersion, error) {
	// Broker reports e.g. "v26.1.1 - 35a825c9c1880ebeedf4c18bb8c6cceaa63566c1".
	// Take the part before the first space.
	trimmed := strings.TrimSpace(s)
	if idx := strings.Index(trimmed, " "); idx >= 0 {
		trimmed = trimmed[:idx]
	}

	m := redpandaVersionRegex.FindStringSubmatch(trimmed)
	if m == nil {
		return redpandaVersion{}, fmt.Errorf(
			"%q is not a valid Redpanda release version (expected vMAJOR.MINOR.PATCH)", trimmed)
	}

	major, _ := strconv.Atoi(m[1])
	minor, _ := strconv.Atoi(m[2])
	patch, _ := strconv.Atoi(m[3])

	if major == 0 {
		return redpandaVersion{}, fmt.Errorf(
			"%q appears to be a development build (major version 0)", trimmed)
	}
	return redpandaVersion{major: major, minor: minor, patch: patch}, nil
}

// minorOffsetFromLatest returns how many minor releases back v is from the
// latest release. 0 means latest, 1 means N-1, 2 means N-2, etc. Returns
// a negative number if v is newer than latest (unknown future release).
// Returns -1 if v's major.minor is not found in the known release list.
func minorOffsetFromLatest(v redpandaVersion) int {
	// If version is newer than latest (future release we don't know yet),
	// treat as "current" (offset 0).
	if v.major > redpandaLatestMajor ||
		(v.major == redpandaLatestMajor && v.minor > redpandaLatestMinor) {
		return 0
	}

	key := v.MajorMinor()
	for i, release := range redpandaReleaseOrder {
		if release == key {
			return i
		}
	}
	// Major.minor older than the oldest entry in redpandaReleaseOrder.
	return len(redpandaReleaseOrder)
}
