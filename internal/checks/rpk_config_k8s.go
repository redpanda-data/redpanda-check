package checks

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/vuldin/redpanda-check/internal/checker"
	k8sutil "github.com/vuldin/redpanda-check/internal/k8s"
)

// The overprovisioned and ballast_file checks read the rpk: section of each
// broker's configuration. /v1/node_config doesn't return it, so in Kubernetes
// they fall back to the redpanda.yaml in the ConfigMap the broker pods mount.
// Each function sets r and returns true when it could read that, and returns
// false (leaving r alone) otherwise.

func k8sRPKConfigs(ctx context.Context, pc *checker.ProductionChecker) (map[string]map[string]any, bool) {
	if !pc.IsK8s() {
		return nil, false
	}
	configs, err := k8sutil.RPKConfigs(ctx, pc.K8sClient, pc.Namespace)
	if err != nil || len(configs) == 0 {
		return nil, false
	}
	return configs, true
}

func sortedPods(configs map[string]map[string]any) []string {
	pods := make([]string, 0, len(configs))
	for p := range configs {
		pods = append(pods, p)
	}
	sort.Strings(pods)
	return pods
}

func overprovisionedFromK8s(ctx context.Context, pc *checker.ProductionChecker, r *checker.CheckResult) bool {
	configs, ok := k8sRPKConfigs(ctx, pc)
	if !ok {
		return false
	}
	var enabled []string
	for _, pod := range sortedPods(configs) {
		// Unset means rpk's default, false.
		if b, _ := configs[pod]["overprovisioned"].(bool); b {
			enabled = append(enabled, pod)
		}
	}
	if len(enabled) > 0 {
		r.Status = checker.StatusFail
		r.Details = fmt.Sprintf("Overprovisioned mode is enabled (rpk.overprovisioned in redpanda.yaml) on: %s", strings.Join(enabled, ", "))
		return true
	}
	r.Status = checker.StatusPass
	r.Details = fmt.Sprintf("Overprovisioned mode is disabled on %d broker pod(s) (rpk.overprovisioned in redpanda.yaml)", len(configs))
	return true
}

func ballastFileFromK8s(ctx context.Context, pc *checker.ProductionChecker, r *checker.CheckResult) bool {
	configs, ok := k8sRPKConfigs(ctx, pc)
	if !ok {
		return false
	}
	// The tuner creates the ballast file only when tune_ballast_file is true;
	// ballast_file_path and ballast_file_size have defaults.
	var missing []string
	for _, pod := range sortedPods(configs) {
		if b, _ := configs[pod]["tune_ballast_file"].(bool); !b {
			missing = append(missing, pod)
		}
	}
	if len(missing) > 0 {
		r.Status = checker.StatusWarn
		r.Details = fmt.Sprintf("Ballast file not enabled (rpk.tune_ballast_file is false or unset in redpanda.yaml) on: %s", strings.Join(missing, ", "))
		return true
	}
	r.Status = checker.StatusPass
	r.Details = fmt.Sprintf("Ballast file enabled on %d broker pod(s) (rpk.tune_ballast_file in redpanda.yaml)", len(configs))
	return true
}
