package k8s

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/yaml"
)

// RPKConfigs returns the rpk: section of each Redpanda pod's redpanda.yaml,
// keyed by pod name.
//
// The Admin API's /v1/node_config doesn't include the rpk: section, so the
// checks that need it (overprovisioned, ballast_file) can't read it from the
// broker. In Kubernetes, the Helm chart and the operator render redpanda.yaml
// into a ConfigMap that each broker pod mounts, so it's read from there.
//
// A pod is left out when none of its ConfigMap volumes has a redpanda.yaml
// that parses. A redpanda.yaml without an rpk: section gives an empty map:
// rpk's defaults apply.
func RPKConfigs(ctx context.Context, client kubernetes.Interface, namespace string) (map[string]map[string]any, error) {
	pods, err := RedpandaPods(ctx, client, namespace)
	if err != nil {
		return nil, err
	}

	// ConfigMap name -> its rpk: section, or nil when it has no usable
	// redpanda.yaml. Brokers in one pool share a ConfigMap.
	byConfigMap := make(map[string]map[string]any)
	lookup := func(name string) map[string]any {
		if rpk, ok := byConfigMap[name]; ok {
			return rpk
		}
		byConfigMap[name] = nil
		cm, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return nil
		}
		raw, ok := cm.Data["redpanda.yaml"]
		if !ok {
			return nil
		}
		var doc map[string]any
		if err := yaml.Unmarshal([]byte(raw), &doc); err != nil {
			return nil
		}
		rpk, _ := doc["rpk"].(map[string]any)
		if rpk == nil {
			rpk = map[string]any{}
		}
		byConfigMap[name] = rpk
		return rpk
	}

	configs := make(map[string]map[string]any)
	for _, pod := range pods {
		for _, v := range pod.Spec.Volumes {
			if v.ConfigMap == nil {
				continue
			}
			if rpk := lookup(v.ConfigMap.Name); rpk != nil {
				configs[pod.Name] = rpk
				break
			}
		}
	}
	if len(configs) == 0 {
		return nil, fmt.Errorf("no redpanda.yaml found in the ConfigMaps mounted by the Redpanda pods in namespace %s", namespace)
	}
	return configs, nil
}
