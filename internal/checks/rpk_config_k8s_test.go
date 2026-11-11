package checks_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/redpanda-data/common-go/rpadmin"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/vuldin/redpanda-check/internal/checker"
	"github.com/vuldin/redpanda-check/internal/checks"
)

// k8sChecker is newTestChecker with /v1/node_config lacking the rpk: section
// (as real brokers return it) and a fake Kubernetes namespace holding one
// pool's ConfigMap with the given redpanda.yaml, mounted by two broker pods.
func k8sChecker(t *testing.T, redpandaYAML string) *checker.ProductionChecker {
	t.Helper()
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/brokers":     jsonHandler(t, []rpadmin.Broker{{NodeID: 0}, {NodeID: 1}}),
		"/v1/node_config": jsonHandler(t, map[string]any{"node_id": 0}),
	})
	labels := map[string]string{"app.kubernetes.io/name": "redpanda"}
	objs := []runtime.Object{
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "rp", Namespace: "redpanda"},
			Spec:       corev1.ServiceSpec{ClusterIP: corev1.ClusterIPNone, Selector: labels},
		},
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "rp-central", Namespace: "redpanda"},
			Data:       map[string]string{"redpanda.yaml": redpandaYAML},
		},
	}
	for _, name := range []string{"rp-central-0", "rp-central-1"} {
		objs = append(objs, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "redpanda", Labels: labels},
			Spec: corev1.PodSpec{Volumes: []corev1.Volume{
				{Name: "lifecycle-scripts", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "rp-sts-lifecycle"}}},
				{Name: "base-config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "rp-central"},
				}}},
			}},
		})
	}
	pc.K8sClient = fake.NewSimpleClientset(objs...)
	return pc
}

func TestOverprovisioned_K8s_Pass(t *testing.T) {
	pc := k8sChecker(t, "redpanda:\n  data_directory: /var/lib/redpanda/data\nrpk:\n  overprovisioned: false\n")
	checks.Overprovisioned(context.Background(), pc)
	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestOverprovisioned_K8s_Fail(t *testing.T) {
	pc := k8sChecker(t, "rpk:\n  overprovisioned: true\n")
	checks.Overprovisioned(context.Background(), pc)
	if pc.Results[0].Status != checker.StatusFail {
		t.Errorf("expected FAIL, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestOverprovisioned_K8s_NoConfigMap_Skip(t *testing.T) {
	pc := k8sChecker(t, "")
	pc.K8sClient = fake.NewSimpleClientset()
	checks.Overprovisioned(context.Background(), pc)
	if pc.Results[0].Status != checker.StatusSkip {
		t.Errorf("expected SKIP, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestBallastFile_K8s_Pass(t *testing.T) {
	pc := k8sChecker(t, "rpk:\n  tune_ballast_file: true\n  ballast_file_size: 1GiB\n")
	checks.BallastFile(context.Background(), pc)
	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestBallastFile_K8s_Warn_Unset(t *testing.T) {
	pc := k8sChecker(t, "rpk:\n  overprovisioned: false\n")
	checks.BallastFile(context.Background(), pc)
	if pc.Results[0].Status != checker.StatusWarn {
		t.Errorf("expected WARN, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}
