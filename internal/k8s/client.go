package k8s

import (
	"context"
	"fmt"
	"net"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Client holds a Kubernetes clientset and the REST config it was built from.
// InCluster is true when the config came from the pod's service account,
// which means pod DNS names resolve and no port-forward is needed.
type Client struct {
	Kube      kubernetes.Interface
	Config    *rest.Config
	InCluster bool
}

// NewClient creates a Kubernetes client. It first attempts in-cluster
// config, then falls back to the provided kubeconfig path (or the default
// loader if kubeconfigPath is empty).
func NewClient(kubeconfigPath string) (*Client, error) {
	inCluster := true
	cfg, err := rest.InClusterConfig()
	if err != nil {
		inCluster = false
		loadRules := clientcmd.NewDefaultClientConfigLoadingRules()
		if kubeconfigPath != "" {
			loadRules.ExplicitPath = kubeconfigPath
		}
		cfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			loadRules,
			&clientcmd.ConfigOverrides{},
		).ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("unable to load kubeconfig: %v", err)
		}
	}
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &Client{Kube: kube, Config: cfg, InCluster: inCluster}, nil
}

// RedpandaPods returns the Redpanda pods in the given namespace by finding the
// headless service and listing pods that match its selector.
func RedpandaPods(ctx context.Context, client kubernetes.Interface, namespace string) ([]corev1.Pod, error) {
	svc, err := headlessService(ctx, client, namespace)
	if err != nil {
		return nil, err
	}
	podList, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(svc.Spec.Selector).String(),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to list pods: %v", err)
	}
	return podList.Items, nil
}

// Broker is a Redpanda pod with an admin API port.
type Broker struct {
	Pod corev1.Pod
	// Address is the pod's in-cluster admin address, "<fqdn>:<port>".
	Address string
	// Host is the FQDN part of Address.
	Host string
	// AdminPort is the admin container port on the pod.
	AdminPort int32
}

// DiscoverBrokers finds the Redpanda pods in the given namespace through its
// headless service and returns each one that exposes an admin port.
func DiscoverBrokers(ctx context.Context, client kubernetes.Interface, namespace string) ([]Broker, error) {
	svc, err := headlessService(ctx, client, namespace)
	if err != nil {
		return nil, err
	}

	podList, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(svc.Spec.Selector).String(),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to list pods: %v", err)
	}

	domain := strings.TrimSuffix(clusterDomain(), ".")
	var brokers []Broker
	for _, p := range podList.Items {
		// Skip pods that can't serve: pending, terminating or finished.
		// Each one would cost a failed port-forward and retries.
		if p.Status.Phase != corev1.PodRunning || p.Status.PodIP == "" || p.DeletionTimestamp != nil {
			continue
		}
		port, ok := adminPort(p)
		if !ok {
			continue
		}
		hostname := p.Spec.Hostname
		if hostname == "" {
			hostname = p.Name
		}
		host := fmt.Sprintf("%s.%s.%s.svc.%s", hostname, svc.Name, p.Namespace, domain)
		brokers = append(brokers, Broker{
			Pod:       p,
			Address:   fmt.Sprintf("%s:%d", host, port),
			Host:      host,
			AdminPort: port,
		})
	}
	if len(brokers) == 0 {
		return nil, fmt.Errorf("no running Redpanda pods with an admin port in namespace %s", namespace)
	}
	return brokers, nil
}

func adminPort(p corev1.Pod) (int32, bool) {
	for _, c := range p.Spec.Containers {
		for _, port := range c.Ports {
			if port.Name == "admin" {
				return port.ContainerPort, true
			}
		}
	}
	return 0, false
}

// headlessService returns the headless service that selects the Redpanda
// pods. Headless services without a selector are skipped: a StretchCluster
// in flat-network mode creates one per broker, and an empty selector would
// match every pod in the namespace. Services labeled as Redpanda's are
// preferred over other headless services.
func headlessService(ctx context.Context, client kubernetes.Interface, namespace string) (*corev1.Service, error) {
	services, err := client.CoreV1().Services(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("unable to list services: %v", err)
	}
	var fallback *corev1.Service
	for i := range services.Items {
		svc := &services.Items[i]
		if svc.Spec.ClusterIP != corev1.ClusterIPNone || len(svc.Spec.Selector) == 0 {
			continue
		}
		if svc.Labels["app.kubernetes.io/name"] == "redpanda" {
			return svc, nil
		}
		if fallback == nil {
			fallback = svc
		}
	}
	if fallback != nil {
		return fallback, nil
	}
	return nil, fmt.Errorf("no headless service found in namespace %s", namespace)
}

func clusterDomain() string {
	cname, err := net.LookupCNAME("kubernetes.default.svc")
	if err != nil {
		return "cluster.local."
	}
	return strings.TrimPrefix(cname, "kubernetes.default.svc.")
}
