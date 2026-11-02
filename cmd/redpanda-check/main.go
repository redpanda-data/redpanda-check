package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/vuldin/redpanda-check/internal/admin"
	"github.com/vuldin/redpanda-check/internal/checker"
	"github.com/vuldin/redpanda-check/internal/checks"
	"github.com/vuldin/redpanda-check/internal/k8s"
	"k8s.io/client-go/kubernetes"
)

var version = "dev"

func main() {
	root := newRootCmd()

	// Handle --help-autocomplete for rpk plugin integration.
	if len(os.Args) > 1 && os.Args[1] == "--help-autocomplete" {
		if err := printAutocomplete(os.Stdout, root); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if err := root.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	var (
		adminURLs     []string
		tlsCA         string
		tlsCert       string
		tlsKey        string
		tlsSkipVerify bool
		saslUser      string
		saslPass      string
		namespace     string
		kubeconfig    string
		format        string
		showAll       bool
		showVersion   bool
	)

	root := &cobra.Command{
		Use:   "redpanda-check",
		Short: "Production readiness checks for Redpanda deployments",
		Long: `Validates a Redpanda deployment against production readiness standards.

Checks cluster health, configuration, resource allocation, and operational
best practices. Connects to the Redpanda admin API directly.

By default only failed and warning checks are shown. Use -a to see all results.`,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if showVersion {
				fmt.Println(version)
				return nil
			}
			// Env-var fallback for SASL credentials. Lets callers (notably
			// `rpk check`) pass the password without putting it on the
			// command line, where it would leak via process listings and
			// rpk's plugin-args debug log. Flags win if both are set.
			if saslUser == "" {
				saslUser = os.Getenv("REDPANDA_SASL_USERNAME")
			}
			if saslPass == "" {
				saslPass = os.Getenv("REDPANDA_SASL_PASSWORD")
			}
			return run(cmd.Context(), runConfig{
				adminURLs:     adminURLs,
				tlsCA:         tlsCA,
				tlsCert:       tlsCert,
				tlsKey:        tlsKey,
				tlsSkipVerify: tlsSkipVerify,
				saslUser:      saslUser,
				saslPass:      saslPass,
				namespace:     namespace,
				kubeconfig:    kubeconfig,
				format:        format,
				showAll:       showAll,
			})
		},
	}

	f := root.Flags()
	f.StringSliceVar(&adminURLs, "admin-url", nil, "Admin API addresses (comma-separated)")
	f.StringVar(&tlsCA, "admin-tls-ca", "", "TLS CA certificate path")
	f.StringVar(&tlsCert, "admin-tls-cert", "", "TLS client certificate path")
	f.StringVar(&tlsKey, "admin-tls-key", "", "TLS client key path")
	f.BoolVar(&tlsSkipVerify, "admin-tls-skip-verify", false, "Skip TLS certificate verification (insecure)")
	f.StringVar(&saslUser, "sasl-user", "", "SASL username for admin API basic auth (or REDPANDA_SASL_USERNAME env var)")
	f.StringVar(&saslPass, "sasl-password", "", "SASL password (or REDPANDA_SASL_PASSWORD env var; prefer env var to avoid leaking via process listings)")
	f.StringVarP(&namespace, "namespace", "n", "", "Kubernetes namespace (enables K8s checks)")
	f.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig file")
	f.StringVar(&format, "format", "text", "Output format: text, json")
	f.BoolVarP(&showAll, "all", "a", false, "Show all checks including passing and skipped")
	f.BoolVar(&showVersion, "version", false, "Print version and exit")

	return root
}

type runConfig struct {
	adminURLs     []string
	tlsCA         string
	tlsCert       string
	tlsKey        string
	tlsSkipVerify bool
	saslUser      string
	saslPass      string
	namespace     string
	kubeconfig    string
	format        string
	showAll       bool
}

func run(ctx context.Context, cfg runConfig) error {
	ns := resolveNamespace(cfg.namespace)

	// Attempt K8s client setup for discovery and/or K8s-specific checks.
	var k8sClient kubernetes.Interface
	var discoveredAddrs []string

	if len(cfg.adminURLs) == 0 || cfg.namespace != "" {
		cl, err := k8s.NewClient(cfg.kubeconfig)
		if err == nil {
			k8sClient = cl
			addrs, err := k8s.DiscoverAdminAddresses(ctx, cl, ns)
			if err == nil && len(addrs) > 0 {
				discoveredAddrs = addrs
			}
		}
	}

	if len(cfg.adminURLs) == 0 {
		if len(discoveredAddrs) > 0 {
			cfg.adminURLs = discoveredAddrs
			fmt.Fprintf(os.Stderr, "Discovered Redpanda in namespace %q with %d brokers\n", ns, len(cfg.adminURLs))
		} else {
			return fmt.Errorf("no --admin-url provided and Kubernetes discovery failed; specify --admin-url")
		}
	}

	if cfg.tlsSkipVerify {
		fmt.Fprintf(os.Stderr, "WARNING: TLS certificate verification is disabled. Do not use --admin-tls-skip-verify in production.\n")
	}

	// Build admin API client.
	adminClient, err := admin.NewClient(admin.Config{
		Addresses:     cfg.adminURLs,
		TLSCa:        cfg.tlsCA,
		TLSCert:      cfg.tlsCert,
		TLSKey:       cfg.tlsKey,
		TLSSkipVerify: cfg.tlsSkipVerify,
		SASLUser:      cfg.saslUser,
		SASLPassword:  cfg.saslPass,
	})
	if err != nil {
		return fmt.Errorf("unable to create admin API client: %v", err)
	}
	defer adminClient.Close()

	// Build the checker with shared clients.
	pc := &checker.ProductionChecker{
		AdminClient: adminClient,
		K8sClient:   k8sClient,
		Namespace:   ns,
	}

	// Run all checks, organized by level and type.
	allChecks := []checker.Check{
		// Critical checks (must pass for production readiness).
		checks.ClusterHealth,
		checks.License,
		checks.LicenseExpiry,
		checks.EnterpriseLicense,
		checks.BrokerCount,
		checks.BrokerMembership,
		checks.MaintenanceMode,
		checks.DeveloperMode,
		checks.Overprovisioned,
		checks.ReplicationFactor,
		checks.MinTopicReplications,
		checks.ExistingTopicsReplication,
		checks.VersionConsistency,
		checks.Authorization,
		checks.SASLUsers,
		checks.TLSEnabled,
		checks.InternalRPCTLS,
		checks.AdvertisedAddresses,
		// Recommended checks (best practices).
		checks.VersionRecency,
		checks.Superusers,
		checks.DataBalancing,
		checks.RackAwareness,
		checks.TieredStorage,
		checks.AuditLogging,
		checks.CoreBalancing,
		checks.PartitionBalancerStatus,
		checks.BallastFile,
		checks.DebugBundlePermissions,
		// K8s-specific checks (skip gracefully when no K8s client).
		checks.PersistentStorage,
		checks.StorageClassValidation,
		checks.StoragePerformance,
		checks.ResourceLimits,
		checks.PodDisruptionBudget,
		checks.CPUMemoryRatio,
		checks.CPUMemoryRatioRecommended,
		checks.NoFractionalCPU,
		checks.TopologySpread,
		checks.NodeIsolation,
		checks.TuningInitContainer,
		checks.KubernetesVersion,
		checks.NetworkPolicies,
	}

	for _, check := range allChecks {
		check(ctx, pc)
	}

	// Generate and print report.
	report := checker.NewReport(pc.Results)

	switch strings.ToLower(cfg.format) {
	case "json":
		if err := checker.PrintJSON(os.Stdout, report); err != nil {
			return fmt.Errorf("unable to print JSON report: %v", err)
		}
	default:
		checker.PrintText(os.Stdout, report, cfg.showAll, k8sClient != nil)
	}

	if report.OverallStatus == checker.StatusFail {
		os.Exit(1)
	}
	return nil
}

func resolveNamespace(ns string) string {
	if ns != "" {
		return ns
	}
	if env := os.Getenv("NAMESPACE"); env != "" {
		return env
	}
	if data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		return strings.TrimSpace(string(data))
	}
	return "redpanda"
}

// pluginHelp is one entry of the --help-autocomplete output rpk reads.
// Use is the command's cobra Use line, positional arguments included, so
// rpk can print them in the command's usage. Older rpk ignores it.
type pluginHelp struct {
	Path    string       `json:"path"`
	Short   string       `json:"short"`
	Long    string       `json:"long"`
	Example string       `json:"example"`
	Args    []string     `json:"args"`
	Use     string       `json:"use"`
	Flags   []pluginFlag `json:"flags"`
}

// pluginFlag matches the flag shape rpk prints in --print-tree.
type pluginFlag struct {
	Name        string `json:"name"`
	Shorthand   string `json:"shorthand,omitempty"`
	Type        string `json:"type"`
	Description string `json:"description"`
	Default     any    `json:"default"`
	Required    bool   `json:"required"`
	Deprecated  string `json:"deprecated,omitempty"`
}

func printAutocomplete(w io.Writer, root *cobra.Command) error {
	var helps []pluginHelp
	walkHelps(root, "check", &helps)
	return json.NewEncoder(w).Encode(helps)
}

// walkHelps adds an entry for cmd and each visible subcommand. Cobra's help
// and completion commands are skipped because rpk provides its own.
func walkHelps(cmd *cobra.Command, path string, out *[]pluginHelp) {
	args := []string{}
	flags := []pluginFlag{}
	cmd.LocalFlags().VisitAll(func(f *pflag.Flag) {
		if f.Hidden || f.Name == "help" {
			return
		}
		args = append(args, "--"+f.Name)
		flags = append(flags, pluginFlag{
			Name:        f.Name,
			Shorthand:   f.Shorthand,
			Type:        f.Value.Type(),
			Description: f.Usage,
			Default:     parseDefault(f),
			Required:    slices.Contains(f.Annotations[cobra.BashCompOneRequiredFlag], "true"),
			Deprecated:  f.Deprecated,
		})
	})
	*out = append(*out, pluginHelp{
		Path:    path,
		Short:   cmd.Short,
		Long:    cmd.Long,
		Example: cmd.Example,
		Args:    args,
		Use:     cmd.Use,
		Flags:   flags,
	})
	for _, sub := range cmd.Commands() {
		if sub.Hidden || sub.Name() == "help" || sub.Name() == "completion" {
			continue
		}
		walkHelps(sub, path+"_"+sub.Name(), out)
	}
}

// parseDefault types a flag default the same way rpk does in --print-tree.
func parseDefault(f *pflag.Flag) any {
	t := f.Value.Type()
	if strings.HasSuffix(t, "Slice") || strings.HasSuffix(t, "Array") {
		if f.DefValue == "" || f.DefValue == "[]" {
			return []any{}
		}
		return f.DefValue
	}
	if f.DefValue == "" {
		if t == "string" {
			return ""
		}
		return nil
	}
	switch t {
	case "bool":
		if b, err := strconv.ParseBool(f.DefValue); err == nil {
			return b
		}
	case "int", "int8", "int16", "int32", "int64",
		"uint", "uint8", "uint16", "uint32", "uint64", "count":
		if n, err := strconv.ParseInt(f.DefValue, 10, 64); err == nil {
			return n
		}
	case "float32", "float64":
		if x, err := strconv.ParseFloat(f.DefValue, 64); err == nil {
			return x
		}
	}
	return f.DefValue
}
