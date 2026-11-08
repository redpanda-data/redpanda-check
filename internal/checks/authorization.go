package checks

import (
	"context"
	"fmt"
	"strings"

	"github.com/vuldin/redpanda-check/internal/checker"
)

// Authorization validates that Kafka clients must authenticate.
//
// There are two ways to require authentication in Redpanda:
//  1. Global: enable_sasl=true enables SASL on all Kafka listeners.
//  2. Per-listener: kafka_enable_authorization=true plus each kafka_api
//     listener sets authentication_method to sasl or mtls_identity. With
//     mtls_identity the principal comes from the client certificate
//     (kafka_mtls_principal_mapping_rules), and ACLs apply to it like a SASL user.
//
// If the per-listener approach is used, this check verifies every listener
// on every broker authenticates clients with one of those methods.
func Authorization(ctx context.Context, pc *checker.ProductionChecker) {
	r := checker.CheckResult{
		Name:        "authorization_enabled",
		Description: "Authorization enabled",
		Level:       checker.LevelCritical,
	}

	config, err := pc.ClusterConfig(ctx)
	if err != nil {
		r.Status = checker.StatusFail
		r.Details = fmt.Sprintf("Unable to get cluster config: %v", err)
		pc.AddResult(r)
		return
	}

	sasl := boolFromConfig(config, "enable_sasl")
	kafkaAuth := boolFromConfig(config, "kafka_enable_authorization")

	if sasl {
		// Global SASL — all listeners covered.
		r.Status = checker.StatusPass
		if kafkaAuth {
			r.Details = "Authorization enabled (enable_sasl and kafka_enable_authorization)"
		} else {
			r.Details = "Authorization enabled (enable_sasl)"
		}
		pc.AddResult(r)
		return
	}

	if !kafkaAuth {
		// Neither enabled.
		r.Status = checker.StatusFail
		r.Details = "Authorization is disabled (enable kafka_enable_authorization or enable_sasl)"
		pc.AddResult(r)
		return
	}

	// Per-listener mode: kafka_enable_authorization=true but enable_sasl=false.
	// Each kafka_api listener must have authentication_method: sasl.
	configs, _, err := pc.PerBrokerNodeConfig(ctx)
	if err != nil {
		r.Status = checker.StatusFail
		r.Details = fmt.Sprintf("kafka_enable_authorization is enabled but unable to verify per-listener SASL: %v", err)
		pc.AddResult(r)
		return
	}

	var missing []string
	for brokerID, nc := range configs {
		listeners, ok := nc["kafka_api"].([]any)
		if !ok || len(listeners) == 0 {
			missing = append(missing, fmt.Sprintf("broker %s: no kafka_api listeners found", brokerID))
			continue
		}
		for _, entry := range listeners {
			m, ok := entry.(map[string]any)
			if !ok {
				continue
			}
			name, _ := m["name"].(string)
			if name == "" {
				name = fmt.Sprintf("port %v", m["port"])
			}
			authMethod, _ := m["authentication_method"].(string)
			switch authMethod {
			case "sasl", "mtls_identity":
			case "":
				missing = append(missing, fmt.Sprintf("broker %s, listener %s: authentication_method not set", brokerID, name))
			default:
				missing = append(missing, fmt.Sprintf("broker %s, listener %s: authentication_method=%q (expected sasl or mtls_identity)", brokerID, name, authMethod))
			}
		}
	}

	if len(missing) > 0 {
		r.Status = checker.StatusFail
		r.Details = fmt.Sprintf("kafka_enable_authorization is enabled but listeners don't authenticate clients:\n%s",
			strings.Join(missing, "\n"))
	} else {
		r.Status = checker.StatusPass
		r.Details = "Authorization enabled (kafka_enable_authorization with per-listener SASL or mTLS identity)"
	}
	pc.AddResult(r)
}
