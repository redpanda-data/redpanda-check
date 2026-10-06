package checks_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/vuldin/redpanda-check/internal/checker"
	"github.com/vuldin/redpanda-check/internal/checks"
)

// newTestChecker creates a ProductionChecker backed by a fake admin API server
// that responds to the provided handlers. The caller provides a map of URL
// path → handler func. Unmatched paths return 404.
func newTestChecker(t *testing.T, handlers map[string]http.HandlerFunc) *checker.ProductionChecker {
	t.Helper()
	mux := http.NewServeMux()
	for path, handler := range handlers {
		mux.HandleFunc(path, handler)
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client, err := rpadmin.NewAdminAPI([]string{srv.URL}, &rpadmin.NopAuth{}, nil)
	if err != nil {
		t.Fatalf("unable to create admin client: %v", err)
	}
	t.Cleanup(client.Close)

	return &checker.ProductionChecker{
		AdminClient: client,
		Namespace:   "redpanda",
	}
}

func jsonHandler(t *testing.T, v any) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(v)
	}
}

// --- ClusterHealth ---

func TestClusterHealth_Pass(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/cluster/health_overview": jsonHandler(t, rpadmin.ClusterHealthOverview{
			IsHealthy: true,
			AllNodes:  []int{0, 1, 2},
		}),
	})
	checks.ClusterHealth(context.Background(), pc)

	if len(pc.Results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(pc.Results))
	}
	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestClusterHealth_Fail(t *testing.T) {
	lc := 5
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/cluster/health_overview": jsonHandler(t, rpadmin.ClusterHealthOverview{
			IsHealthy:        false,
			UnhealthyReasons: []string{"leaderless partitions"},
			NodesDown:        []int{2},
			LeaderlessCount:  &lc,
		}),
	})
	checks.ClusterHealth(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusFail {
		t.Errorf("expected FAIL, got %s", pc.Results[0].Status)
	}
}

// --- BrokerCount ---

func TestBrokerCount_Pass(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0}, {NodeID: 1}, {NodeID: 2},
		}),
	})
	checks.BrokerCount(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestBrokerCount_Warn(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0},
		}),
	})
	checks.BrokerCount(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusWarn {
		t.Errorf("expected WARN, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

// --- BrokerMembership ---

func TestBrokerMembership_Pass(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0, MembershipStatus: "active"},
			{NodeID: 1, MembershipStatus: "active"},
		}),
	})
	checks.BrokerMembership(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestBrokerMembership_Fail(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0, MembershipStatus: "active"},
			{NodeID: 1, MembershipStatus: "draining"},
		}),
	})
	checks.BrokerMembership(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusFail {
		t.Errorf("expected FAIL, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

// --- MaintenanceMode ---

func TestMaintenanceMode_Pass(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0, Maintenance: nil},
			{NodeID: 1, Maintenance: &rpadmin.MaintenanceStatus{Draining: false}},
		}),
	})
	checks.MaintenanceMode(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestMaintenanceMode_Fail(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0, Maintenance: &rpadmin.MaintenanceStatus{Draining: true}},
		}),
	})
	checks.MaintenanceMode(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusFail {
		t.Errorf("expected FAIL, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

// --- VersionConsistency ---

func TestVersionConsistency_Pass_Latest(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0, Version: "v26.1.1"},
			{NodeID: 1, Version: "v26.1.1"},
		}),
	})
	checks.VersionConsistency(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestVersionConsistency_Pass_NMinus2_StillSupported(t *testing.T) {
	// N-2 is still supported by VersionConsistency; VersionRecency emits the WARN.
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0, Version: "v25.3.0"},
		}),
	})
	checks.VersionConsistency(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestVersionConsistency_Fail_NMinus3_NoLongerSupported(t *testing.T) {
	// Regression test for redpanda-check#14: with the release table current
	// (26.2 latest), 25.2 is N-3 and must FAIL as unsupported -- not PASS as
	// "N-2" the way a stale table (26.1 treated as latest) used to grade it.
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0, Version: "v25.2.0"},
		}),
	})
	checks.VersionConsistency(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusFail {
		t.Errorf("expected FAIL, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestVersionConsistency_Fail_InconsistentAcrossBrokers(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0, Version: "v26.1.1"},
			{NodeID: 1, Version: "v26.1.2"},
		}),
	})
	checks.VersionConsistency(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusFail {
		t.Errorf("expected FAIL, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestVersionConsistency_Fail_InvalidFormatDevBuild(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0, Version: "v0.0.0-dev - 000000"},
		}),
	})
	checks.VersionConsistency(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusFail {
		t.Errorf("expected FAIL, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestVersionConsistency_Fail_UnsupportedOld(t *testing.T) {
	// 24.1 is many minor releases behind 26.1 — out of support.
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0, Version: "v24.1.1"},
		}),
	})
	checks.VersionConsistency(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusFail {
		t.Errorf("expected FAIL, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

// --- VersionRecency ---

func TestVersionRecency_Pass_Latest(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0, Version: "v26.1.1"},
		}),
	})
	checks.VersionRecency(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestVersionRecency_Pass_NMinus1(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0, Version: "v26.1.0"},
		}),
	})
	checks.VersionRecency(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestVersionRecency_Warn_NMinus2(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0, Version: "v25.3.0"},
		}),
	})
	checks.VersionRecency(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusWarn {
		t.Errorf("expected WARN, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestVersionRecency_Skip_OlderThanNMinus2(t *testing.T) {
	// Older than N-2 is already a Critical FAIL in VersionConsistency, so
	// VersionRecency skips to avoid duplicate signaling.
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0, Version: "v24.1.1"},
		}),
	})
	checks.VersionRecency(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusSkip {
		t.Errorf("expected SKIP, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestVersionRecency_Skip_InvalidFormat(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0, Version: "v0.0.0-dev - 000000"},
		}),
	})
	checks.VersionRecency(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusSkip {
		t.Errorf("expected SKIP, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

// --- DeveloperMode ---

func TestDeveloperMode_Pass(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0},
		}),
		"/v1/node_config": jsonHandler(t, map[string]any{
			"developer_mode": false,
		}),
	})
	checks.DeveloperMode(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestDeveloperMode_Fail(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0},
		}),
		"/v1/node_config": jsonHandler(t, map[string]any{
			"developer_mode": true,
		}),
	})
	checks.DeveloperMode(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusFail {
		t.Errorf("expected FAIL, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

// --- Overprovisioned ---

func TestOverprovisioned_Pass(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0},
		}),
		"/v1/node_config": jsonHandler(t, map[string]any{
			"overprovisioned": false,
		}),
	})
	checks.Overprovisioned(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

// --- ReplicationFactor ---

func TestReplicationFactor_Pass(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/cluster_config": jsonHandler(t, map[string]any{
			"default_topic_replications": float64(3),
		}),
	})
	checks.ReplicationFactor(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestReplicationFactor_Fail(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/cluster_config": jsonHandler(t, map[string]any{
			"default_topic_replications": float64(1),
		}),
	})
	checks.ReplicationFactor(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusFail {
		t.Errorf("expected FAIL, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

// --- Authorization ---

func TestAuthorization_Pass_GlobalSASL(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/cluster_config": jsonHandler(t, map[string]any{
			"kafka_enable_authorization": true,
			"enable_sasl":               true,
		}),
	})
	checks.Authorization(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestAuthorization_Pass_PerListenerSASL(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/cluster_config": jsonHandler(t, map[string]any{
			"kafka_enable_authorization": true,
			"enable_sasl":               false,
		}),
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0},
		}),
		"/v1/node_config": jsonHandler(t, map[string]any{
			"kafka_api": []any{
				map[string]any{"name": "internal", "address": "0.0.0.0", "port": float64(9093), "authentication_method": "sasl"},
				map[string]any{"name": "external", "address": "0.0.0.0", "port": float64(9094), "authentication_method": "sasl"},
			},
		}),
	})
	checks.Authorization(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestAuthorization_Fail_NeitherEnabled(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/cluster_config": jsonHandler(t, map[string]any{
			"kafka_enable_authorization": false,
			"enable_sasl":               false,
		}),
	})
	checks.Authorization(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusFail {
		t.Errorf("expected FAIL, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestAuthorization_Fail_PerListenerMissingSASL(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/cluster_config": jsonHandler(t, map[string]any{
			"kafka_enable_authorization": true,
			"enable_sasl":               false,
		}),
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0},
		}),
		"/v1/node_config": jsonHandler(t, map[string]any{
			"kafka_api": []any{
				map[string]any{"name": "internal", "address": "0.0.0.0", "port": float64(9093), "authentication_method": "sasl"},
				map[string]any{"name": "external", "address": "0.0.0.0", "port": float64(9094)},
			},
		}),
	})
	checks.Authorization(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusFail {
		t.Errorf("expected FAIL, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

// --- License ---

func TestLicense_Pass(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/features/license": jsonHandler(t, rpadmin.License{
			Loaded: true,
			Properties: rpadmin.LicenseProperties{
				Type:         "enterprise",
				Organization: "test-org",
				Expires:      4102444800, // 2100-01-01
			},
		}),
	})
	checks.License(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestLicense_Fail_Expired(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/features/license": jsonHandler(t, rpadmin.License{
			Loaded: true,
			Properties: rpadmin.LicenseProperties{
				Type:         "enterprise",
				Organization: "test-org",
				Expires:      1000000000, // 2001-09-08 (long expired)
			},
		}),
	})
	checks.License(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusFail {
		t.Errorf("expected FAIL, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

// --- LicenseExpiry ---

func TestLicenseExpiry_Pass(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/features/license": jsonHandler(t, rpadmin.License{
			Loaded: true,
			Properties: rpadmin.LicenseProperties{
				Expires: time.Now().Add(90 * 24 * time.Hour).Unix(),
			},
		}),
	})
	checks.LicenseExpiry(context.Background(), pc)

	if len(pc.Results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(pc.Results))
	}
	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestLicenseExpiry_Warn_30Days(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/features/license": jsonHandler(t, rpadmin.License{
			Loaded: true,
			Properties: rpadmin.LicenseProperties{
				Expires: time.Now().Add(15 * 24 * time.Hour).Unix(),
			},
		}),
	})
	checks.LicenseExpiry(context.Background(), pc)

	if len(pc.Results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(pc.Results))
	}
	r := pc.Results[0]
	if r.Status != checker.StatusWarn {
		t.Errorf("expected WARN, got %s: %s", r.Status, r.Details)
	}
	if r.Level != checker.LevelRecommended {
		t.Errorf("expected level recommended, got %s", r.Level)
	}
}

func TestLicenseExpiry_Fail_7Days(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/features/license": jsonHandler(t, rpadmin.License{
			Loaded: true,
			Properties: rpadmin.LicenseProperties{
				Expires: time.Now().Add(3 * 24 * time.Hour).Unix(),
			},
		}),
	})
	checks.LicenseExpiry(context.Background(), pc)

	if len(pc.Results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(pc.Results))
	}
	r := pc.Results[0]
	if r.Status != checker.StatusFail {
		t.Errorf("expected FAIL, got %s: %s", r.Status, r.Details)
	}
	if r.Level != checker.LevelCritical {
		t.Errorf("expected level critical, got %s", r.Level)
	}
}

func TestLicenseExpiry_NoResult_WhenNotLoaded(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/features/license": jsonHandler(t, rpadmin.License{
			Loaded: false,
		}),
	})
	checks.LicenseExpiry(context.Background(), pc)

	if len(pc.Results) != 0 {
		t.Errorf("expected no results for unloaded license, got %d", len(pc.Results))
	}
}

func TestLicense_Warn_NotLoaded(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/features/license": jsonHandler(t, rpadmin.License{
			Loaded: false,
		}),
	})
	checks.License(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusWarn {
		t.Errorf("expected WARN, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

// --- ExistingTopicsReplication ---

func TestExistingTopicsReplication_Pass(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/cluster/partitions": jsonHandler(t, []rpadmin.ClusterPartition{
			{Ns: "kafka", Topic: "test-topic", PartitionID: 0, Replicas: []rpadmin.Replica{{NodeID: 0}, {NodeID: 1}, {NodeID: 2}}},
		}),
	})
	checks.ExistingTopicsReplication(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestExistingTopicsReplication_Fail(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/cluster/partitions": jsonHandler(t, []rpadmin.ClusterPartition{
			{Ns: "kafka", Topic: "under-rep", PartitionID: 0, Replicas: []rpadmin.Replica{{NodeID: 0}}},
		}),
	})
	checks.ExistingTopicsReplication(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusFail {
		t.Errorf("expected FAIL, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

// --- DataBalancing ---

func TestDataBalancing_Pass(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/cluster_config": jsonHandler(t, map[string]any{
			"partition_autobalancing_mode": "continuous",
		}),
	})
	checks.DataBalancing(context.Background(), pc)
	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestDataBalancing_Warn(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/cluster_config": jsonHandler(t, map[string]any{
			"partition_autobalancing_mode": "off",
		}),
	})
	checks.DataBalancing(context.Background(), pc)
	if pc.Results[0].Status != checker.StatusWarn {
		t.Errorf("expected WARN, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

// --- RackAwareness ---

func TestRackAwareness_Pass(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/cluster_config": jsonHandler(t, map[string]any{
			"enable_rack_awareness": true,
		}),
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0, Rack: "us-east-1a"},
			{NodeID: 1, Rack: "us-east-1b"},
		}),
	})
	checks.RackAwareness(context.Background(), pc)
	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestRackAwareness_Warn_Disabled(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/cluster_config": jsonHandler(t, map[string]any{
			"enable_rack_awareness": false,
		}),
	})
	checks.RackAwareness(context.Background(), pc)
	if pc.Results[0].Status != checker.StatusWarn {
		t.Errorf("expected WARN, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

// --- MinTopicReplications ---

func TestMinTopicReplications_Pass(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/cluster_config": jsonHandler(t, map[string]any{
			"minimum_topic_replications": float64(3),
		}),
	})
	checks.MinTopicReplications(context.Background(), pc)
	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestMinTopicReplications_Fail(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/cluster_config": jsonHandler(t, map[string]any{
			"minimum_topic_replications": float64(1),
		}),
	})
	checks.MinTopicReplications(context.Background(), pc)
	if pc.Results[0].Status != checker.StatusFail {
		t.Errorf("expected FAIL, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

// --- TieredStorage ---

func TestTieredStorage_Pass(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/cluster_config": jsonHandler(t, map[string]any{
			"cloud_storage_enabled": true,
			"cloud_storage_bucket":  "my-bucket",
			"cloud_storage_region":  "us-east-1",
		}),
	})
	checks.TieredStorage(context.Background(), pc)
	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

// --- AuditLogging ---

func TestAuditLogging_Pass(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/cluster_config": jsonHandler(t, map[string]any{
			"audit_enabled": true,
		}),
	})
	checks.AuditLogging(context.Background(), pc)
	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

// --- CoreBalancing ---

func TestCoreBalancing_Pass(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/cluster_config": jsonHandler(t, map[string]any{
			"core_balancing_on_core_count_change": true,
		}),
	})
	checks.CoreBalancing(context.Background(), pc)
	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

// --- EnterpriseLicense ---

func TestEnterpriseLicense_Pass_NoFeatures(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/features/enterprise": jsonHandler(t, rpadmin.EnterpriseFeaturesResponse{
			LicenseStatus: "valid",
			Violation:     false,
			Features:      []rpadmin.EnterpriseFeature{},
		}),
	})
	checks.EnterpriseLicense(context.Background(), pc)
	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestEnterpriseLicense_Fail_Violation(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/features/enterprise": jsonHandler(t, rpadmin.EnterpriseFeaturesResponse{
			LicenseStatus: "expired",
			Violation:     true,
			Features: []rpadmin.EnterpriseFeature{
				{Name: "audit_logging", Enabled: true},
			},
		}),
	})
	checks.EnterpriseLicense(context.Background(), pc)
	if pc.Results[0].Status != checker.StatusFail {
		t.Errorf("expected FAIL, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

// --- PartitionBalancerStatus ---

func TestPartitionBalancerStatus_Pass(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/cluster/partition_balancer/status": jsonHandler(t, map[string]any{
			"status": "ready",
		}),
	})
	checks.PartitionBalancerStatus(context.Background(), pc)
	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestPartitionBalancerStatus_Stalled(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/cluster/partition_balancer/status": jsonHandler(t, map[string]any{
			"status": "stalled",
		}),
	})
	checks.PartitionBalancerStatus(context.Background(), pc)
	if pc.Results[0].Status != checker.StatusWarn {
		t.Errorf("expected WARN, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

// --- SASLUsers ---

func TestSASLUsers_Pass(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/cluster_config": jsonHandler(t, map[string]any{
			"kafka_enable_authorization": true,
			"enable_sasl":               true,
		}),
		"/v1/security/users": jsonHandler(t, []string{"admin", "producer"}),
	})
	checks.SASLUsers(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestSASLUsers_Fail_NoUsers(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/cluster_config": jsonHandler(t, map[string]any{
			"kafka_enable_authorization": true,
			"enable_sasl":               true,
		}),
		"/v1/security/users": jsonHandler(t, []string{}),
	})
	checks.SASLUsers(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusFail {
		t.Errorf("expected FAIL, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestSASLUsers_Skip_AuthDisabled(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/cluster_config": jsonHandler(t, map[string]any{
			"kafka_enable_authorization": false,
			"enable_sasl":               false,
		}),
	})
	checks.SASLUsers(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusSkip {
		t.Errorf("expected SKIP, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

// --- Superusers ---

func TestSuperusers_Pass(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/cluster_config": jsonHandler(t, map[string]any{
			"kafka_enable_authorization": true,
			"superusers":                []any{"admin"},
		}),
	})
	checks.Superusers(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestSuperusers_Warn_Empty(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/cluster_config": jsonHandler(t, map[string]any{
			"kafka_enable_authorization": true,
			"superusers":                []any{},
		}),
	})
	checks.Superusers(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusWarn {
		t.Errorf("expected WARN, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestSuperusers_Skip_AuthDisabled(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/cluster_config": jsonHandler(t, map[string]any{
			"kafka_enable_authorization": false,
			"enable_sasl":               false,
		}),
	})
	checks.Superusers(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusSkip {
		t.Errorf("expected SKIP, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

// --- AdvertisedAddresses ---

func TestAdvertisedAddresses_Pass(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0},
		}),
		"/v1/node_config": jsonHandler(t, map[string]any{
			"advertised_kafka_api": []any{
				map[string]any{"address": "192.168.1.10", "port": float64(9092)},
			},
			"advertised_rpc_api": map[string]any{
				"address": "192.168.1.10", "port": float64(33145),
			},
		}),
	})
	checks.AdvertisedAddresses(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestAdvertisedAddresses_Fail_ZeroAddress(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0},
		}),
		"/v1/node_config": jsonHandler(t, map[string]any{
			"advertised_kafka_api": []any{
				map[string]any{"address": "0.0.0.0", "port": float64(9092)},
			},
			"advertised_rpc_api": map[string]any{
				"address": "192.168.1.10", "port": float64(33145),
			},
		}),
	})
	checks.AdvertisedAddresses(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusFail {
		t.Errorf("expected FAIL, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestAdvertisedAddresses_Fail_Missing(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0},
		}),
		"/v1/node_config": jsonHandler(t, map[string]any{}),
	})
	checks.AdvertisedAddresses(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusFail {
		t.Errorf("expected FAIL, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

// --- CPUMemoryRatio (non-K8s, via metrics) ---

func TestCPUMemoryRatio_Pass_ViaMetrics(t *testing.T) {
	// 4 shards, each with 1 GiB allocated + 1 GiB available = 2 GiB per shard
	// 4 cores * 2 GiB = 8 GiB total, ratio = 2.0:1
	metricsBody := `
redpanda_memory_allocated_memory{shard="0"} 1073741824.0
redpanda_memory_allocated_memory{shard="1"} 1073741824.0
redpanda_memory_allocated_memory{shard="2"} 1073741824.0
redpanda_memory_allocated_memory{shard="3"} 1073741824.0
redpanda_memory_available_memory{shard="0"} 1073741824.0
redpanda_memory_available_memory{shard="1"} 1073741824.0
redpanda_memory_available_memory{shard="2"} 1073741824.0
redpanda_memory_available_memory{shard="3"} 1073741824.0
`
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0, NumCores: 4},
		}),
		"/public_metrics": func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(metricsBody))
		},
	})
	checks.CPUMemoryRatio(context.Background(), pc)

	if len(pc.Results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(pc.Results))
	}
	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestCPUMemoryRatio_Fail_ViaMetrics(t *testing.T) {
	// 4 shards, each with 0.25 GiB allocated + 0.25 GiB available = 0.5 GiB per shard
	// 4 cores * 0.5 GiB = 2 GiB total, ratio = 0.5:1 (below 2:1)
	metricsBody := `
redpanda_memory_allocated_memory{shard="0"} 268435456.0
redpanda_memory_allocated_memory{shard="1"} 268435456.0
redpanda_memory_allocated_memory{shard="2"} 268435456.0
redpanda_memory_allocated_memory{shard="3"} 268435456.0
redpanda_memory_available_memory{shard="0"} 268435456.0
redpanda_memory_available_memory{shard="1"} 268435456.0
redpanda_memory_available_memory{shard="2"} 268435456.0
redpanda_memory_available_memory{shard="3"} 268435456.0
`
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0, NumCores: 4},
		}),
		"/public_metrics": func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(metricsBody))
		},
	})
	checks.CPUMemoryRatio(context.Background(), pc)

	if len(pc.Results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(pc.Results))
	}
	if pc.Results[0].Status != checker.StatusFail {
		t.Errorf("expected FAIL, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestCPUMemoryRatioRecommended_Warn_ViaMetrics(t *testing.T) {
	// 4 cores, 2 GiB per core = ratio 2.0:1 (passes 1:2 minimum but below 1:4 recommended)
	metricsBody := `
redpanda_memory_allocated_memory{shard="0"} 1073741824.0
redpanda_memory_allocated_memory{shard="1"} 1073741824.0
redpanda_memory_allocated_memory{shard="2"} 1073741824.0
redpanda_memory_allocated_memory{shard="3"} 1073741824.0
redpanda_memory_available_memory{shard="0"} 1073741824.0
redpanda_memory_available_memory{shard="1"} 1073741824.0
redpanda_memory_available_memory{shard="2"} 1073741824.0
redpanda_memory_available_memory{shard="3"} 1073741824.0
`
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/brokers": jsonHandler(t, []rpadmin.Broker{
			{NodeID: 0, NumCores: 4},
		}),
		"/public_metrics": func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(metricsBody))
		},
	})
	checks.CPUMemoryRatioRecommended(context.Background(), pc)

	if len(pc.Results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(pc.Results))
	}
	if pc.Results[0].Status != checker.StatusWarn {
		t.Errorf("expected WARN, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

// --- DebugBundlePermissions ---

func TestDebugBundlePermissions_Pass(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/debug/bundle/check_permissions": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"probes":[
				{"category":"file","resource":"/proc/slabinfo","ok":true},
				{"category":"command","resource":"journalctl","ok":true}
			]}`))
		},
	})
	checks.DebugBundlePermissions(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusPass {
		t.Errorf("expected PASS, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestDebugBundlePermissions_Warn(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/debug/bundle/check_permissions": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"probes":[
				{"category":"file","resource":"/proc/slabinfo","ok":false,"error":"permission denied","hint":"requires root"},
				{"category":"command","resource":"dmidecode","ok":false,"error":"requires root"}
			]}`))
		},
	})
	checks.DebugBundlePermissions(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusWarn {
		t.Errorf("expected WARN, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestDebugBundlePermissions_Skip_NotImplemented(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/debug/bundle/check_permissions": func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		},
	})
	checks.DebugBundlePermissions(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusSkip {
		t.Errorf("expected SKIP, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

func TestDebugBundlePermissions_Skip_NoSuperuser(t *testing.T) {
	pc := newTestChecker(t, map[string]http.HandlerFunc{
		"/v1/debug/bundle/check_permissions": func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusForbidden)
		},
	})
	checks.DebugBundlePermissions(context.Background(), pc)

	if pc.Results[0].Status != checker.StatusSkip {
		t.Errorf("expected SKIP, got %s: %s", pc.Results[0].Status, pc.Results[0].Details)
	}
}

// --- K8s checks skip when no K8s client ---

func TestK8sChecks_SkipWithoutClient(t *testing.T) {
	// These checks should all SKIP when K8sClient is nil.
	pc := &checker.ProductionChecker{
		Namespace: "redpanda",
	}

	k8sChecks := []struct {
		name  string
		check checker.Check
	}{
		{"PersistentStorage", checks.PersistentStorage},
		{"ResourceLimits", checks.ResourceLimits},
		{"PodDisruptionBudget", checks.PodDisruptionBudget},
		{"NoFractionalCPU", checks.NoFractionalCPU},
		{"TopologySpread", checks.TopologySpread},
		{"NodeIsolation", checks.NodeIsolation},
		{"StorageClassValidation", checks.StorageClassValidation},
		{"StoragePerformance", checks.StoragePerformance},
		{"KubernetesVersion", checks.KubernetesVersion},
		{"NetworkPolicies", checks.NetworkPolicies},
	}

	for _, tc := range k8sChecks {
		t.Run(tc.name, func(t *testing.T) {
			before := len(pc.Results)
			tc.check(context.Background(), pc)
			if len(pc.Results) <= before {
				t.Fatal("expected a result to be added")
			}
			r := pc.Results[len(pc.Results)-1]
			if r.Status != checker.StatusSkip {
				t.Errorf("expected SKIP, got %s: %s", r.Status, r.Details)
			}
		})
	}
}
