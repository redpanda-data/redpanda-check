# redpanda-check

Production readiness validation for Redpanda deployments. Connects to the Redpanda admin API and checks cluster health, configuration, resource allocation, and operational best practices.

Designed to run as an [rpk managed plugin](https://docs.redpanda.com/current/reference/rpk/) or as a standalone binary.

## Installation

There are three ways to get `redpanda-check` running. They're listed here in
order of recommendation; pick the one that matches your environment.

### 1. Automated: rpk managed plugin (recommended)

Requires `rpk` v26.2.1 or later. Just run:

```
rpk check
```

On first use, `rpk` auto-downloads the latest release, verifies its checksum,
and caches it; no separate install step needed. It also manages upgrades:

```
rpk check install                          # install the latest version
rpk check install --check-version 0.1.2    # install a specific version, if still published
rpk check upgrade                          # upgrade the cached copy
rpk check uninstall                        # remove it
rpk check --version                        # print the installed plugin version
```

> Note: `--check-version` only works for a version still retained in the published
plugin manifest, which is not necessarily every historical GitHub release tag. Older
versions get pruned as new ones ship (at the time of writing, only the two
most recent releases are retained). Requesting a pruned version fails with
`unable to find version "X"; you may install 'redpanda-check' manually`.

When invoked this way, `rpk check` also fills in `--admin-url` and any
TLS/SASL connection details from your active rpk profile if you don't pass
them explicitly, so it behaves like any other `rpk` subcommand rather than a
separately-configured tool.

See the [rpk check reference](https://docs.redpanda.com/current/reference/rpk/rpk-check/rpk-check/)
for the full command reference.

### 2. Manual: install as a managed plugin (offline / air-gapped environments)

Use this when the host running `rpk` has no network path to
`rpk-plugins.redpanda.com`. First get the binary onto the host through some other channel
then place it where `rpk`'s plugin loader expects a *managed* plugin to live.

1. Obtain a `redpanda-check` binary for the target OS/architecture. This can come from Redpanda customer success team, or you can build your own following the first part of the standalone binary steps below.
2. Copy it into `rpk`'s default plugin directory (`~/.local/bin`) under the
   exact filename `rpk` looks for:

   ```
   mkdir -p ~/.local/bin
   cp redpanda-check ~/.local/bin/.rpk.managed-check
   chmod 755 ~/.local/bin/.rpk.managed-check
   ```

3. Confirm `rpk` picks it up:

   ```
   rpk check --version
   rpk check
   ```

`rpk check install`, `upgrade`, and `uninstall` all work against a manually
placed binary exactly as they would against an auto-downloaded one (`install`
reports it's already present, `uninstall` removes the file, and so on) — none
of that requires network access. `rpk` only reaches out to the network when it
needs to *download* a version it doesn't already have on disk, so once the
binary is in place, every other `rpk check` operation is fully offline.

If the binary is instead named `~/.local/bin/.rpk-check` (no `.managed`
segment), `rpk` still finds and runs it as a plain plugin, but prints a
one-time warning that it isn't rpk-managed and won't respond to
`install`/`upgrade`/`uninstall`. Use the `.managed` filename above to avoid
that and get full managed-plugin behavior.

### 3. Standalone binary

Run `redpanda-check` directly, independent of `rpk` entirely. Useful for CI
pipelines or any context where you don't want `rpk` involved at all.

```
go install github.com/vuldin/redpanda-check/cmd/redpanda-check@latest
```

Or build from source:

```
git clone https://github.com/redpanda-data/redpanda-check.git
cd redpanda-check
go build -o redpanda-check ./cmd/redpanda-check
```

## Usage

Point it at your Redpanda admin API:

```
redpanda-check --admin-url localhost:9644
```

For Kubernetes deployments, pass a namespace to enable K8s-specific checks (resource limits, PDBs, storage validation):

```
redpanda-check --admin-url localhost:9644 --namespace redpanda
```

If you omit `--admin-url`, the tool attempts to discover admin API endpoints from Kubernetes using the current kubeconfig context:

```
redpanda-check --namespace redpanda
```

### Authentication

```
redpanda-check --admin-url localhost:9644 \
  --sasl-user admin --sasl-password secret

redpanda-check --admin-url localhost:9644 \
  --admin-tls-ca /path/to/ca.crt \
  --admin-tls-cert /path/to/client.crt \
  --admin-tls-key /path/to/client.key
```

### Output

By default only failures and warnings are printed. Use `-a` (or `--all`) to see all results:

```
redpanda-check --admin-url localhost:9644 -a
```

JSON output for CI/CD pipelines:

```
redpanda-check --admin-url localhost:9644 --format json
```

The process exits with code 1 if any critical check fails.

## Checks

All checks run against the admin API and work regardless of how Redpanda was deployed (Helm, operator, Ansible, bare metal). Checks that require Kubernetes are skipped when no `--namespace` is provided.

Based on the Redpanda production readiness checklists:
- [Bare metal / VM](https://docs.redpanda.com/current/deploy/redpanda/manual/production/production-readiness/)
- [Kubernetes](https://docs.redpanda.com/current/deploy/redpanda/kubernetes/k-production-readiness/)

### Critical

Must pass for production readiness. Failures exit with code 1.

| Check | What it validates |
|-------|-------------------|
| cluster_health | All brokers healthy, no leaderless/under-replicated partitions |
| license_check | Valid license loaded and not expired |
| license_expiry | License not expiring within 7 days |
| enterprise_license_compliance | No enterprise features used without valid license |
| broker_count | At least 3 brokers |
| broker_membership | All brokers active (not decommissioning) |
| maintenance_mode | No brokers in maintenance mode |
| developer_mode | Developer mode disabled |
| overprovisioned | Overprovisioned mode disabled |
| replication_factor | Default topic replication factor >= 3 |
| min_topic_replications | Minimum topic replications >= 3 |
| existing_topics_replication | All topics have replication factor >= 3 |
| version_consistency | Same valid `vMAJOR.MINOR.PATCH` version on all brokers, and within the supported window (latest or N-2) |
| authorization_enabled | SASL enabled cluster-wide (`enable_sasl`) or per-listener (`kafka_enable_authorization` with `authentication_method: sasl` on each Kafka listener) |
| sasl_users | At least one SASL user exists when auth is enabled |
| tls_enabled | TLS on Kafka API and Admin API listeners (checked per-broker) |
| internal_rpc_tls | TLS on internal RPC (checked per-broker) |
| advertised_addresses | Advertised Kafka and RPC addresses are routable (not 0.0.0.0 or empty) |
| persistent_storage | PVCs present, no hostPath provisioner (K8s) |
| storage_performance | Local NVMe storage, not network-attached EBS/PD/Azure Disk (K8s) |
| resource_limits | CPU/memory requests equal limits (K8s) |
| pod_disruption_budget | PDB exists in namespace (K8s) |
| cpu_memory_ratio | Memory-to-CPU ratio >= 2 GiB per core |
| no_fractional_cpu | Whole-integer CPU allocations (K8s) |

### Recommended

Best practices. Failures produce warnings but do not affect exit code.

| Check | What it validates |
|-------|-------------------|
| license_expiry | License not expiring within 30 days |
| superusers_configured | At least one superuser configured when auth is enabled |
| continuous_data_balancing | partition_autobalancing_mode is "continuous" |
| rack_awareness | Rack awareness enabled, all brokers have rack IDs |
| tiered_storage | Cloud storage enabled |
| audit_logging | Audit logging enabled |
| core_balancing | Core balancing on core count change enabled |
| partition_balancer_status | Partition balancer not stalled |
| ballast_file | Ballast file configured (checked per-broker) |
| debug_bundle_permissions | `rpk debug bundle --dry-run` on each broker reports no permission gaps (file access, command availability, K8s RBAC) |
| storage_class_validation | StorageClass provisioner and parameters match cloud provider best practices (K8s) |
| cpu_memory_ratio_recommended | Memory-to-CPU ratio >= 4 GiB per core |
| topology_spread | topologySpreadConstraints or podAntiAffinity configured (K8s) |
| node_isolation | Dedicated node scheduling via nodeSelector/tolerations (K8s) |
| tuning_init_container | Tuning init container completed on all pods (K8s) |
| kubernetes_version | Kubernetes nodes running a supported version (K8s) |
| version_recency | Redpanda version is the latest or N-1 (warns at N-2 approaching EOL) |
| network_policies | At least one NetworkPolicy exists in namespace (K8s) |

## Releasing

Tagged releases trigger a GitHub Actions workflow that:

1. Builds multi-arch binaries via GoReleaser (linux/darwin, amd64/arm64)
2. Uploads artifacts to `s3://rpk-plugins-repo/check/`
3. Generates `rpk-plugins.redpanda.com/check/manifest.json`

To release:

```
git tag v0.1.0
git push origin v0.1.0
```

## Testing

```
go test ./...
```
