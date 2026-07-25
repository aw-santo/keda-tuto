---
title: Production Operations
tags:
  - keda
  - kubernetes
  - operations
  - platform
  - autoscaling
  - senior
aliases:
  - KEDA Production
  - KEDA Upgrades
  - watchNamespace
  - KEDA HA
status: ready
created: 2026-07-25
---

# Production Operations

> [!abstract] Scope
> Running KEDA as **platform infrastructure** rather than as a feature of one team's Deployment: how to install it so upgrades don't surprise you, why version pinning is non-negotiable, what `helm uninstall` actually destroys, what "high availability" does and doesn't mean here, how to scope it in a shared cluster, and the gap nobody mentions in tutorials — **KEDA scales pods, not nodes.** The one idea to take away: KEDA is a **cluster-level singleton with a moving API**, and every operational decision follows from those two facts.

Related: [[00 - KEDA Index]], [[03 - KEDA Architecture — Operator, Metrics Adapter & the HPA It Owns]], [[04 - The ScaledObject CRD, End to End]], [[07 - The Kafka Scaler — Every Field & the Replica Math]], [[08 - Scale-to-Zero & Activation]], [[10 - Tuning Scaling Behaviour]], [[12 - Authentication — TriggerAuthentication, SASL, TLS & MSK IAM]], [[14 - Observability & Debugging — Why Isn't It Scaling]], [[16 - Alternatives & Trade-offs]]

> [!warning] Written against KEDA v2.20.1 · Kubernetes 1.33–1.35
> Chart values, RBAC and CRD fields all move between minor releases. Verify against [the deploy page](https://keda.sh/docs/2.20/deploy/) and [the chart values](https://github.com/kedacore/charts/tree/v2.20.1/keda) for the version you actually run.

---

## 1. Installing for real

Three supported paths. Pick one and stick to it — mixing them is how you end up with orphaned CRDs.

### Helm, with the version pinned

```bash
helm repo add kedacore https://kedacore.github.io/charts
helm repo update

helm install keda kedacore/keda \
  --namespace keda --create-namespace \
  --version 2.20.1 \
  -f keda-values.yaml
```

Two things in that command are the whole point:

- **`--version 2.20.1`.** Without it, Helm resolves to whatever is newest in the repo *at the moment you run it*, which means the same command produces different KEDA versions in dev and prod. Given that KEDA ships breaking changes in minor releases (§2), that's not a style preference.
- **`-f keda-values.yaml`, committed to git.** KEDA has a lot of meaningful values (Prometheus, `watchNamespace`, resource requests, Secret permissions). Every one of them set with `--set` is a value you will forget on the next upgrade.

Verify the install landed — three Deployments, six CRDs, one APIService:

```bash
kubectl -n keda get deploy
kubectl get crd | grep keda.sh
kubectl get apiservice v1beta1.external.metrics.k8s.io   # must be AVAILABLE=True
```

If the APIService isn't `Available`, nothing works and no other check means anything ([[14 - Observability & Debugging — Why Isn't It Scaling]]).

> [!tip] The component names differ between Helm and the raw manifests
> Helm gives you `keda-operator`, `keda-operator-metrics-apiserver` and `keda-admission-webhooks`. The released YAML manifests give you `keda-operator`, **`keda-metrics-apiserver`** and **`keda-admission`**. Runbooks and dashboards written against one installation method quietly break on the other — a real problem when someone migrates from manifests to Helm.

### Raw manifests

```bash
kubectl apply --server-side \
  -f https://github.com/kedacore/keda/releases/download/v2.20.1/keda-2.20.1.yaml
```

`--server-side` is required, not optional: the CRDs are too large for client-side apply. Each release publishes **three flavours**:

| Asset | Contents | Use it when |
|---|---|---|
| `keda-<ver>.yaml` | everything, **including** the admission webhooks | the default choice for a manifest install |
| `keda-<ver>-core.yaml` | everything **except** the admission webhooks | webhooks conflict with your policy stack, or you can't run another webhook in the API request path |
| `keda-<ver>-crds.yaml` | the six CRDs only | you manage CRDs separately from the workloads (strongly recommended — see §3) |

Note the trade-off in dropping the webhooks: you lose the validations that reject two ScaledObjects on the same workload, a foreign HPA on your target, and out-of-range replica bounds. Those become runtime confusion instead of an `apply`-time error.

### OperatorHub / OLM

A community wrapper in a separate repository, distributed through OperatorHub on the `stable` channel. It works differently from the other two in a way that catches people: installing the operator deploys **nothing**. You must then create a `KedaController` custom resource **named `keda`** in KEDA's namespace — only that name is reconciled, and there must be exactly one per cluster. On the way out, delete the `KedaController` *first*.

### Choosing

| Method | Choose it when | Watch out for |
|---|---|---|
| **Helm, pinned** | you have any kind of GitOps or release process; you want to tune values | the CRD/uninstall behaviour in §3 |
| **Raw manifests** | you want zero templating, byte-reviewable YAML, or an air-gapped mirror | no values to tune — you patch after apply; upgrades are apply-over-apply |
| **OLM / OperatorHub** | you already run OLM (typically OpenShift) and want its lifecycle model | community-maintained wrapper, a version behind at times; the `KedaController` CR step |

> [!tip] Committed recommendation
> **Helm with a pinned `--version`, a committed values file, and `crds.install=false` with the CRDs applied separately from the `-crds.yaml` asset.** That combination gives you tunability, reproducibility, and immunity to the uninstall footgun in §3.

---

## 2. Version and upgrade discipline — the most important section

Three facts about KEDA's release model determine how you operate it. None of them is obvious from the docs' front page.

**1. Releases land roughly every four months.** The project targets January, May and September, and slips by a few weeks in practice. So you will do two to three KEDA upgrades a year whether you plan for them or not.

**2. The supported Kubernetes window is about three minors — "N-2".** KEDA v2.20 is tested against Kubernetes v1.33–v1.35. Since managed Kubernetes providers force minor upgrades on their own schedule, this window moves underneath you: a cluster upgrade can push you *out* of your pinned KEDA version's tested range without you touching KEDA at all. Nothing breaks loudly when that happens, which is exactly the problem.

**3. The CRDs are `v1alpha1` and are staying there — deliberately.** The maintainers have declined to graduate the API, on the grounds that maintaining multiple API versions is overhead a small project can't carry. The consequence is the single most important operational fact about KEDA:

> [!important] Breaking changes ship in **minor** releases
> Because the API never graduated past `v1alpha1`, KEDA is not bound by Kubernetes' API deprecation policy. Removals happen under the project's own **two-release deprecation window** — a field is announced deprecated, then removed roughly two releases later. Recent minors have each removed previously-valid fields: v2.18 removed the CPU/memory scaler's `type` (use `metricType`) and IBM MQ's `tls`; v2.19 removed NATS Streaming support; v2.20 removed GCP Pub/Sub's `subscriptionSize`, Huawei Cloudeye's `minMetricValue`, InfluxDB's `authToken` in trigger metadata, and dropped `authModes` from several scalers.
> A "patch-level" mental model imported from other operators — "2.19 to 2.20 is routine" — is wrong here. Treat every minor as a potentially breaking upgrade.

### The upgrade checklist

Run this every time. It takes twenty minutes and it is the difference between a boring upgrade and an outage.

1. **Confirm what you're on, and pin what you're going to.**
   ```bash
   helm -n keda list
   helm search repo kedacore/keda --versions | head
   ```
2. **Read the release notes for *removals*, not features.** Go to the GitHub release for the target version and read the **Breaking Changes** section and the **Deprecations** section — then read the rest of the notes anyway, because the most disruptive change in v2.20 was *not* in the Breaking Changes section (see the RBAC example below). There are no release blog posts for recent versions; GitHub Releases and the CHANGELOG are the record.
3. **Check the Kubernetes compatibility window** for the target version against your actual cluster version, including where your cluster will be in three months.
4. **Grep your own manifests for every removed field.** Removals are per-scaler, so this is a search across all your ScaledObjects — not just the ones the release notes' examples mention.
   ```bash
   kubectl get scaledobjects,scaledjobs -A -o yaml > all-keda-crs.yaml
   grep -n 'subscriptionSize\|minMetricValue\|authModes' all-keda-crs.yaml
   ```
5. **Upgrade a non-production cluster first**, then diff behaviour: are all ScaledObjects still `Ready`? Are replica counts stable? Any new events?
   ```bash
   helm upgrade keda kedacore/keda -n keda --version 2.21.0 -f keda-values.yaml
   kubectl -n keda rollout status deploy/keda-operator
   kubectl get so -A     # every one still READY=True?
   ```
6. **Watch for newly-*rejected* resources.** New validation rules apply on `CREATE` and `UPDATE`, so existing objects keep running after an upgrade and only fail the next time someone edits them — which may be weeks later, in the middle of an unrelated change. v2.20 added a hard rejection for ScaledObject names over 63 characters (and for derived HPA names over 63, which caps the ScaledObject name at 54 in practice). A name that was fine in v2.19 now fails on re-apply. Dry-run your whole inventory after upgrading:
   ```bash
   kubectl apply --dry-run=server -f all-keda-crs.yaml
   ```

> [!warning] Gotcha — v2.20 moved event recording to `events.k8s.io`, and tightened RBAC breaks silently
> With the Kubernetes 0.35 dependency bump, KEDA records Kubernetes Events through the `events.k8s.io` API group instead of the legacy core `events` resource. The bundled manifests and Helm chart already grant the new permission — but **if you replaced KEDA's RBAC with your own restricted role**, event recording fails after the upgrade. Nothing crashes: scaling still works, and you simply stop getting `KEDAScaleTargetActivated` / `KEDAScaleTargetDeactivated` events, which is precisely what you need during the *next* incident. It is also easy to misdiagnose as "KEDA stopped scaling" when someone checks events first.
> **Mitigation:** grant this **before** upgrading, not after:
> ```yaml
> - apiGroups: ["events.k8s.io"]
>   resources: ["events"]
>   verbs: ["create", "patch"]
> ```
> More generally: if you maintain custom RBAC for KEDA, diff the chart's rendered ClusterRole against yours on every upgrade — `helm template` makes that a one-liner.

> [!warning] Gotcha — `--reuse-values` silently skips new chart defaults
> `helm upgrade --reuse-values` carries your old values forward but does **not** merge in values the new chart version introduced. New security defaults, new flags and new sub-trees are simply absent, and you get a subtly different deployment from a fresh install of the same version.
> **Mitigation:** always upgrade with `-f keda-values.yaml` from git. If you must use `--reuse-values`, diff `helm get values keda -n keda` against the new chart's `values.yaml` first.

---

## 3. Uninstalling — the one that takes production down

> [!warning] Gotcha — `helm uninstall keda` DELETES KEDA's CRDs, which cascades to every ScaledObject in the cluster
> KEDA's chart ships its CRDs as **ordinary templates** (not in Helm's special `crds/` directory) and none of them carries a keep-resource policy. So `helm uninstall keda` removes all six CRDs, and Kubernetes garbage-collects every custom resource of those kinds — **every `ScaledObject`, `ScaledJob`, `TriggerAuthentication`, `ClusterTriggerAuthentication` and `CloudEventSource` in the entire cluster, in every namespace, belonging to every team.** The docs describe uninstall as removing KEDA "while retaining your configuration files", which reads as reassurance and is not what happens to the cluster state. And because the CRDs are templates, **`helm uninstall --skip-crds` and `helm install --skip-crds` have no effect at all** — Helm's CRD flags only apply to the `crds/` directory the chart doesn't use.
> **Mitigation, three parts:**
> 1. **Back up every KEDA CR before touching the release.** This takes seconds and there is no excuse:
>    ```bash
>    kubectl get scaledobjects,scaledjobs,triggerauthentications -A -o yaml > keda-crs-backup.yaml
>    kubectl get clustertriggerauthentications,cloudeventsources,clustercloudeventsources -o yaml >> keda-crs-backup.yaml
>    ```
> 2. **Separate CRD lifecycle from release lifecycle.** Install with `--set crds.install=false` and apply the CRDs yourself from the release's `keda-<ver>-crds.yaml` asset. Then Helm structurally *cannot* delete them, and an accidental `helm uninstall` costs you a scaling outage instead of a data-loss event.
> 3. **Know the aftermath.** Deleting a ScaledObject does not restore the workload's replica count unless `advanced.restoreToOriginalReplicaCount: true` was set — so a workload sitting at 0 replicas when the CRDs vanish **stays at 0**, with no controller left to bring it back. Scale-to-zero workloads are the ones that break hardest here.

Other uninstall details worth knowing: the `keda` namespace survives and must be deleted separately; a CR stuck in `Terminating` is a finalizer issue, cleared with `kubectl patch scaledobject <name> -p '{"metadata":{"finalizers":null}}' --type=merge`; a manifest install must be removed with the **same** flavour used to install it; and under OLM you delete the `KedaController` CR first.

---

## 4. High availability, honestly

> [!important] KEDA's own words: "KEDA does not provide full support for high-availability due to upstream limitations."
> Take that literally. It is not marketing caution.

What you actually get:

| Component | Supported replicas | What extra replicas buy you |
|---|---|---|
| **Operator** | 2 | **Faster failover only.** Leader election means exactly one replica is active; the standby does nothing until the lease expires. |
| **Metrics server (adapter)** | 1 | Nothing — this is the supported count. |
| **Admission webhooks** | more than 1 is fine | genuine availability; it's a stateless request-path webhook |

> [!warning] Gotcha — operator replicas do not add throughput
> Running `operator.replicaCount: 2` looks like horizontal scaling and isn't. The standby holds no scalers, polls nothing, and serves no metric requests. If your operator is CPU-bound from polling hundreds of ScaledObjects, a second replica changes nothing at all — you need bigger resource limits and the levers in §8. Worse, capacity plans built on "we run 2 replicas so we have 2× headroom" are wrong in a way that only shows up under load.
> **Mitigation:** run 2 operator replicas for **failover time**, and size the *single active* replica for your full load. Add a PodDisruptionBudget and anti-affinity so a node drain doesn't take both. Treat the metrics server's single replica as a known, accepted single point of failure — during its restart the HPAs briefly can't read metrics, which surfaces as `<unknown>` targets and a frozen replica count, not as a scale-to-zero event.

The reason the metrics server can't be scaled out is structural, and it's the same reason KEDA is a cluster singleton: Kubernetes permits exactly one provider of the `external.metrics.k8s.io` API group per cluster. (If you do run multiple metrics-server replicas behind the service, the API server needs aggregator routing enabled to reach them — which is a caveat, not a supported configuration.)

---

## 5. Multi-tenancy and scoping

The constraint that shapes everything here, verbatim from KEDA's FAQ:

> "Kubernetes currently only supports one metric server serving `external.metrics.k8s.io` metrics per cluster."

So **KEDA is a shared, cluster-level component by construction.** You cannot give each team its own KEDA in one cluster — the second install's metrics server will fight the first for the API group. What you *can* do is narrow which resources one KEDA install pays attention to.

### Namespace scoping

```yaml
# keda-values.yaml
watchNamespace: "orders,payments,search"    # empty (default) = all namespaces
```

This sets the operator's watch scope. Resources outside the list are ignored — completely and silently. That silence is the trap:

> [!warning] Gotcha — a ScaledObject in an unwatched namespace does nothing and says nothing
> With `watchNamespace` set, a ScaledObject created in an excluded namespace is accepted by the API server, sits there with empty status conditions, gets no HPA, produces no events, and generates no error anywhere. To the team that created it, KEDA is broken. To the platform team, KEDA is healthy. Both are right. Every new namespace onboarding then requires a KEDA values change and a rollout, which is exactly the sort of coupling nobody documents.
> **Mitigation:** prefer `watchNamespace: ""` (all namespaces) unless you have a concrete reason, and control access with RBAC on the CRDs instead. If you do scope it, automate the list from the same source of truth that creates namespaces, and make "is your namespace in KEDA's watch list?" the first line of your platform runbook.

Also note the blast radius that comes with a shared install: the admission webhook, the operator's reconcile budget, its polling load, and its client-side API rate limits are all **shared across tenants**. One team creating a thousand ScaledObjects with a 5-second `pollingInterval` degrades everyone's scaling latency.

### One cluster-wide KEDA versus per-tenant clusters

| Model | Choose it when | Cost |
|---|---|---|
| **One cluster-wide KEDA** (`watchNamespace: ""`) | almost always — it's the model KEDA is designed for | shared blast radius; upgrades are a cluster-wide event needing cross-team coordination |
| **Namespace-scoped install** (`watchNamespace: "a,b"`) | you are rolling KEDA out incrementally, or a strict subset of namespaces is allowed to autoscale | silent no-ops outside the list; onboarding needs a KEDA change; still only one install per cluster |
| **Per-tenant clusters** | hard tenancy boundaries, independent upgrade cadences, or regulatory separation | a cluster per tenant, and a KEDA upgrade per tenant; only sane when you already run multi-cluster |

---

## 6. Security posture

The KEDA operator resolves the credentials your triggers need, which means it reads the Secrets your `TriggerAuthentication` objects point at. By default the chart grants it broad Secret access so it can do that anywhere in the cluster.

That's a real blast radius: a component with cluster-wide Secret read is a high-value target, and KEDA's Secret access exists purely to hand credentials to scalers. Two chart values let you narrow it:

```yaml
permissions:
  operator:
    restrict:
      secret: true       # narrows the operator's Secret access (sets KEDA_RESTRICT_SECRET_ACCESS)
  metricServer:
    restrict:
      secret: true
```

> [!warning] Gotcha — restricting Secret access breaks trigger auths that reference Secrets elsewhere
> Turning the restriction on is the right security posture and it *will* change behaviour: a `TriggerAuthentication` in a tenant namespace pointing at a Secret the operator can no longer read stops resolving, and the affected ScaledObject goes not-`Ready` with a trigger error. The failure appears at reconcile time in an unrelated namespace, so it is easy to attribute to the tenant's config rather than to your hardening change.
> **Mitigation:** enable it in staging first and inventory every `TriggerAuthentication` in the cluster before rolling it to production. Where you need shared credentials, move them behind a `ClusterTriggerAuthentication` with the Secret in KEDA's own namespace, or — better — out of Kubernetes Secrets entirely and into an external store (Vault, AWS Secrets Manager, Azure Key Vault, GCP Secret Manager) or workload identity, all of which KEDA supports. See [[12 - Authentication — TriggerAuthentication, SASL, TLS & MSK IAM]].

Two more levers worth setting deliberately:

- **Which kinds KEDA may scale.** The chart's RBAC defaults to allowing KEDA to write the `scale` subresource on any API group and kind. If your tenants only ever scale Deployments and StatefulSets, narrow the allowed `scaledRefKinds` list. It costs nothing and removes "KEDA can resize arbitrary custom resources" from your threat model.
- **Webhook failure policy.** The admission webhook ships with a failure policy of `Ignore`, which means validation is **best-effort**: if the webhook pod is down or times out, invalid ScaledObjects are admitted. Set it to `Fail` if you want the validations (two ScaledObjects on one workload, foreign HPA ownership, name length, replica bounds) to be guarantees rather than suggestions — at the cost of making KEDA's webhook a dependency of applying its own CRs.

The pod-level defaults are already sensible: non-root, read-only root filesystem, all capabilities dropped, `RuntimeDefault` seccomp. Don't loosen them.

---

## 7. KEDA and node autoscaling — the gap nobody mentions

Here is the sentence that should be on the front page of every KEDA tutorial and isn't:

> [!important] KEDA scales **pods**. If there is no room for those pods, nothing gets processed.
> KEDA sees lag, reports a metric, the HPA asks for 20 replicas, and the Deployment dutifully creates 20 Pods. Fourteen of them go `Pending` because no node has capacity. Your backlog keeps growing at full rate while your dashboards show `Ready=True`, `Active=True`, a healthy metric, and "20 replicas" — because 20 pods *were* requested. Nothing in KEDA is broken and nothing in KEDA will tell you.

Diagnosis is one command, and it belongs in your drill ([[14 - Observability & Debugging — Why Isn't It Scaling]]):

```bash
kubectl -n lab get pods                  # compare Running against the replica count
kubectl -n lab describe pod <pending>    # Events: FailedScheduling — Insufficient cpu / memory
```

### The latency nobody budgets for

Between "KEDA decided" and "work is being processed" sit several serial delays:

```text
poll interval        →  up to pollingInterval (default 30 s)
HPA sync + policy    →  seconds, plus any stabilisation window you configured
scheduling attempt   →  immediate, and it FAILS: no capacity
node autoscaler sees the Pending pod  →  seconds
cloud provider creates the instance   →  tens of seconds to minutes
node joins, CNI ready, image pulled   →  tens of seconds (image size dominates)
pod starts, consumer joins group      →  plus a Kafka rebalance
```

Realistically that's minutes, not seconds — and it's *serial* with everything KEDA does. Tuning `pollingInterval` down to 5 seconds to "react faster" is pointless if node provisioning takes two minutes.

### Cluster Autoscaler and Karpenter

Both work with KEDA, and both are triggered by the same thing: **unschedulable Pods**. Cluster Autoscaler adds nodes to node groups you predefined; Karpenter provisions instances directly from the Pending pods' requirements, which usually makes it faster and better-fitting. Neither knows anything about Kafka lag, and neither acts until a Pod is already Pending — which is the whole source of the latency above.

**Mitigations, in order of how much they help:**

1. **Headroom via over-provisioning.** Run low-priority "pause" pods that reserve real capacity and do nothing. When your consumer scales up, the scheduler evicts them instantly and your real pods start on already-warm nodes, while the node autoscaler replaces the headroom in the background. This converts node-provisioning latency into scheduling latency and is the single most effective fix.
2. **Right-size requests.** Oversized CPU/memory requests are the most common reason a scale-up needs a new node at all. Fixing requests often removes the problem entirely.
3. **Shrink your images and pre-pull.** Image pull is frequently the largest single term in node-ready time.
4. **Cap `maxReplicaCount` at something your cluster can actually host** — for Kafka, the partition count is already your natural ceiling ([[07 - The Kafka Scaler — Every Field & the Replica Math]]). Asking for replicas you can never schedule just fills the cluster with Pending pods.
5. **Set `spec.fallback`** so a scaling decision made on stale data doesn't compound the problem ([[11 - Multiple Triggers, Scaling Modifiers & Fallback]]).

### Scale-to-zero meets node consolidation

Scale-to-zero and node autoscaling interact in a way that's easy to like and easy to get wrong. When KEDA takes a workload to zero, the node that hosted it becomes empty, and both Cluster Autoscaler and Karpenter will remove it — which is exactly the cost saving you wanted. But it also means the *next* activation pays the full node-provisioning cost, so a workload that idles at zero and wakes every ten minutes may spend most of its wake-up latency waiting for an instance. And consolidation cuts the other way too: Karpenter can decide to consolidate a node your consumer is happily running on, killing pods mid-batch and triggering a Kafka rebalance for reasons that have nothing to do with load.

**Mitigation:** protect running consumers with a `PodDisruptionBudget`, and for genuinely disruption-sensitive consumers use Karpenter's `karpenter.sh/do-not-disrupt: "true"` annotation. Make sure `terminationGracePeriodSeconds` exceeds your worst-case batch time, and handle `SIGTERM` to commit before exiting — a consolidation event and a scale-down look identical to your consumer, so the same graceful-shutdown work covers both. For workloads where wake-up latency matters more than cost, use `minReplicaCount: 1` and keep one warm pod instead of chasing zero.

> [!tip] Complementary layers, not competitors
> KEDA answers "how many pods should exist, given the backlog." The node autoscaler answers "is there somewhere to put them." A production event-driven platform needs both, plus correct resource requests to connect them. An interview answer that treats KEDA and Karpenter as alternatives has missed the point; they operate on different objects. Broader comparisons live in [[16 - Alternatives & Trade-offs]].

---

## 8. Operating at scale

What grows as your ScaledObject count grows:

- **Polling load on your event sources.** Every trigger is polled every `pollingInterval` — and, unless you cache, the event source is *also* hit on each HPA sync (roughly every 15 seconds). Five hundred Kafka triggers at `pollingInterval: 10` is a lot of metadata and offset-fetch traffic aimed at your brokers, from a single pod.
- **Reconcile work in the operator.** More CRs, more watches, more status writes, more HPA updates. The operator is a single active replica (§4), so this is vertical.
- **Kubernetes API load.** Status updates and HPA writes flow through KEDA's client-side rate limits, which are modest by default. When the operator is heavily loaded you see scaling *latency*, not errors — decisions arrive late, which for a bursty topic is indistinguishable from not scaling.
- **Admission webhook memory.** The webhook validates on every create and update; v2.20 specifically fixed an out-of-memory condition that appeared at very large ScaledObject counts (tens of thousands). If you're in that territory, be on v2.20 or later and give the webhook real memory limits.

### The levers

| Lever | Effect | Cost |
|---|---|---|
| **Raise `pollingInterval`** | linearly fewer queries to the event source | slower reaction, and slower 0→1 activation — it's the only knob for that transition |
| **`useCachedMetrics: true`** (per trigger) | the event source is hit at `pollingInterval` cadence *only*, not additionally on every HPA sync | the HPA acts on data up to `pollingInterval` old; not supported for `cpu`, `memory` or `cron` triggers |
| **Raise KEDA's resource requests/limits** | the operator stops being CPU-throttled | defaults are modest (requests `100m`/`100Mi`, limits `1` CPU/`1000Mi` per component) — raise them deliberately |
| **Raise the operator's Kubernetes client QPS/burst and reconcile concurrency** | fewer late status updates and HPA writes | more load on your API server; measure before and after |
| **Fewer, coarser ScaledObjects** | less of everything | less per-workload control |

> [!tip] Best practice — `useCachedMetrics` is the fix for "KEDA is hammering my brokers"
> This is the single highest-leverage value in this section, and it's per-trigger rather than global, so you can enable it on the noisy high-cardinality triggers and leave latency-sensitive ones alone. It changes *where* the HPA's number comes from (KEDA's cache instead of a live query), not what the number means.

And watch KEDA's own error metrics as a capacity signal: a rising `rate(keda_scaler_detail_errors_total[5m])` under load usually means timeouts against your event source, which means you are polling it harder than it wants ([[14 - Observability & Debugging — Why Isn't It Scaling]]).

---

## 9. The gotchas that cause real incidents

Collected, including the ones detailed above — this is the list to review before you put KEDA in front of revenue.

> [!warning] Unpinned KEDA breaks on a routine minor upgrade
> `helm install kedacore/keda` without `--version` resolves to whatever is newest today. Because breaking changes ship in minors, a chart refresh months later can remove a field your ScaledObjects use and your workloads stop scaling — with no correlation to any deploy you made.
> **Mitigation:** pin `--version`, keep values in git, and run the §2 checklist per upgrade. Pin in CI too, so nobody's `helm repo update` becomes a production change.

> [!warning] `helm uninstall keda` destroys every KEDA CR in the cluster
> The CRDs are ordinary chart templates with no keep policy, so uninstalling cascades to all ScaledObjects, ScaledJobs and TriggerAuthentications cluster-wide. Helm's `--skip-crds` does not protect you.
> **Mitigation:** back up all CRs first; install with `crds.install=false` and manage the CRDs from the `-crds.yaml` asset separately. See §3.

> [!warning] Assuming operator replicas add throughput
> Only one operator replica is active. A second cuts failover time and adds zero capacity.
> **Mitigation:** size the single active replica for full load; use the second for failover, with a PDB and anti-affinity.

> [!warning] Pods `Pending` because nodes never arrive
> KEDA and the HPA both consider their job done once the replica count is set. Backlog grows while pods wait for capacity, and every KEDA-side signal looks healthy.
> **Mitigation:** run a node autoscaler, keep over-provisioning headroom, right-size requests, and put "compare Running pods to the replica count" in your debugging drill.

> [!warning] Installing a second external-metrics adapter
> Kubernetes serves `external.metrics.k8s.io` from exactly one provider. Adding prometheus-adapter with external-metrics enabled alongside KEDA means one of them wins the APIService and the other's metrics vanish — symptom: `<unknown>` in HPA targets across the cluster, for reasons that appear unrelated to whatever was just installed.
> **Mitigation:** pick one external-metrics provider per cluster. If you need Prometheus queries to drive scaling, use KEDA's Prometheus **scaler** rather than a second adapter.

> [!warning] Deleting a ScaledObject leaves the workload frozen at its current replica count
> Replicas are not restored on delete unless `advanced.restoreToOriginalReplicaCount: true` was set. Delete one while it sits at 0 and the workload **stays at 0** with no controller to wake it — a silent outage with no failing pod to page on.
> **Mitigation:** set `restoreToOriginalReplicaCount: true` where a sane resting count exists, and make "check the target's replica count" the step after any ScaledObject deletion. Never delete a ScaledObject for a scaled-to-zero workload without immediately scaling the Deployment by hand.

> [!warning] Custom RBAC drifting from the chart's
> KEDA's required permissions change between minors (§2's `events.k8s.io` example). A hand-maintained role rots silently, and the symptoms are partial — scaling works, events or status updates don't.
> **Mitigation:** if you must customise RBAC, diff `helm template` output against your role on every upgrade and treat the diff as a required review item.

---

## 10. Alternatives — deployment models

| Model | How it works | Choose it when | Don't, when |
|---|---|---|---|
| **One cluster-wide KEDA** | single install, `watchNamespace: ""`, serves every namespace | the default, and what KEDA is designed for; you have a platform team owning upgrades | tenants need independent KEDA versions — impossible in one cluster anyway |
| **Namespace-scoped install** | one install, `watchNamespace` set to a list | incremental rollout, or only some namespaces may autoscale | you have frequent namespace churn — every new namespace needs a KEDA rollout, and resources outside the list fail silently |
| **Per-tenant clusters** | one KEDA per cluster, one cluster per tenant | hard tenancy or regulatory boundaries; genuinely independent upgrade cadences | you don't already run multi-cluster — you've multiplied your upgrade surface to solve a scoping problem |

The honest summary: **one cluster-wide install is the right answer for almost everyone**, because the one-external-metrics-provider-per-cluster rule means the alternatives are "narrow what the single install sees" or "buy more clusters". Scope with RBAC on the CRDs, not with `watchNamespace`, unless you have a specific reason.

> [!tip] Try it
> [[17 - LAB 4 — Production-Grade Kafka Autoscaling Capstone]] assembles a pinned install, a values file, fallback, alerting and a deliberately under-provisioned node pool — so you can watch pods go `Pending` while every KEDA signal stays green. That failure is much more convincing at 3 p.m. on your laptop than at 3 a.m. in production.

---

## 11. Questions you should be able to answer

- Why is pinning KEDA's version non-negotiable? *(The CRDs are permanently `v1alpha1` by maintainer decision, so breaking changes ship in minor releases under a two-release deprecation window.)*
- What does `helm uninstall keda` destroy? *(The CRDs — and with them every ScaledObject, ScaledJob and TriggerAuthentication in the cluster. `--skip-crds` doesn't help; the CRDs are ordinary templates.)*
- How many operator replicas should you run, and what do you get? *(Two, for faster failover. Only one is active — extra replicas add no throughput. The metrics server supports one replica.)*
- Why can't each team have its own KEDA in a shared cluster? *(Kubernetes allows exactly one provider of `external.metrics.k8s.io` per cluster, so KEDA is a cluster singleton.)*
- Your ScaledObject is `Ready` and `Active`, the metric is flowing, the HPA says 20 replicas — and lag is still growing. What's wrong? *(Pods are `Pending`. KEDA scales pods, not nodes; there's no capacity.)*
- Are KEDA and Karpenter alternatives? *(No — complementary layers. KEDA decides how many pods; the node autoscaler finds somewhere to put them, and only reacts once pods are already Pending.)*
- What breaks after upgrading to v2.20 with a custom restricted RBAC role? *(Event recording — KEDA moved to `events.k8s.io`; grant `create`/`patch` on `events.k8s.io/events` before upgrading.)*
- You delete a ScaledObject that was scaled to zero. What happens to the workload? *(It stays at 0 forever — replicas are only restored if `restoreToOriginalReplicaCount: true` was set.)*
- KEDA is hammering your Kafka brokers with metadata requests. Cheapest fix? *(`useCachedMetrics: true` on the trigger — the event source is then hit only at `pollingInterval`, not additionally on every HPA sync.)*
- Kubernetes compatibility window for KEDA v2.20? *(v1.33–v1.35 — roughly three minors, "N-2". Your managed-cluster upgrades move you through it whether you touch KEDA or not.)*

---

## 12. Sources

- [KEDA — Deploying KEDA (Helm, manifests, OLM)](https://keda.sh/docs/2.20/deploy/)
- [KEDA — Cluster operations: compatibility, HA, firewall, env vars](https://keda.sh/docs/2.20/operate/cluster/)
- [KEDA — FAQ (one external metrics server per cluster)](https://keda.sh/docs/2.20/reference/faq/)
- [KEDA — Admission webhooks](https://keda.sh/docs/2.20/operate/admission-webhooks/)
- [KEDA — ScaledObject specification](https://keda.sh/docs/2.20/reference/scaledobject-spec/)
- [KEDA — CHANGELOG (breaking changes and deprecations per release)](https://github.com/kedacore/keda/blob/main/CHANGELOG.md)
- [KEDA — GitHub Releases (the record for recent versions; there are no release blog posts)](https://github.com/kedacore/keda/releases)
- [KEDA — ROADMAP and release cadence](https://github.com/kedacore/keda/blob/main/ROADMAP.md)
- [KEDA governance — SUPPORT.md (the Kubernetes "N-2" window)](https://github.com/kedacore/governance/blob/main/SUPPORT.md)
- [KEDA governance — DEPRECATIONS.md (the two-release window)](https://github.com/kedacore/governance/blob/main/DEPRECATIONS.md)
- [KEDA Helm chart and values](https://github.com/kedacore/charts/tree/v2.20.1/keda)
- [KEDA on OperatorHub (community OLM wrapper)](https://operatorhub.io/operator/keda)
- [Kubernetes — Cluster Autoscaler](https://github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler)
- [Karpenter — disruption and consolidation](https://karpenter.sh/docs/concepts/disruption/)
- [Kubernetes — Pod priority, preemption and over-provisioning](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-priority-preemption/)
- [Kubernetes — Pod lifecycle and graceful termination](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/)
