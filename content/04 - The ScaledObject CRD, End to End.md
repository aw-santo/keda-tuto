---
title: The ScaledObject CRD, End to End
tags:
  - keda
  - kubernetes
  - autoscaling
  - crd
  - reference
  - senior
aliases:
  - ScaledObject
  - ScaledObject spec
  - minReplicaCount
  - idleReplicaCount
  - useCachedMetrics
status: ready
created: 2026-07-25
---

# The ScaledObject CRD, End to End

> [!abstract] Scope
> The reference module: every field of a `ScaledObject`, in plain language, with its **real default** — plus how to read its status, the annotations you'll reach for during an incident, and exactly what the admission webhook will refuse to accept. Assumes you know from [[03 - KEDA Architecture — Operator, Metrics Adapter & the HPA It Owns]] that a `ScaledObject` is really a *recipe for an HPA plus a polling loop*, because half these fields configure KEDA and the other half are passed straight through to the HPA, and knowing which is which is the whole skill.

Related: [[00 - KEDA Index]], [[03 - KEDA Architecture — Operator, Metrics Adapter & the HPA It Owns]], [[07 - The Kafka Scaler — Every Field & the Replica Math]], [[08 - Scale-to-Zero & Activation]], [[10 - Tuning Scaling Behaviour]], [[11 - Multiple Triggers, Scaling Modifiers & Fallback]], [[12 - Authentication — TriggerAuthentication, SASL, TLS & MSK IAM]], [[13 - ScaledJob — When a Pod per Message Beats a Consumer Pool]], [[14 - Observability & Debugging — Why Isn't It Scaling]]

> [!warning] Written against KEDA v2.20 · Kubernetes 1.33–1.35
> The CRDs are `v1alpha1` and the maintainers have said they will stay there, so **fields get removed in minor releases** under a two-release deprecation window. Every default below is current for v2.20 — re-check against the [ScaledObject spec](https://keda.sh/docs/2.20/reference/scaledobject-spec/) for the version you actually run.

---

## 1. The whole thing at once

Everything except `scaleTargetRef` and `triggers` is optional. Here is a fully-populated example to use as a map — the sections that follow walk it in groups.

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: orders-consumer            # ≤ 54 chars — see §10
  namespace: prod                  # must be the workload's namespace
spec:
  scaleTargetRef:                  # REQUIRED — what to scale
    name: orders-consumer          #   required
    apiVersion: apps/v1            #   default apps/v1
    kind: Deployment               #   default Deployment
    envSourceContainerName: worker #   default: the first container

  minReplicaCount: 0               # default 0
  maxReplicaCount: 12              # default 100
  idleReplicaCount: 0              # unset = disabled; only 0 is supported

  pollingInterval: 30              # seconds, default 30
  cooldownPeriod: 300              # seconds, default 300 — ONLY the final step to zero
  initialCooldownPeriod: 0         # seconds, default 0

  advanced:
    restoreToOriginalReplicaCount: true    # default FALSE — see §11
    horizontalPodAutoscalerConfig:
      name: keda-hpa-orders-consumer       # default keda-hpa-<so name>
      behavior:                            # passed straight through to the HPA
        scaleUp:
          stabilizationWindowSeconds: 60

  triggers:                        # REQUIRED, at least one
  - type: kafka
    name: orders-lag               # optional, must be unique within the ScaledObject
    metricType: AverageValue       # default AverageValue
    useCachedMetrics: true         # default false
    metadata:
      bootstrapServers: kafka-bootstrap.kafka:9092
      consumerGroup: orders-processor
      topic: orders
      lagThreshold: "500"
    authenticationRef:
      name: kafka-scram-auth
      kind: TriggerAuthentication  # or ClusterTriggerAuthentication

  fallback:                        # what to do when a scaler errors
    failureThreshold: 3
    replicas: 4
    behavior: currentReplicasIfHigher
```

The short name is `so`, so `kubectl get so` works everywhere below.

---

## 2. `scaleTargetRef` — what gets scaled

| Field | Default | Notes |
|---|---|---|
| `name` | **required** | the workload's name, in the same namespace as the `ScaledObject` |
| `apiVersion` | `apps/v1` | |
| `kind` | `Deployment` | |
| `envSourceContainerName` | the first container | which container's env vars a trigger may read from |

The `ScaledObject` must live in the **same namespace as its target**. There is no cross-namespace scaling.

**It works on anything with a `scale` subresource**, not just Deployments — StatefulSets, Argo Rollouts, Flink deployments, your own CRD, anything that exposes `/scale`. Just name the group:

```yaml
  scaleTargetRef:
    apiVersion: apps/v1
    kind: StatefulSet
    name: orders-stateful
```

> [!warning] Gotcha — scaling a custom resource fails on RBAC, not on KEDA
> KEDA can scale any `/scale`-capable resource, but its ServiceAccount has to be *allowed* to read and write that resource's scale subresource. Point a `ScaledObject` at a custom kind and you'll get permission errors in the operator log rather than a validation failure at apply time, so it looks like KEDA doesn't support the kind at all.
> **Mitigation:** grant the operator's role access to that API group's `scale` subresource. The Helm chart has a values entry for exactly this (`rbac.scaledRefKinds`, which by default is permissive); if you tightened it, that's your first suspect.

**`envSourceContainerName`** is only relevant when a trigger's metadata uses the `FromEnv` form to read a value out of your pod's environment (e.g. `bootstrapServersFromEnv`). It tells KEDA which container's env to read. Multi-container pods where the sidecar is listed first are the reason this field exists.

---

## 3. Replica bounds — and the idle/min distinction

| Field | Default | Notes |
|---|---|---|
| `minReplicaCount` | **`0`** | the floor while triggers are active. 0 means scale-to-zero is on |
| `maxReplicaCount` | **`100`** | the ceiling |
| `idleReplicaCount` | unset (disabled) | **only `0` is supported**, and it must be **less than** `minReplicaCount` |

Two things worth pinning down.

**The default `maxReplicaCount` is 100, and it is rarely what you want.** Omit it and a runaway backlog can march your Kafka consumer to 100 pods on a 12-partition topic, where 88 of them will sit idle burning quota ([[07 - The Kafka Scaler — Every Field & the Replica Math]]). Always state it.

**`minReplicaCount: 0` means the generated HPA gets `minReplicas: 1`.** That's not a contradiction — it's how KEDA crosses zero. See [[03 - KEDA Architecture — Operator, Metrics Adapter & the HPA It Owns]] §5.

**Idle versus min.** With just `minReplicaCount: 0` you scale down to zero and, on the first event, up to *one* — then the HPA works its way up from there over several sync cycles. `idleReplicaCount` decouples "the count when there's nothing to do" from "the count when there is":

```yaml
  idleReplicaCount: 0      # nothing to do → run nothing
  minReplicaCount: 5       # something to do → jump straight to 5
```

That is the config for a workload with an expensive cold start — a JVM consumer that takes 40 seconds to be useful, say. Instead of one pod slowly clearing a backlog while the HPA ramps, you get five immediately. You pay nothing while idle and you skip the ramp when work arrives.

Because `idleReplicaCount` must be strictly less than `minReplicaCount`, and only `0` is allowed, the only shape it can ever take is "idle at zero, floor at N ≥ 1."

---

## 4. Timing

| Field | Default | What it actually controls |
|---|---|---|
| `pollingInterval` | **30 s** | how often the operator asks your event source for a value |
| `cooldownPeriod` | **300 s** | how long after the last activity before the **final step to zero or idle** |
| `initialCooldownPeriod` | **0 s** | a grace period after the `ScaledObject` is created before the first scale-to-zero is allowed |

> [!important] `cooldownPeriod` is not "the scale-down delay"
> It governs **only** the last transition — N → 0, or N → `idleReplicaCount`. Going from 9 pods to 4 is the HPA's decision, and its timing is the HPA's `behavior.scaleDown.stabilizationWindowSeconds` (Kubernetes default 300 s), which you set through `advanced.horizontalPodAutoscalerConfig.behavior`. Tuning `cooldownPeriod` to make scale-down calmer does nothing at all. This is the most-misunderstood pair of settings in KEDA and [[10 - Tuning Scaling Behaviour]] exists to untangle it.

**`initialCooldownPeriod`** covers a specific annoyance: apply a `ScaledObject` to an idle workload and, with a default `cooldownPeriod` and no prior activity recorded, it can be scaled to zero almost immediately. If you want a newly-created `ScaledObject` to leave the workload alone for ten minutes before it's allowed to switch it off, set `initialCooldownPeriod: 600`.

> [!note] v2.20 warns you when timing fields are pointless
> Apply a `ScaledObject` with `minReplicaCount: 1` and a `pollingInterval`, and the webhook returns a **non-blocking warning** telling you `pollingInterval` is only relevant when `minReplicaCount = 0`, `idleReplicaCount = 0`, or `useCachedMetrics` is enabled — with a matching warning for `cooldownPeriod`. The resource is still accepted. The reasoning: with a floor of 1 and no caching, the HPA drives everything from its own ~15-second sync and KEDA's polling loop isn't in the scaling path. Don't ignore the warning — it usually means you were tuning the wrong knob.

---

## 5. `triggers[]` — where the signal comes from

At least one trigger is required. Each one contributes a metric to the generated HPA, and when there are several the HPA takes the **highest** resulting replica count ([[11 - Multiple Triggers, Scaling Modifiers & Fallback]]).

| Field | Default | Notes |
|---|---|---|
| `type` | **required** | the scaler: `kafka`, `prometheus`, `aws-sqs-queue`, `cron`, `cpu`, … |
| `name` | none | optional label for the trigger. **Must be unique** within the `ScaledObject` if set |
| `metricType` | **`AverageValue`** | how the HPA interprets your threshold |
| `metadata` | `{}` | the scaler's own settings — all values are **strings** |
| `authenticationRef` | none | `{name, kind}`; `kind` is `TriggerAuthentication` (default) or `ClusterTriggerAuthentication` |
| `useCachedMetrics` | `false` | serve the HPA from cache instead of hitting the source. See below |

**Name your triggers.** It costs one line and it's what makes `.status.triggersActivity` and KEDA's per-trigger metrics readable — otherwise you're correlating by index. It also becomes part of the HPA's external metric name, so a good name shows up in `kubectl describe hpa`.

**`metricType` decides what your threshold means**, and it's the second most common source of a mis-scaled workload after a bad threshold:

| `metricType` | The HPA compares your threshold against | Use for |
|---|---|---|
| `AverageValue` (default) | the metric **divided by the current replica count** | almost everything — "how much backlog should *one pod* own" |
| `Value` | the raw metric total | when the metric is already a per-pod or intensive figure (a rate, a percentage, a gauge) |
| `Utilization` | a percentage of requests | **cpu/memory triggers only** — rejected for external metrics |

Get this wrong and the arithmetic inverts: with `Value`, a lag of 1000 against a threshold of 500 gives you two pods and stays there forever, because the total never changes as you add pods.

**`useCachedMetrics: true`** is the answer to "KEDA is hammering my Kafka brokers." Normally the HPA's ~15-second sync causes a fresh call into the scaler each time, *on top of* the operator's own `pollingInterval` loop — so a busy cluster with hundreds of `ScaledObject`s can generate a surprising amount of metadata traffic. Turn caching on and the HPA is served from the value the polling loop already collected, so **your event source is contacted at `pollingInterval` cadence only**.

The trade-off is honest and small: the HPA acts on data up to one `pollingInterval` old. If that matters, lower `pollingInterval` — you're still ahead, because you've replaced two independent call rates with one you control.

> [!warning] Gotcha — `useCachedMetrics` is rejected for cpu, memory and cron
> Apply it on any of those three and the whole `ScaledObject` is refused with a message naming the scaler. It makes no sense for them: cpu/memory never go through KEDA's metrics path at all, and `cron` computes its answer from the clock with nothing to cache.
> **Mitigation:** set it per-trigger, not as a blanket policy. On a `ScaledObject` that mixes a Kafka trigger with a CPU trigger, put it on the Kafka trigger only.

> [!tip] Best practice — turn `useCachedMetrics` on for polled external sources
> For Kafka, SQS, Prometheus and friends it's close to a free win: fewer calls, less broker load, one predictable cadence, and one fewer way for a slow event source to make HPA syncs time out. Leave it off only where you genuinely need reaction faster than `pollingInterval` and have lowered `pollingInterval` accordingly.

---

## 6. `advanced` — the HPA pass-through

```yaml
  advanced:
    restoreToOriginalReplicaCount: true
    horizontalPodAutoscalerConfig:
      name: my-orders-hpa
      behavior:
        scaleDown:
          stabilizationWindowSeconds: 300
          policies:
          - type: Percent
            value: 50
            periodSeconds: 60
        scaleUp:
          stabilizationWindowSeconds: 60
```

**`restoreToOriginalReplicaCount`** (default **`false`**) — on deletion, put the workload back to the replica count it had when the `ScaledObject` first took over. Read §11 before you decide to leave this off.

**`horizontalPodAutoscalerConfig.name`** overrides the generated `keda-hpa-<name>`. Two reasons to use it: your `ScaledObject` name is long enough that the prefixed HPA name won't fit (§10), or you're adopting an existing HPA, where a matching name is *required* alongside the ownership-transfer annotation ([[03 - KEDA Architecture — Operator, Metrics Adapter & the HPA It Owns]] §8). Changing this field on a live `ScaledObject` deletes the old HPA and creates a new one — a brief window with no autoscaler.

**`horizontalPodAutoscalerConfig.behavior`** is a verbatim pass-through of the HPA's own `behavior` block: stabilisation windows, scale-up/scale-down policies, `selectPolicy`. **This is where you control 1↔N timing** — nothing in KEDA's own fields does it. Note that the per-behavior `tolerance` setting requires **Kubernetes v1.33 or newer**.

**`scalingModifiers`** also lives under `advanced`. It lets you combine several triggers with a formula — `"(orders_lag + retry_lag) / 2"` — instead of taking the max, collapsing them into a single composite metric on the HPA. It has its own fields (`formula`, `target`, `activationTarget`, `metricType`) and its own failure modes, all in [[11 - Multiple Triggers, Scaling Modifiers & Fallback]].

---

## 7. `fallback` — what happens when the scaler errors

```yaml
  fallback:
    failureThreshold: 3                     # consecutive polling failures before it kicks in
    replicas: 4                             # the number to use
    behavior: currentReplicasIfHigher       # static | currentReplicas |
                                            # currentReplicasIfHigher | currentReplicasIfLower |
                                            # scalingModifiers      (default: static)
```

Without it, an unreachable event source leaves the HPA with an unreadable metric — and an HPA that can't read one of its metrics **refuses to scale down at all**, pinning the workload wherever it happened to be. `fallback` converts "no data" into "this many pods," which is a decision you can reason about.

There are preconditions (at least one non-cpu/memory trigger using `AverageValue`, and it interacts carefully with scale-to-zero). Full treatment in [[11 - Multiple Triggers, Scaling Modifiers & Fallback]].

---

## 8. Reading the status — your everyday debugging tools

Start here, always:

```bash
kubectl get so -n prod
```

```text
NAME             SCALETARGETKIND      SCALETARGETNAME   MIN  MAX  TRIGGERS  READY  ACTIVE  FALLBACK  PAUSED  AGE
orders-consumer  apps/v1.Deployment   orders-consumer   0    12   kafka     True   True     False    False   6d
```

Those four booleans are the four **conditions**, and each answers a different question:

| Condition | `True` means |
|---|---|
| **`Ready`** | the `ScaledObject` is valid, the scalers were built, and the HPA exists and is healthy. If this is `False`, nothing else matters — fix it first |
| **`Active`** | the triggers currently report work to do, so the workload is scaled above zero. `False` with `Ready: True` on an idle queue is completely normal |
| **`Fallback`** | scalers are failing and the `fallback` replica count is in force. Should always alert |
| **`Paused`** | an operator has deliberately frozen this `ScaledObject` via annotation (§9) |

Since v2.20, the HPA's own health feeds back into `Ready`: if the HPA reports that scaling is inactive, the `ScaledObject` goes `Ready: False` with a reason of `HPAMetricsUnavailable`, or `ScalingDegraded` if it was already unhealthy. That's a genuinely useful change — it means a broken metrics path surfaces on the resource you're already looking at instead of only on the HPA.

Then the detail:

```bash
kubectl describe so orders-consumer -n prod
```

`describe` gives you the conditions with their **reasons and messages** — `ScalerNotActive`, `ScalerCooldown`, `TriggerError`, `PartialTriggerError` ("some triggers … are not working correctly") — plus the events KEDA has emitted, including `KEDAScaleTargetActivated` and `KEDAScaleTargetDeactivated` with their from/to replica counts. That event log is often the fastest way to see what actually happened overnight.

Three status fields are worth knowing by name:

```bash
# which HPA belongs to this ScaledObject
kubectl get so orders-consumer -o jsonpath='{.status.hpaName}'

# the exact external metric names — you need these to query the metrics API by hand
kubectl get so orders-consumer -o jsonpath='{.status.externalMetricNames}'

# per-trigger activity: which trigger thinks there is work? (added in v2.19)
kubectl get so orders-consumer -o jsonpath='{.status.triggersActivity}' | jq .
```

`.status.triggersActivity` is the one that saves time on a multi-trigger workload: instead of inferring which of four triggers is keeping the workload awake, you read it. Also present and occasionally useful: `.status.originalReplicaCount` (what a restore-on-delete would return you to), `.status.pausedReplicaCount`, `.status.health` (per-metric failure counts feeding the fallback logic), and `.status.lastActiveTime` (the clock that `cooldownPeriod` measures from).

For the layer below this — the raw metrics API, the Prometheus metric names, CloudEvents — see [[14 - Observability & Debugging — Why Isn't It Scaling]].

---

## 9. Operational annotations

These go on the `ScaledObject`'s `metadata.annotations` and take effect without an edit to the spec, which makes them the right tool during an incident.

| Annotation | Effect |
|---|---|
| `autoscaling.keda.sh/paused: "true"` | freeze scaling entirely at the current replica count. Sets `Paused: True` |
| `autoscaling.keda.sh/paused-replicas: "10"` | scale to exactly 10, then freeze there |
| `autoscaling.keda.sh/paused-scale-in: "true"` | allow scale-out, block **all** scale-in — both KEDA's step to zero and the HPA's scale-down |
| `autoscaling.keda.sh/paused-scale-out: "true"` | allow scale-in, block **all** scale-out — both 0→1 and the HPA's scale-up |
| `autoscaling.keda.sh/force-activation: "true"` | treat the triggers as active regardless of what they report, so the workload leaves zero and stays off it |

```bash
# freeze during a suspicious incident
kubectl annotate so orders-consumer autoscaling.keda.sh/paused=true

# pin at 20 pods while you drain a backlog by hand
kubectl annotate so orders-consumer autoscaling.keda.sh/paused-replicas=20 --overwrite

# release it
kubectl annotate so orders-consumer autoscaling.keda.sh/paused-replicas-
```

**What pausing does and doesn't stop.** It stops *scaling decisions*: KEDA won't activate or deactivate, and the two directional variants also write `selectPolicy: Disabled` into the generated HPA so the HPA can't move either. It does **not** stop the operator polling your event source, it does **not** delete the HPA, and it does **not** stop your metrics being published — which is exactly what you want, because it means you can watch what *would* have happened while the workload is held still.

**`paused-scale-in` is the underrated one.** During a Kafka incident where you don't want consumers disappearing mid-recovery, it lets the system keep adding capacity while guaranteeing nothing shrinks. Much better than pausing outright.

**`force-activation` only forces the zero crossing.** It gets the workload to 1 and keeps it there; from 1 upward the HPA is still working from real metric values. It's the tool for pre-warming before a known burst — not a way to hold a workload at 30 pods.

> [!warning] Gotcha — you cannot reliably un-pause by setting the annotation to false
> KEDA parses these annotations leniently: **anything that isn't a recognisable boolean is treated as `true`**. So `autoscaling.keda.sh/paused: "maybe"`, `"yes"`, or a stray `"1 "` with a trailing space all pause the `ScaledObject`. Worse, `paused-replicas` pauses on **mere presence** — its value is the replica count, so there is no value of it that means "not paused." Setting `paused-replicas: "false"` freezes the workload and produces a confusing replica target on top.
> **Mitigation:** always **remove** the annotation to resume — `kubectl annotate so <name> autoscaling.keda.sh/paused-` (note the trailing hyphen). Never "set it to false." And when a workload is mysteriously stuck at a fixed count, `kubectl get so <name> -o jsonpath='{.metadata.annotations}'` should be one of your first three commands.

---

## 10. What the admission webhook rejects

KEDA validates on `CREATE` and `UPDATE`, so most mistakes come back as a `kubectl apply` error rather than a broken cluster. The practical list:

1. **Two `ScaledObject`s targeting the same workload** — *"the workload … is already managed by the ScaledObject …"*. Also caught: two `ScaledObject`s whose HPA names would collide.
2. **A workload already managed by a foreign HPA** — one you wrote by hand. Rejected unless you're deliberately transferring ownership ([[03 - KEDA Architecture — Operator, Metrics Adapter & the HPA It Owns]] §8).
3. **`minReplicaCount` greater than `maxReplicaCount`**, and **`idleReplicaCount` not less than `minReplicaCount`**. The bounds check also exists as a schema-level rule, so it's enforced even with the webhook disabled.
4. **A name too long.** Since v2.20 KEDA refuses a `ScaledObject` whose name exceeds 63 characters, *and* one whose derived HPA name would exceed 63 — and since it prefixes `keda-hpa-` (9 characters) to build that name, the **practical limit on a `ScaledObject` name is 54 characters**. The error tells you to shorten the name or set `advanced.horizontalPodAutoscalerConfig.name`.
5. **A cpu/memory-only `ScaledObject` with `minReplicaCount: 0`** — scale-to-zero can't work when the only signal is a metric that requires running pods. Also rejected: a cpu or memory trigger whose target container has no matching resource *request* defined.
6. **Duplicate trigger names** — *"triggerName … is defined multiple times … but it must be unique"*. Same for two triggers that would generate the same external metric name.
7. **`useCachedMetrics` on cpu, memory or cron** (§5).
8. **Malformed `fallback` or `scalingModifiers`** — negative values, a formula without a target, a `Utilization` metric type on an external metric.

> [!warning] Gotcha — validation is best-effort by default
> The `ValidatingWebhookConfiguration` ships with `failurePolicy: Ignore`. If the webhook pod is down, unreachable, or exceeds its 10-second timeout, **your invalid resource is admitted anyway** — and then fails at reconcile time, in the operator's log, where nobody is looking. This also means CI that relies on a dry-run apply catching a bad config can pass on a cluster whose webhook is broken.
> **Mitigation:** if you depend on these rejections as a guardrail, set the chart's `webhooks.failurePolicy=Fail` — and then make the webhook Deployment's availability something you actually alert on, because at `Fail` a dead webhook blocks every KEDA apply. On EKS with a custom CNI, the webhook is a classic reachability casualty; the chart's host-network option exists for that case.
> Note too that **`ScaledJob`'s webhook validates far less** — essentially only its triggers — so don't expect the same safety net there ([[13 - ScaledJob — When a Pod per Message Beats a Consumer Pool]]).

---

## 11. Gotchas that cause real pain

> [!warning] Deleting a ScaledObject does NOT restore your replicas
> `restoreToOriginalReplicaCount` defaults to **`false`**. So deleting a `ScaledObject` leaves the workload at whatever replica count it happened to be at — and if it was **scaled to zero at that moment, it stays at zero**, with nothing left in the cluster that will ever bring it back. The `kubectl delete` succeeds cleanly, the HPA is garbage-collected tidily, and your consumer is silently off. This is one of the most common real KEDA incidents, and it is at its most likely exactly when you'd do it: cleaning up an idle workload out of hours.
> **Mitigation:** set `spec.advanced.restoreToOriginalReplicaCount: true` on every scale-to-zero `ScaledObject`, as a default in your templates. And make `kubectl get deploy <target>` a mandatory step after removing a `ScaledObject` — never trust the delete output. (Note the restore only works if KEDA ever recorded an original count; a `ScaledObject` that never scaled anything has nothing to restore.)

> [!warning] The `paused` annotation has no "false" you can trust
> Covered in §9 — anything unparseable means `true`, and `paused-replicas` pauses on presence alone.
> **Mitigation:** remove annotations to resume; never set them false. If you manage `ScaledObject`s through GitOps, make sure your templating **omits** the annotation rather than rendering `paused: "false"` — a rendered-false `paused` happens to work today, but a rendered-false `paused-replicas` freezes production.

> [!warning] A ScaledObject stuck in `Terminating` because of its finalizer
> KEDA adds a finalizer so it can clean up (and optionally restore replicas) on delete. If the operator is down, has lost RBAC, or can no longer resolve the scale target — the Deployment was deleted first, say — the finalizer never clears and the `ScaledObject` hangs in `Terminating` indefinitely, blocking namespace deletion with it.
> **Mitigation:** fix the operator first if you can, because clearing the finalizer skips the cleanup. When you must force it:
> ```bash
> kubectl patch scaledobject <name> -n <ns> \
>   -p '{"metadata":{"finalizers":null}}' --type=merge
> ```
> Then check the target workload's replica count by hand, since the restore step didn't run. If a whole namespace is stuck, look for `ScaledJob`s too — they have the same finalizer and the same patch.

> [!warning] An upgrade rejects a name that used to be fine
> The 63-character HPA-name rule arrived in **v2.20**. A `ScaledObject` created on v2.19 with a 60-character name works fine, survives the upgrade as an existing object — and then fails the next time anything applies it, including an unrelated `kubectl apply` from your GitOps controller. The symptom is a deploy pipeline failing on a resource nobody changed, with an error about name length.
> **Mitigation:** before upgrading, audit for long names:
> ```bash
> kubectl get so -A -o json | jq -r '.items[] | select((.metadata.name|length) > 54) | "\(.metadata.namespace)/\(.metadata.name) \(.metadata.name|length)"'
> ```
> Fix them by renaming the `ScaledObject` (a delete and recreate — mind the restore behaviour above) or, if renaming is expensive, by setting a short `advanced.horizontalPodAutoscalerConfig.name`, which satisfies the derived-name check. Keep names under 54 characters as a standing convention; generated names like `<team>-<service>-<env>-consumer` reach it faster than you'd think.

---

## 12. Alternatives — `ScaledObject` and its neighbours

| Approach | What it manages | Choose it when |
|---|---|---|
| **`ScaledObject`** | replica count of a long-running workload, via an HPA it owns, plus 0↔1 itself | the default. A consumer pool that processes many messages per pod and should shrink to zero when idle |
| **`ScaledJob`** | creates one Kubernetes **Job per unit of work**; no HPA at all | each message is long-running or resource-heavy, you want Kubernetes-level retry and per-item isolation, and a mid-flight pod kill is unacceptable. It also *increases* rebalances — see [[13 - ScaledJob — When a Pod per Message Beats a Consumer Pool]] |
| **A plain HPA you write** | replica count from CPU/memory or your own custom metric | CPU really is the signal, you never need zero, and you'd rather not run another controller. You lose 60+ scalers, scale-to-zero, and the trigger-authentication machinery |
| **Manual replica management** | you, or a cron job, running `kubectl scale` | the load pattern is a known schedule with no variance. Even then, prefer a `cron` trigger inside a `ScaledObject` — same behaviour, declarative, and it composes with a real event trigger for the unexpected days |

> [!tip] The committed default
> Reach for `ScaledObject` unless per-message isolation or long per-message runtime pushes you to `ScaledJob`. And prefer a `cron` trigger over a scheduled `kubectl scale` even for purely time-based scaling — you get the same result, in one declarative place, on a resource you can already observe.

---

## 13. Questions you should be able to answer

- What are the defaults for `minReplicaCount` and `maxReplicaCount`? *(0 and 100 — and 100 is rarely what you want.)*
- `idleReplicaCount: 0` with `minReplicaCount: 5` — what happens on the first message? *(Straight from 0 to 5, skipping the HPA's ramp.)*
- Which field controls how fast a workload goes from 9 pods to 4? *(Not `cooldownPeriod` — the HPA's `behavior.scaleDown`, set via `advanced.horizontalPodAutoscalerConfig.behavior`.)*
- Your event source is complaining about request volume from KEDA. What do you change? *(`useCachedMetrics: true` on the polled triggers, plus a deliberate `pollingInterval`.)*
- What does `metricType: Value` do to a lag threshold? *(Compares the raw total instead of the per-pod average — the count stops growing with replicas.)*
- You delete a `ScaledObject` while the workload is at zero. What's the replica count afterwards? *(Zero, unless `restoreToOriginalReplicaCount: true` was set.)*
- `Ready: True`, `Active: False`. Is something wrong? *(No — that's a healthy `ScaledObject` on an idle source.)*
- How do you un-pause a `ScaledObject`? *(Remove the annotation. Setting it to false is not reliable, and is actively wrong for `paused-replicas`.)*
- Why would KEDA reject a 58-character `ScaledObject` name? *(`keda-hpa-` plus the name must fit in 63, so the practical limit is 54.)*
- Your obviously-invalid `ScaledObject` was accepted. Why? *(The webhook's `failurePolicy` is `Ignore` by default and the webhook was unreachable.)*
- Which command tells you *which* of four triggers is keeping the workload awake? *(`kubectl get so <name> -o jsonpath='{.status.triggersActivity}'`.)*

---

## 14. Sources

- [KEDA — ScaledObject specification](https://keda.sh/docs/2.20/reference/scaledobject-spec/)
- [KEDA — Scaling Deployments (activation vs scaling, pausing, annotations)](https://keda.sh/docs/2.20/concepts/scaling-deployments/)
- [KEDA — Authentication and `TriggerAuthentication`](https://keda.sh/docs/2.20/concepts/authentication/)
- [KEDA — Admission webhooks](https://keda.sh/docs/2.20/operate/admission-webhooks/)
- [KEDA — Troubleshooting](https://keda.sh/docs/2.20/troubleshooting/)
- [KEDA — FAQ](https://keda.sh/docs/2.20/reference/faq/)
- [KEDA — ScaledJob specification (for the comparison)](https://keda.sh/docs/2.20/reference/scaledjob-spec/)
- [KEDA — CHANGELOG (the 63-character name rule, `triggersActivity`, HPA health in `Ready`)](https://github.com/kedacore/keda/blob/main/CHANGELOG.md)
- [Kubernetes — HorizontalPodAutoscaler v2 API (`behavior`, policies, tolerance)](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/horizontal-pod-autoscaler-v2/)
- [Kubernetes — HorizontalPodAutoscaler walkthrough](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/)
- [Kubernetes — Finalizers](https://kubernetes.io/docs/concepts/overview/working-with-objects/finalizers/)
- [Kubernetes — Dynamic admission control and `failurePolicy`](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/)
