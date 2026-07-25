---
title: 11 - Multiple Triggers, Scaling Modifiers & Fallback
tags:
  - keda
  - kafka
  - autoscaling
  - kubernetes
  - hpa
  - senior
aliases:
  - KEDA Multiple Triggers
  - scalingModifiers
  - KEDA fallback
  - metricType
  - composite-metric
status: ready
created: 2026-07-25
---

# Multiple Triggers, Scaling Modifiers & Fallback

> [!abstract] Scope
> Composing more than one signal into a single scaling decision, and surviving the moment one of those signals goes dark. Three ideas: **several triggers give you the maximum, never the sum**; `scalingModifiers` lets you replace that maximum with a formula; and `fallback` is what stops an unreachable scaler from quietly pinning your workload at whatever size it happened to be. The last one looks like a nice-to-have and is actually the difference between a brief broker outage and a week of unexplained cloud spend. Builds on the replica arithmetic in [[07 - The Kafka Scaler — Every Field & the Replica Math]] and the timing knobs in [[10 - Tuning Scaling Behaviour]].

Related: [[00 - KEDA Index]], [[03 - KEDA Architecture — Operator, Metrics Adapter & the HPA It Owns]], [[04 - The ScaledObject CRD, End to End]], [[07 - The Kafka Scaler — Every Field & the Replica Math]], [[08 - Scale-to-Zero & Activation]], [[10 - Tuning Scaling Behaviour]], [[14 - Observability & Debugging — Why Isn't It Scaling]], [[16 - Alternatives & Trade-offs]]

> [!warning] Written against KEDA v2.20 · Kubernetes 1.33–1.35 · Apache Kafka 4.3
> `fallback.behavior: scalingModifiers` is **new in v2.20** and won't validate on older versions. `fallback` support for `Value`-type triggers arrived in v2.18. Check the [ScaledObject spec](https://keda.sh/docs/2.20/reference/scaledobject-spec/) for whatever version you actually run — KEDA ships breaking changes in minor releases.

---

## 1. Several triggers: the HPA takes the highest, not the sum

A ScaledObject can carry as many triggers as you like. Each one independently proposes a replica count, and then:

> [!important] The rule
> Each trigger becomes its own metric on the generated HPA. The HPA computes a desired replica count **per metric**, and then — quoting the Kubernetes docs — *"the largest of the desired replica counts is chosen."*
>
> **Not the sum. The maximum.** This misunderstanding is common and expensive in both directions: people add triggers expecting more capacity and get none, or they build a two-trigger config expecting a max and budget for a sum.

### A worked two-trigger example: Kafka lag plus a known daily peak

You have a 12-partition orders topic. Every weekday from 09:00 you get a predictable surge, and lag-based scaling always lags it by a minute or two — by the time the backlog is visible, customers have already waited. So pre-warm on a clock.

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: orders-consumer
spec:
  scaleTargetRef:
    name: orders-consumer
  pollingInterval: 30
  minReplicaCount: 1
  maxReplicaCount: 12                 # the partition count
  triggers:
  - type: kafka
    name: orders_lag                  # naming triggers is good hygiene; mandatory for formulas (§3)
    metadata:
      bootstrapServers: kafka-bootstrap.kafka:9092
      consumerGroup: orders-processor
      topic: orders
      lagThreshold: "1000"
  - type: cron
    name: morning_peak
    metadata:
      timezone: Asia/Kolkata
      start: "0 9 * * 1-5"            # 09:00, Mon–Fri
      end: "0 11 * * 1-5"             # 11:00, Mon–Fri
      desiredReplicas: "8"
```

Walk the arithmetic at three moments:

| Time | Kafka lag | Kafka proposes | Cron proposes | HPA runs |
|---|---|---|---|---|
| 08:55 | 1,400 | `ceil(1400/1000)` = **2** | 0 (outside window) | **2** |
| 09:01 | 1,400 | **2** | **8** | **8** — the floor arrives before the traffic does |
| 09:40 | 10,600 | `ceil(10600/1000)` = **11** | **8** | **11** — real lag overtakes the floor |
| 11:05 | 900 | **1** | 0 | **1** — but the HPA's scale-down window makes the descent gradual |

Two things to notice. At 09:01 the cron trigger is acting as a **floor**, not an addition: 2 and 8 give 8, not 10. And at 09:40 the cron trigger has become irrelevant — once real demand exceeds the scheduled floor, the schedule stops mattering. That is exactly the behaviour you want from a pre-warm, and it only works because the rule is "maximum".

> [!tip] How the cron trigger produces a replica count
> The cron trigger reports its `desiredReplicas` as the metric value against a target of 1 while inside the window, and 0 outside it. The HPA's own division then lands on exactly `desiredReplicas`. Nothing special is happening — it's the same `ceil(value / target)` as every other trigger, arranged so the answer is the number you wrote.

### The other common pairing: Kafka plus CPU as a safety net

Lag is a proxy for work, and it's a bad proxy when messages vary wildly in cost ([[07 - The Kafka Scaler — Every Field & the Replica Math]]). A CPU trigger alongside the Kafka one catches the case where a handful of expensive messages saturates your pods while lag stays modest:

```yaml
  - type: cpu
    metricType: Utilization           # cpu/memory accept Utilization or AverageValue only
    metadata:
      value: "75"
```

It genuinely helps — and it costs you scale-to-zero. See the gotcha in §7.

---

## 2. `metricType`: `AverageValue` vs `Value`

Every trigger has a `metricType`, defaulting to `AverageValue`. It decides **how the HPA turns your metric into a replica count**, and it is the difference between two quite different questions.

| | `AverageValue` (default) | `Value` |
|---|---|---|
| The target means | "this much, **per pod**" | "this much, in total" |
| HPA computes | `ceil(value / target)` | `ceil(currentReplicas × value / target)` |
| Sensitive to current replica count | **no** | **yes** |
| Good for | a **backlog** metric: queue depth, consumer lag, unprocessed items | an **intensive** metric: average latency, average age of oldest message, a rate |

**Why the difference matters.** Suppose the metric is 20 and the target is 5.

- `AverageValue` → `ceil(20/5)` = **4 pods**, whatever you're running now. The reasoning: "each pod should own 5 units of backlog; there are 20 units; that's 4 pods." Total work divides across replicas, so the answer is absolute.
- `Value` → with 3 pods running, `ceil(3 × 20/5)` = **12 pods**. The reasoning: "the current fleet is producing a reading 4× worse than target, so quadruple the fleet." The metric doesn't shrink just because you split it up — average latency stays average latency — so the correction has to be proportional to what you're already running.

That is the test. **Ask whether adding a pod mechanically reduces the metric.** Lag: yes, halved across twice the consumers → `AverageValue`. Average time-in-queue: not arithmetically, only as a second-order effect → `Value`.

For Kafka, **`AverageValue` is almost always right**, and it's the default, which is why `lagThreshold` reads naturally as "backlog per pod."

> [!warning] Gotcha — cpu and memory triggers don't accept `Value`
> Every scaler supports `AverageValue` and `Value` **except** `cpu` and `memory`, which support `AverageValue` and `Utilization`. Copying `metricType: Value` onto a CPU trigger is rejected. And `Utilization` is only meaningful for cpu/memory — external metrics reject it outright, including inside `scalingModifiers`.
> **Mitigation:** cpu/memory → `Utilization` (a percentage of the container's *request*, so the request must be set) or `AverageValue` (an absolute quantity). Everything else → leave it at `AverageValue` unless you can articulate why the reading shouldn't divide.

---

## 3. `scalingModifiers` — replacing the maximum with a formula

Sometimes "maximum" is genuinely the wrong composition. If one consumer pool drains **two** topics, the work it faces is the *sum* of two backlogs, and taking the max under-provisions it. `scalingModifiers` lets you write the arithmetic yourself.

```yaml
spec:
  advanced:
    scalingModifiers:
      formula: "orders_lag + shipments_lag"   # mandatory when target is set
      target: "1000"                          # mandatory when formula is set
      activationTarget: "50"                  # optional. Default 0
      metricType: AverageValue                # optional. AverageValue | Value. Default AverageValue
  triggers:
  - type: kafka
    name: orders_lag                          # every trigger used in the formula MUST be named
    metadata:
      bootstrapServers: kafka-bootstrap.kafka:9092
      consumerGroup: fulfilment
      topic: orders
      lagThreshold: "1000"
  - type: kafka
    name: shipments_lag
    metadata:
      bootstrapServers: kafka-bootstrap.kafka:9092
      consumerGroup: fulfilment
      topic: shipments
      lagThreshold: "1000"
```

**Walk the arithmetic.** Orders lag 2,600, shipments lag 900:

- **Without** the formula (plain max-of-triggers): `max(ceil(2600/1000), ceil(900/1000))` = `max(3, 1)` = **3 pods**
- **With** the formula: `(2600 + 900) = 3500`, then `ceil(3500/1000)` = **4 pods**

Three pods for 3,500 messages of combined backlog under-provisions a pool that has to clear both. The formula gives the honest answer.

Note also that once the formula is in charge, the per-trigger `lagThreshold` values stop driving the replica count — `scalingModifiers.target` does. Keep them set anyway (they're required metadata and they document intent), but understand that `target` is now the dial.

**The formula language** is [`expr`](https://expr.medv.io/docs/Language-Definition), so you get arithmetic, comparisons, ternaries and the `??` nil-coalescing operator. It must return a **single value, not a boolean**. Useful shapes:

| Formula | What it expresses |
|---|---|
| `orders_lag + shipments_lag` | total backlog across two sources |
| `(trig_one + trig_two) / 2` | average of two signals — smooths a noisy one against a stable one |
| `orders_lag > 5000 ? orders_lag : orders_lag / 4` | be conservative below a threshold, aggressive above it |
| `primary ?? backup ?? 8` | preference chain with a static last resort — see §4 |

> [!important] The formula replaces your external metrics with one composite metric
> This is the part that changes your debugging life. When `scalingModifiers` is active, KEDA does **not** publish `s0-kafka-orders` and `s1-kafka-shipments` to the HPA. It publishes a **single** external metric named `composite-metric`, and the HPA sees one number. (Any cpu/memory metrics survive alongside it, because those never travel through KEDA at all.)
>
> So `kubectl describe hpa` shows one metric where you expected two, `kubectl get --raw` on the old per-trigger metric names returns nothing, and any dashboard panel keyed on them goes blank. The per-trigger values still exist in KEDA's own Prometheus metrics — which is exactly why giving every trigger a `name` matters, since that name is what distinguishes it there.

**Constraints worth knowing before you reach for it:**

- `formula` and `target` are a package deal — one without the other is rejected.
- `metricType` must be `AverageValue` or `Value`. **`Utilization` is rejected** for external metrics.
- Every trigger referenced in the formula must have a `name`, and names must be unique within the ScaledObject.
- `activationTarget` is the CRD-level analogue of a per-scaler activation threshold: if the composed value is at or below it, the ScaledObject isn't Active and can scale to zero ([[08 - Scale-to-Zero & Activation]]).

> [!tip] Committed recommendation
> Reach for `scalingModifiers` when the composition is genuinely arithmetic — a **sum** across sources one pool drains, or an **average** to damp a noisy signal. Do *not* use it to reproduce a maximum; that's the free default. And do not use it as your first tool: it costs you per-trigger observability at the HPA, which is where you look during an incident.

---

## 4. `fallback` — what happens when a scaler can't be reached

```yaml
spec:
  fallback:
    failureThreshold: 3          # mandatory. Consecutive scaler ERRORS before fallback engages
    replicas: 4                  # mandatory. The reference replica count
    behavior: currentReplicasIfHigher   # optional. Default: static
```

KEDA counts **consecutive failures per scaler**. Past `failureThreshold`, instead of reporting nothing to the HPA, it reports a synthetic metric value engineered so the HPA's own division lands on the replica count you asked for. The workload ends up at a defined size instead of an undefined one.

> [!important] Fallback fires on *errors*, not on zero
> `failureThreshold: 3` means three consecutive failures to **get** the metric — an unreachable broker, an auth failure, a DNS problem, a timeout. A scaler that successfully reports "lag is 0" has not failed; that's a healthy reading of an empty topic and it scales you down (and eventually to zero) exactly as intended. Fallback is about *not knowing*, never about *knowing there's no work*.

### The `behavior` options

| `behavior` | Resulting replicas | Choose it when |
|---|---|---|
| `static` (default) | exactly `fallback.replicas` | you know a safe steady-state size and want the workload to converge there regardless of where it was. Best for predictable, moderate-throughput consumers. |
| `currentReplicas` | whatever is running right now | **hold position.** You trust the last good decision more than any guess, and the outage is expected to be short. `fallback.replicas` is ignored for the calculation but still required by the schema. |
| `currentReplicasIfHigher` | `max(current, fallback.replicas)` | **the safe default for Kafka.** Never shrink during blindness, and if the outage caught you at 1 pod, come up to a floor. Protects against the scenario where an outage begins during a quiet moment and a burst arrives while you're blind. |
| `currentReplicasIfLower` | `min(current, fallback.replicas)` | cost is the dominant risk and the workload is interruption-tolerant. Caps runaway spend during a long blind period, at the price of possibly shedding capacity you needed. |
| `scalingModifiers` (v2.20+) | whatever the formula resolves to | you have a formula and a **redundant signal**. Failed triggers are passed into the formula as `nil`, so `primary ?? backup ?? 8` degrades gracefully: primary, then backup, then a literal. Requires `scalingModifiers.formula` to be defined. |

The `scalingModifiers` behavior is the interesting one, because it turns fallback from a blunt instrument into a preference chain evaluated per trigger rather than per ScaledObject:

```yaml
spec:
  advanced:
    scalingModifiers:
      formula: "orders_lag ?? lag_exporter ?? 4"   # Kafka direct → Prometheus exporter → floor of 4
      target: "1000"
  fallback:
    failureThreshold: 3
    replicas: 6                  # the ultimate fallback if the formula itself resolves to nil
    behavior: scalingModifiers
```

If every trigger in the chain fails **and** you left off the trailing literal, KEDA falls back to `fallback.replicas`. With the literal present, the literal wins.

> [!warning] Gotcha — fallback needs a trigger that isn't cpu or memory
> A ScaledObject whose triggers are *only* `cpu` and/or `memory` is rejected outright when you add a `fallback` section, with a message about needing an `AverageValue` trigger. The message is stale — `Value`-type triggers have been supported since v2.18 — but the requirement behind it is real: cpu and memory metrics never pass through KEDA, so there is no KEDA-side reading that could fail and nothing to substitute.
> **Mitigation:** fallback protects your event-source triggers. Make sure at least one exists, and don't expect it to do anything for the cpu/memory ones.

> [!warning] Gotcha — `Value`-type fallback needs at least one ready replica
> For `Value`-type triggers the fallback value is computed relative to the current ready replica count. With **zero** ready replicas that computation has no meaning, and the fallback is abandoned. A workload sitting at zero with a broken scaler therefore cannot be rescued by a `Value`-type fallback.
> **Mitigation:** for anything that scales to zero, keep the trigger on `AverageValue` (the default), whose fallback doesn't depend on the running count. This is one more reason `AverageValue` is the right choice for Kafka lag.

---

## 5. Why fallback matters far more than it looks

Here is the failure mode it exists to prevent, and it deserves to be the thing you remember from this module.

> [!warning] Gotcha — one unreadable metric freezes scale-down for the entire workload
> Kubernetes, verbatim: *"If any of these metrics cannot be converted into a desired replica count (e.g. due to an error fetching the metrics from the metrics APIs) **and a scale down is suggested by the metrics which can be fetched, scaling is skipped.**"*
>
> Read that carefully. The HPA does not fall back to the metrics it *can* read. It **refuses to act at all** in the down direction. So one unreachable Kafka cluster among your triggers doesn't merely stop that topic's scaling — it **pins the whole workload at its current replica count**, indefinitely.
>
> And the pin is silent. The Deployment is healthy. Pods are Running and Ready. Nothing crash-loops, no alert fires, dashboards look normal because the *other* metrics are reporting fine. If the outage began during your afternoon peak, you are now paying for peak capacity around the clock, and the discovery mechanism is a finance conversation next month. The HPA is still free to scale *up* if a readable metric demands it — so the number can drift upward and never come back down.
>
> **Mitigation — all three, they're not alternatives:**
> 1. **Configure `fallback` on every production ScaledObject.** `currentReplicasIfHigher` with a sensible `replicas` and `failureThreshold: 3` costs you nothing and converts "unreadable" into a defined number the HPA can act on.
> 2. **Alert on KEDA's scaler error counters** (`keda_scaler_detail_errors_total`, `keda_scaled_object_errors_total`) and on the ScaledObject's `Fallback` status condition. A fallback that nobody notices is a hidden outage — see §7.
> 3. **Check `kubectl describe hpa`** whenever a replica count looks stuck. `ScalingActive: False` with a metrics-unavailable reason is this exact situation. Since v2.20 that HPA condition also propagates back onto the ScaledObject as a not-Ready state, so `kubectl get so` shows it too.

---

## 6. When multiple triggers are right — and when to split the workload

The genuinely useful question isn't "can I add another trigger" (you can) but "should these two signals be scaling the same pods at all". A committed answer:

**Use multiple triggers on one ScaledObject when** the pods are interchangeable and any pod can serve work from any signal; the signals scale the *same* resource in the *same* direction; and you want a floor-or-ceiling relationship rather than independent scaling. The Kafka-plus-cron pre-warm in §1 is the archetype: one workload, one kind of work, two views of how much of it there is.

**Split into separate Deployments — each with its own ScaledObject — when any of these is true:**

| Signal | Why one ScaledObject can't serve it |
|---|---|
| **Different scaling shapes** | one topic is steady, another is bursty 20× a day. A single pod count must satisfy both, so you over-provision for the steady work all day to survive the bursts. |
| **Different per-message cost** | 5 ms events and 5 s events under one `lagThreshold` means the metric is meaningless for at least one of them. No single threshold is correct. |
| **Different failure domains** | two Kafka clusters under one ScaledObject means either cluster's outage freezes scale-down for *both* workloads (§5). Splitting halves the blast radius. |
| **Different latency requirements** | a user-facing topic wants instant scale-up and warm minimum replicas; a nightly reconciliation topic wants calm and scale-to-zero. Those are opposite `behavior` blocks; you can only have one per HPA. |
| **Different partition counts** | `maxReplicaCount` is a single number, but the partition ceiling is per topic. One value cannot be simultaneously right for a 6-partition and a 48-partition topic. |

> [!tip] The heuristic
> **One ScaledObject per scaling *shape*, not per workload.** Two separate Deployments running the same container image with different consumer groups and different ScaledObjects is not duplication — it is the correct design when the two streams behave differently. It costs you one extra Deployment manifest and buys you independent tuning, independent failure domains and a legible autoscaling story for each.

---

## 7. The gotchas that cause real incidents

> [!warning] Gotcha — expecting triggers to sum
> Somebody adds a second Kafka trigger to handle a second topic and expects capacity for both. They get `max(a, b)`. The under-provisioning is invisible at low volume — when one topic dominates, max and sum agree closely — and appears only when both are busy at once, which is precisely your worst moment.
> **Mitigation:** if the pods drain both streams, you want a **`scalingModifiers` sum** (§3), not two plain triggers. If you can't or won't use a formula, split into two Deployments with independent ScaledObjects (§6). Don't leave a max in place while reasoning about it as a sum.

> [!warning] Gotcha — duplicate trigger names are rejected, so a copy-pasted trigger fails to apply
> Trigger `name` values must be unique within a ScaledObject. The overwhelmingly common cause is duplicating a trigger block to add a second topic and forgetting to change the name — the admission webhook rejects the whole object, which means **your previously working configuration stays live** while you believe you've deployed a change. Worse, KEDA's webhook ships with a permissive failure policy, so if the webhook pod is down the duplicate is admitted and you get undefined behaviour instead of a clean rejection.
> **Mitigation:** read `kubectl apply` output rather than trusting the exit code, name triggers after what they measure (`orders_lag`, not `trigger2`), and in production install with the webhook's `failurePolicy` set to `Fail` so validation is actually enforced.

> [!warning] Gotcha — adding a CPU trigger silently kills scale-to-zero
> A CPU or memory trigger is **always considered active** — there is no such thing as a CPU reading that means "no work". KEDA only scales to zero when *every* trigger is inactive. So the moment you add a CPU safety net to a `minReplicaCount: 0` ScaledObject, the workload never reaches zero again. Nothing errors; `cooldownPeriod` elapses and nothing happens; the ScaledObject just stays Active forever. (If cpu/memory are your *only* triggers, the webhook at least rejects the combination outright.)
> **Mitigation:** choose one. Either keep scale-to-zero and drop the CPU trigger, or accept `minReplicaCount: 1` and keep the safety net. If the real worry was uneven per-message cost, a better answer is often to fix the *metric* — split expensive and cheap work onto different topics — rather than bolting on a second signal.

> [!warning] Gotcha — a formula silently changes what your dashboards show
> The day you add `scalingModifiers`, every per-trigger external metric the HPA was exposing is replaced by a single `composite-metric`. Panels keyed on `s0-kafka-orders` go flat. `kubectl get --raw` recipes in your runbook return nothing. The scaling itself is fine, so nobody connects the blank panels to the config change, and the runbook is discovered to be broken during the next incident.
> **Mitigation:** treat adopting a formula as an observability change as well as a scaling change. Update dashboards and runbooks in the same commit, and drive per-trigger panels from KEDA's Prometheus scaler metrics — labelled by the trigger `name` you set — rather than from the HPA's metric list.

> [!warning] Gotcha — fallback that masks a persistent outage
> Fallback is designed to hide a failure from your *workload*. It will just as happily hide it from *you*. A `static` fallback of 4 replicas on a consumer that normally runs 3–6 produces a pod count nobody looks at twice — while the real situation is that the scaler has been failing for eleven days, lag is unbounded, and offsets may already have aged out of the broker (7 days by default). The workload looks fine right up until the backlog becomes a customer-visible incident.
> **Mitigation:** never ship a fallback without an alert. Alert on the ScaledObject's `Fallback` condition being True for more than a few minutes, and on KEDA's scaler error counters. The fallback buys you time to respond; it is not a response. Pair it with an alert on **real** Kafka lag from a broker-side exporter, because KEDA's own metric is capped and cannot tell you a backlog is out of control ([[07 - The Kafka Scaler — Every Field & the Replica Math]]).

---

## 8. Alternatives — four ways to combine signals

| Approach | What you give up | Choose it when |
|---|---|---|
| **One trigger** | any second signal | **the default, and it's usually right.** One topic, one consumer group, one number. Every additional signal is a thing that can fail (§5) and a thing that has to be explained. Start here and justify anything more. |
| **Several triggers (max)** | control over composition — you get the maximum, full stop | you want a **floor or a ceiling**, not arithmetic: a cron pre-warm, a CPU safety net, a second topic that is genuinely secondary. Cheap, no observability cost, per-trigger metrics stay visible on the HPA. |
| **`scalingModifiers` formula** | per-trigger metrics at the HPA (one `composite-metric` replaces them); adds a formula to your review surface | the composition is genuinely **arithmetic** — a sum across sources one pool drains, an average to damp noise, or a `??` preference chain with a redundant signal. Also the only way to get per-trigger fallback (`behavior: scalingModifiers`). |
| **Separate Deployments** | shared warm capacity; one more Deployment to operate | the signals have **different scaling shapes, different per-message costs, different latency needs, different partition ceilings, or different failure domains** (§6). The most under-used option and often the correct one — it makes each workload independently tunable and independently debuggable. |

---

## 9. Questions you should be able to answer

- Two triggers propose 3 and 8 replicas. How many pods do you get? *(8 — the HPA takes the maximum, never the sum.)*
- Your consumer drains two topics and both are busy. Why is plain max-of-triggers wrong, and what fixes it? *(It under-provisions because the real workload is the sum; a `scalingModifiers` formula adding the two lags fixes it.)*
- What's the difference between `AverageValue` and `Value`? *(`AverageValue` divides the metric across pods — `ceil(value/target)`, independent of current replicas. `Value` corrects proportionally — `ceil(currentReplicas × value/target)`. Use `AverageValue` when adding a pod mechanically reduces the metric.)*
- Which metric types do cpu and memory triggers accept? *(`AverageValue` and `Utilization` only — not `Value`. External metrics are the reverse: `AverageValue` and `Value`, never `Utilization`.)*
- What does the HPA see once `scalingModifiers` is active? *(One external metric, `composite-metric`. Per-trigger external metrics are gone — which breaks dashboards keyed on them.)*
- When does `fallback` engage? *(After `failureThreshold` consecutive scaler **errors** — not when the metric legitimately reads zero.)*
- Your Kafka cluster is unreachable and you have no fallback. What does the HPA do? *(Skips scaling entirely whenever the readable metrics suggest scaling down, pinning the workload at its current size — silently and indefinitely.)*
- Which `fallback.behavior` for a Kafka consumer that must never shrink while blind? *(`currentReplicasIfHigher` — hold, but come up to a floor if the outage caught you small.)*
- Why can't a `Value`-type fallback rescue a workload sitting at zero? *(Its value is computed against the ready replica count; with zero ready replicas the fallback is abandoned. Use `AverageValue`.)*
- You added a CPU trigger and the workload stopped reaching zero. Why? *(cpu/memory triggers are always active, and KEDA only deactivates when every trigger is inactive.)*
- When should two signals become two Deployments instead of two triggers? *(Different scaling shapes, per-message costs, latency requirements, partition ceilings, or failure domains — anything that makes a single `behavior` block, `lagThreshold` or `maxReplicaCount` wrong for one of them.)*

---

## 10. Sources

- [KEDA — ScaledObject specification (`triggers`, `metricType`, `fallback`, `fallback.behavior`, `advanced.scalingModifiers`)](https://keda.sh/docs/2.20/reference/scaledobject-spec/)
- [KEDA — Scaling Deployments: scaling modifiers examples, activating vs scaling thresholds](https://keda.sh/docs/2.20/concepts/scaling-deployments/)
- [KEDA — Cron scaler (`start`, `end`, `timezone`, `desiredReplicas`)](https://keda.sh/docs/2.20/scalers/cron/)
- [KEDA — CPU scaler](https://keda.sh/docs/2.20/scalers/cpu/) · [Memory scaler](https://keda.sh/docs/2.20/scalers/memory/)
- [KEDA — Apache Kafka scaler](https://keda.sh/docs/2.20/scalers/apache-kafka/)
- [KEDA — Prometheus metrics (scaler error counters)](https://keda.sh/docs/2.20/integrations/prometheus/)
- [KEDA — admission webhooks](https://keda.sh/docs/2.20/operate/admission-webhooks/)
- [`expr` — language definition (the formula language)](https://expr.medv.io/docs/Language-Definition)
- [Kubernetes — HorizontalPodAutoscaler walkthrough: scaling on multiple metrics, and skipped scaling on unreadable metrics](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/)
- [Kubernetes — HorizontalPodAutoscaler v2 API reference](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/horizontal-pod-autoscaler-v2/)
