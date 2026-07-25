---
title: 18 - Interview Drill — KEDA & Kafka Autoscaling Q&A
tags:
  - keda
  - kafka
  - autoscaling
  - kubernetes
  - interview
  - senior
aliases:
  - KEDA Interview Questions
  - KEDA Drill
  - Kafka Autoscaling Q&A
  - KEDA Recall
status: ready
created: 2026-07-25
---

# Interview Drill — KEDA & Kafka Autoscaling Q&A

> [!abstract] Scope
> **Recall, not instruction.** Every answer in this module is taught somewhere in modules [[01 - Why Event-Driven Autoscaling — The Limits of HPA]]–[[17 - LAB 4 — Production-Grade Kafka Autoscaling Capstone]]; here they are compressed into graded question→answer sets you can drill in twenty minutes. Two things make this different from a list of trivia. First, wherever there is a **common wrong answer**, it is named — because in an interview the wrong answer is what you'll reach for under pressure. Second, there is a tier of **judgment** questions at the end, which is where senior candidates are actually separated from competent ones: not "what is a ScaledObject" but "should you be autoscaling this at all".

Related: [[00 - KEDA Index]], [[01 - Why Event-Driven Autoscaling — The Limits of HPA]], [[02 - Kafka Consumer Groups, Partitions & Rebalancing]], [[03 - KEDA Architecture — Operator, Metrics Adapter & the HPA It Owns]], [[07 - The Kafka Scaler — Every Field & the Replica Math]], [[08 - Scale-to-Zero & Activation]], [[10 - Tuning Scaling Behaviour]], [[14 - Observability & Debugging — Why Isn't It Scaling]], [[15 - Production Operations]], [[16 - Alternatives & Trade-offs]]

> [!warning] Written against KEDA v2.20.1 · Kubernetes v1.33–v1.35 · Apache Kafka 4.3.x
> Field names, defaults and metric names below are current for those versions. KEDA's CRDs are still `v1alpha1` and the maintainers have said they will stay there, so **breaking changes ship in minor releases** — an answer that was right on v2.17 can be wrong on v2.20. In an interview, say which version you're describing; it reads as experience, not hedging.

> [!important] How to use this
> **Grades.** `L1` = you use KEDA. `L2` = you own it in production. `L3` = senior signal — judgment and trade-offs, not recall. **`⚠`** marks a question with a widely-believed wrong answer; the wrong answer is named in the response so you can hear yourself avoiding it.
>
> Drill it out loud. Every answer here fits in three sentences, and three sentences is what an interview gives you. If yours takes thirty, you don't know it yet — go back to the linked module.

---

## 1. HPA fundamentals, and why event-driven at all

Source module: [[01 - Why Event-Driven Autoscaling — The Limits of HPA]]

**1 · L1 — Write the HPA's replica formula from memory.** `desiredReplicas = ceil[ currentReplicas × ( currentMetricValue / desiredMetricValue ) ]`. Then clamped to `[minReplicas, maxReplicas]`, changes inside a ~10% tolerance band ignored, then stabilisation windows and rate policies applied. The HPA is a divider, not an autoscaler with opinions.

**2 · L1 — Name the HPA's three metric types and say which one KEDA uses.** `resource` (metrics-server, reading the kubelet), `custom` (a custom-metrics adapter, per-pod or per-object), `external` (an external-metrics adapter, scoped to nothing in the cluster). KEDA registers as an **external** metrics provider. The distinction that matters: resource and custom metrics describe your pods, external metrics describe the world — which is why an external metric can still have a value at zero replicas.

**3 · L1 — Give two cases where CPU misleads a queue consumer, in opposite directions.** I/O-blocked consumer: 800 ms downstream call, backlog in the hundreds of thousands, CPU at 3% → a CPU-target HPA **scales down**. Fat Avro payloads on a drained topic: CPU pinned at 100%, nothing in the queue → it **scales up**. CPU utilisation and queue depth are independent variables; you can have any of the four combinations.

**4 · L2 — Frame the argument for backlog-based scaling without mentioning cost.** Backlog is the only signal with a direct relationship to the SLO: queue depth divided by drain rate *is* your latency. CPU has no such relationship. Cost is a side effect, not the reason.

**5 · L1 ⚠ — Can an HPA have `minReplicas: 0`?** Only with the `HPAScaleToZero` alpha feature gate, which has been alpha since Kubernetes 1.16, is off by default, and requires flags on `kube-apiserver` and `kube-controller-manager` that no managed provider will give you. ✗ *Wrong:* "yes, if you set it" — treat it as **unavailable**, not merely risky.

**6 · L2 — Why can a Prometheus-Adapter setup scraping consumer lag never scale from zero?** Because client-side lag (`records-lag-max`) is **published by the consumer**. At zero replicas there is no pod, no exporter, no scrape target, no data point — the number that would wake the workload is emitted by the workload. KEDA asks the *broker* instead, from the operator, which is always running. Generalise it: **scale-to-zero requires a metric source outside the thing being scaled.**

**7 · L1 ⚠ — Does KEDA replace the HPA?** No. It **creates, owns, updates and garbage-collects** an HPA per ScaledObject, feeds it an external metric, and handles only the transitions the HPA cannot make. ✗ *Wrong:* "KEDA is an alternative autoscaler." Practical consequence: odd behaviour between 1 and N is an **HPA** debugging problem; stuck at or refusing to reach 0 is a **KEDA** problem.

**8 · L2 ⚠ — Can KEDA and the Prometheus Adapter coexist in one cluster?** Only if the adapter serves **`custom.metrics.k8s.io`** exclusively. `external.metrics.k8s.io` is a single `APIService`, and Kubernetes supports exactly one provider of it per cluster — whoever registers last wins, and every HPA depending on the loser silently gets `<unknown>`. Check `kubectl get apiservice v1beta1.external.metrics.k8s.io` **before** installing anything.

**9 · L2 — What happens if the HPA and the VPA both watch CPU?** A feedback loop: VPA lowers the request → measured utilisation rises → HPA adds pods → per-pod load falls → VPA lowers the request again. Never point them at the same resource. KEDA on lag plus VPA on CPU/memory is the clean, non-conflicting combination — provided you didn't also add a `cpu` trigger to the ScaledObject.

**10 · L1 — A CPU-target HPA shows `<unknown>/70%` and does nothing. What's missing?** metrics-server. `kubectl top pods` will fail too, and `kubectl describe hpa` names the missing metric in its `ScalingActive` condition.

---

## 2. Kafka consumer groups, partitions, rebalancing, lag

Source module: [[02 - Kafka Consumer Groups, Partitions & Rebalancing]]

**11 · L1 — How many consumers in a group can usefully read a 12-partition topic?** Twelve. Each partition is assigned to **exactly one** consumer in the group; member thirteen joins, is assigned nothing, and polls an empty assignment forever while holding memory and a broker connection.

**12 · L1 ⚠ — One partition is hot and its lag is growing. Does scaling out help?** No. That partition still has exactly one owner, so 40 pods gives you one working and 39 idle. ✗ *Wrong:* "add replicas." The fix is the **producer's partition key**, or repartitioning. Autoscaling amplifies good partitioning; it cannot rescue bad partitioning.

**13 · L1 — What exactly does KEDA subtract to get lag?** End offset minus the group's **committed** offset, per partition, summed. Not the consumer's in-memory position — that's `records-lag-max`, which is position-based, so your application dashboard and KEDA will legitimately disagree and neither is wrong.

**14 · L2 — `kafka-consumer-groups.sh --describe` prints `-` for CURRENT-OFFSET on a partition. What does that mean?** No committed offset exists for that group on that partition, so lag is **undefined** — which is the state of every brand-new group. KEDA has to invent a number, and `offsetResetPolicy` decides what it invents.

**15 · L2 — Your consumer is processing happily but lag never falls. First check?** Whether `CURRENT-OFFSET` is advancing — run `--describe` twice, ten seconds apart. If it isn't, the consumer isn't committing, and KEDA scaling to `maxReplicaCount` is the *correct* response to what it can see. It's an application bug, and no autoscaling setting fixes it. Keep the commit interval well below `pollingInterval`.

**16 · L1 — Name four things that trigger a rebalance, and the implication for autoscaling.** A member joins; a member leaves or fails; the partition count of a subscribed topic changes; a subscribed topic is created or deleted. Therefore **every scale event is a rebalance** — autoscaling a consumer group is not free the way autoscaling a stateless HTTP service is.

**17 · L2 ⚠ — What is the default partition assignor in Kafka 4.x, and is it eager or cooperative?** The default list is `[RangeAssignor, CooperativeStickyAssignor]`, which **uses RangeAssignor** — eager, stop-the-world. ✗ *Wrong:* "cooperative is the default now." The list is a migration aid: cooperative only takes effect once `RangeAssignor` is removed from every member. Of the built-ins, only `CooperativeStickyAssignor` is cooperative.

**18 · L3 — You deployed `CooperativeStickyAssignor` and rebalances are still stop-the-world. Why?** The group's effective protocol is the one **common to all members**. One straggler pod with the default mixed list keeps the entire group eager — no warning, no error, no metric. Verify against the live group with `--describe --group <g> --state`, not against your config.

**19 · L2 ⚠ — Is KIP-848 on by default?** GA and enabled **on the broker** since Kafka 4.0 (via the `group.version` feature flag), but **off in the client** — you must set `group.protocol=consumer`, and the 4.3 client still defaults to `classic`. ✗ *Wrong:* "we're on 4.x so we have it." An upgraded cluster full of upgraded clients can be running the old protocol with nobody having decided.

**20 · L3 — What becomes unusable when you set `group.protocol=consumer`?** `heartbeat.interval.ms`, `session.timeout.ms`, `partition.assignment.strategy`, and programmatic `enforceRebalance()` — the broker now drives timing and assignment (`group.consumer.heartbeat.interval.ms` default 5000, `group.consumer.session.timeout.ms` default 45000). Most production consumers set at least the timeouts, so this is not a config-neutral flip. Custom client-side assignors can't be carried over at all.

**21 · L2 — How long does a pod that ignores SIGTERM keep its partitions after being killed?** Until `session.timeout.ms` expires — **45 s** by default, with heartbeats every 3000 ms. A consumer that calls `close()` sends an explicit LeaveGroup and the group reacts immediately. That difference is entirely down to your container handling SIGTERM.

**22 · L3 — Why is static membership (`group.instance.id`) a poor fit for a KEDA-scaled Deployment?** Because it prioritises state persistence over liveness: a scaled-in static member's partitions are only reassigned after the session timeout (whose ceiling KIP-345 raised to 30 minutes), and nobody is coming back. It also needs durable per-replica identity, so a StatefulSet. Be honest about epistemic status — **neither project documents the interaction**; this is inference from two verified facts.

**23 · L3 ⚠ — How much faster is a cooperative rebalance?** Trick question: **Apache publishes no quantified figure.** The official statements are proportional — the new protocol "decreases rebalance times", cooperative rebalancing avoids stopping the world. ✗ *Wrong:* quoting a vendor "20× faster" multiplier. Say "it removes the global synchronisation barrier" and you're on citable ground. ("Rebalance storm" is likewise vendor vocabulary, and originally about Kafka Connect.)

**24 · L3 — Two Deployments share one `group.id`, each with its own ScaledObject. What goes wrong?** Both compute replicas from the *same* total group lag as though each owned all of it, so both scale to the partition count — 24 members on 12 partitions, half idle. It also blocks any assignor migration, since the group is only cooperative when every member is. One consumer group per ScaledObject, always.

**25 · L2 — You add partitions to relieve the ceiling. What else did you just do?** Triggered a group-wide rebalance, and changed the key→partition mapping for hash-partitioned producers — so **per-key ordering is not preserved across the change** and two pods can briefly process the same key concurrently. Partitions can never be removed. Treat it as a planned change, not a capacity tweak.

**26 · L2 — Two consumer configs that make autoscaling safe, and why.** `max.poll.records` (default 500) bounds the uncommitted window and keeps each poll inside `max.poll.interval.ms` (default 300000). And `terminationGracePeriodSeconds` above your worst-case batch time, with a SIGTERM handler that stops polling → finishes the batch → commits → `close()`s. Neither is a KEDA setting; both are preconditions for autoscaling.

---

## 3. KEDA architecture, and who owns which scaling step

Source modules: [[03 - KEDA Architecture — Operator, Metrics Adapter & the HPA It Owns]], [[04 - The ScaledObject CRD, End to End]]

**27 · L1 — Name KEDA's three components and say which one connects to Kafka.** Operator, metrics API server, admission webhooks — plus six CRDs. **Only the operator** talks to your event source; the metrics adapter has no Kafka client in it. That shapes your network policies, your broker connection count and your credential blast radius.

**28 · L2 — Trace an HPA metric read end to end.** HPA controller (syncing ~every 15 s) → API server → aggregation layer (`external.metrics.k8s.io` isn't built in) → KEDA's metrics API server → **the operator**, over an internal mutually-authenticated hop → the value the scaler last collected. KEDA does not call Kafka at read time; a background loop already did, every `pollingInterval`. So your metric is at most one `pollingInterval` old.

**29 · L1 ⚠ — `kubectl get hpa` shows `MINPODS 1` but your `minReplicaCount` is 0. Bug?** No, and this is the mechanism. The generated HPA **always** has `minReplicas: 1` so it never sees zero; the operator writes `replicas: 0` to the `scale` subresource itself, behind the HPA's back. ✗ *Wrong:* "KEDA failed to apply my min."

**30 · L1 — Who owns 0→1? Who owns 4→9?** The **KEDA operator** owns 0↔1 (and N→0), writing the scale subresource directly. The **HPA controller** owns all 1↔N arithmetic, stabilisation windows, policies and tolerance. Identify the transition before you touch a field — that is most of the debugging.

**31 · L2 — What happens if you delete the generated HPA by hand?** The operator recreates it on the next reconcile. It carries an `ownerReference` to the ScaledObject, so deleting the ScaledObject garbage-collects it. Treat the HPA as read-only output; change it via `spec.advanced.horizontalPodAutoscalerConfig`.

**32 · L2 ⚠ — Will three operator replicas make KEDA react faster?** No. The operator is **leader-elected** — exactly one replica reconciles and polls. Extra replicas only shorten the failover window. ✗ *Wrong:* "scale the operator for throughput." Supported counts are **2 operator, 1 metrics server**; the metrics server is capped at one because only one service can serve the external-metrics API. Scale KEDA *vertically*.

**33 · L2 — Why does a `cpu` trigger behave differently from a `kafka` trigger?** KEDA writes a plain `Resource` metric into the HPA and steps out; the value comes from metrics-server, never through KEDA's metrics path. So debugging a CPU trigger is HPA debugging, and a cpu/memory-only ScaledObject with `minReplicaCount: 0` is rejected by the webhook outright.

**34 · L1 — Defaults for `minReplicaCount` and `maxReplicaCount`?** **0** and **100**. The 100 is rarely what you want: omit it and a runaway backlog marches a Kafka consumer to 100 pods on a 12-partition topic, 88 of them idle. Always state it.

**35 · L2 — What does `idleReplicaCount: 0` with `minReplicaCount: 5` achieve?** The warm-pool pattern: nothing while idle, then **five pods at once** on the first event instead of one pod waiting for the HPA to ramp. Only `0` is supported for `idleReplicaCount` and it must be strictly less than `minReplicaCount`, so the only shape it can take is "idle at zero, floor at N".

**36 · L2 — What does `metricType: Value` do to a lag threshold?** It compares the raw total instead of the per-pod average: `ceil(currentReplicas × value / target)` rather than `ceil(value / target)`. Lag divides across pods, so `AverageValue` (the default) is right for Kafka. The test is: **does adding a pod mechanically reduce the metric?** Yes → `AverageValue`. Average latency or age → `Value`.

**37 · L2 — Your brokers complain about request volume from KEDA. Cheapest fix?** `useCachedMetrics: true` **per trigger**. By default the HPA's ~15 s sync causes a fresh scaler query *on top of* the polling loop; with caching the event source is contacted at `pollingInterval` cadence only. It's rejected for `cpu`, `memory` and `cron` triggers, so put it on the Kafka trigger only.

**38 · L2 — Why would KEDA reject a 58-character ScaledObject name?** Since v2.20 both the name and the **derived HPA name** must fit in 63 characters, and KEDA prefixes `keda-hpa-` (9 chars) — so the practical limit is **54**. A name created on v2.19 survives the upgrade and then fails the next time anything re-applies it, including an unrelated GitOps sync.

**39 · L2 ⚠ — Your obviously-invalid ScaledObject was accepted. Why?** The `ValidatingWebhookConfiguration` ships with **`failurePolicy: Ignore`**, so an unavailable or slow webhook admits the resource unvalidated; it then fails at reconcile time in the operator log, where nobody is looking. ✗ *Wrong:* "the webhook validated it, so it's fine." Set `webhooks.failurePolicy=Fail` if you want the guarantees — and then alert on the webhook's availability.

**40 · L2 ⚠ — How do you un-pause a ScaledObject?** **Remove** the annotation: `kubectl annotate so <name> autoscaling.keda.sh/paused-` (trailing hyphen). ✗ *Wrong:* setting it to `"false"`. KEDA treats anything unparseable as `true`, and `paused-replicas` pauses on **mere presence** — its value is the replica count, so there is no value of it that means "not paused."

**41 · L2 ⚠ — You delete a ScaledObject while the workload is at zero. What's the replica count afterwards?** **Zero, forever.** `advanced.restoreToOriginalReplicaCount` defaults to `false`, the HPA is garbage-collected, and nothing is left watching. ✗ *Wrong:* "deleting it hands control back." This is one of the most common real KEDA incidents, and it happens exactly when you'd do it — cleaning up an idle workload out of hours. To disable autoscaling temporarily, **pause** instead.

**42 · L2 ⚠ — Why is pinning KEDA's version non-negotiable?** Because the CRDs are permanently `v1alpha1` — the maintainers declined to graduate the API on capacity grounds — so KEDA isn't bound by Kubernetes' deprecation policy and **breaking changes ship in minor releases**, under the project's own two-release window. v2.18, v2.19 and v2.20 each removed previously-valid fields. ✗ *Wrong:* "2.19→2.20 is a routine patch."

**43 · L2 — `Ready: True`, `Active: False`, zero pods. Is something wrong?** No — that's the correct idle state of a scale-to-zero workload. The four conditions are `Ready` (spec valid, scalers built, HPA healthy), `Active` (there is work right now), `Fallback` (a scaler is failing and `spec.fallback` is in force — always alert), `Paused`.

---

## 4. The Kafka scaler and the replica math

Source modules: [[07 - The Kafka Scaler — Every Field & the Replica Math]], [[08 - Scale-to-Zero & Activation]]

**44 · L1 ⚠ — Which Kafka trigger type is the stable one?** **`type: kafka`** — stable since KEDA v1.0, vendor-maintained, complete auth support. `type: apache-kafka` is **Experimental** (introduced v2.12 and still labelled experimental eight releases later). ✗ *Wrong:* assuming `apache-kafka` is the modern one, because **the doc page `/scalers/apache-kafka/` documents `type: kafka`** — the slug and the trigger name don't match. The experimental one's single real advantage: several topics in one trigger.

**45 · L1 ⚠ — Who computes the replica count — KEDA or the HPA?** The **HPA**. KEDA reports one number (total consumer-group lag) with an `AverageValue` target of your `lagThreshold`, and the HPA divides: `replicas = ceil(totalLag / lagThreshold)`. ✗ *Wrong:* "the scaler decides how many pods." KEDA only takes over at the edge the HPA can't reach — crossing zero.

**46 · L2 ⚠ — How does KEDA enforce the partition ceiling?** **By truncating the metric it reports**, not by capping the pod count. When true lag implies more pods than there are partitions, KEDA reports `partitions × lagThreshold` — a number chosen precisely so the HPA's own division lands on the partition count. ✗ *Wrong:* "KEDA clamps `maxReplicas`."

**47 · L2 — Kafka says lag is 90,000 and KEDA's metric says 6,000. Explain.** A 12-partition topic with `lagThreshold: 500` → `12 × 500 = 6,000`, and it stops moving no matter how far behind you fall. Not a stuck exporter, not a bug. The consequence is the one that matters: **you cannot use KEDA's reported metric as your lag alert** — it is capped by design and looks healthy during a genuine backlog emergency.

**48 · L2 ⚠ — What do `excludeIdlePartitions` and `topicPartitionRebalance` do?** Nothing — **neither field exists.** They appear in blog posts and AI-generated configs. What people usually want is **`limitToPartitionsWithLag`** (cap pods at the number of partitions with actual backlog) or **`excludePersistentLag`** (discount partitions whose committed offset isn't moving). Check any field name against the scaler docs for your version before concluding a feature is broken.

**49 · L2 ⚠ — A 13-partition topic jumps straight from 1 pod to 13 with nothing in between. Why?** `ensureEvenDistributionOfPartitions: true` restricts the pod count to **integer divisors** of the partition count, and a prime has only two: 1 and itself. 12 gives you 1, 2, 3, 4, 6, 12. ✗ *Wrong:* "the HPA is misbehaving." Only enable it on a well-factored partition count.

**50 · L2 — When is `allowIdleConsumers: true` legitimate?** Essentially one case: your consumer isn't a plain group member — it assigns partitions manually, or each pod runs several consumer instances internally — so the partition count no longer describes your real parallelism. Otherwise leave it `false` **and** pin `maxReplicaCount` to the partition count anyway, so the intent is documented and someone flipping the flag while debugging can't hurt you.

**51 · L2 — What happens if you omit `topic`?** KEDA discovers topics from the consumer group, and the two scalers do it very differently: the stable `kafka` scaler uses topics the group has committed offsets for; the experimental `apache-kafka` scaler walks **every topic in the cluster**, producing a baseline unrelated to your workload that never reaches zero — and at zero replicas the group has no members, the scaler errors, and the workload **can't come back up**. Always set `topic` explicitly.

**52 · L2 — Two topics, one consumer pool: one trigger each, or one trigger with both?** One trigger per topic → each proposes a count and the HPA takes the **highest**; correct when topics differ in volume or per-message cost. One trigger with both → lag is **summed** and divided once; correct only when they're similar. Summing lets a busy topic starve a quiet latency-sensitive one, and the extra pods may not even be assigned to the starving topic's partitions.

**53 · L1 — `activationLagThreshold` versus `lagThreshold`?** `activationLagThreshold` is evaluated **inside the operator** and produces a boolean: is there enough work to leave zero? `lagThreshold` becomes the **HPA's** metric target and drives how many pods once running. Neither has any say in the other's job.

**54 · L2 — Total lag is exactly 20 and `activationLagThreshold` is 20. Does it wake?** No — the comparison is **strictly greater than**. 21 wakes it. If you want "wake on ten messages", write `9`.

**55 · L2 ⚠ — You set `activationLagThreshold: 100` and nothing changes. Why?** `minReplicaCount >= 1`, so there is no zero boundary to cross and the activation value is **ignored entirely** — the scaler is treated as permanently active. ✗ *Wrong:* "it's a bug" or "raise it further." On an always-on workload the knobs are `lagThreshold` plus the HPA's scale-up policies.

**56 · L1 ⚠ — What does `cooldownPeriod: 600` do to a workload scaling from 10 pods to 2?** **Nothing.** `cooldownPeriod` (default 300 s) governs *only* the final step to 0 or to `idleReplicaCount`, measured from the last time a trigger was active. ✗ *Wrong:* "it's the scale-down delay." N→1 is the HPA's `behavior.scaleDown.stabilizationWindowSeconds`. This is the single most common wasted change in KEDA tuning.

**57 · L2 — What is `initialCooldownPeriod` for?** A workload that has **never** been active has nothing for `cooldownPeriod` to measure from, so a freshly created ScaledObject over an empty topic can be taken to zero almost immediately. `initialCooldownPeriod` (default 0) measures from the ScaledObject's creation instead — set ~300 during a first rollout so a new consumer gets a chance to prove itself.

**58 · L2 — A brand-new consumer group with `offsetResetPolicy: latest` and `scaleToZeroOnInvalidOffset: true` never starts. Why?** Deadlock. No committed offsets → every partition reports lag 0 → never activates → no consumer → still no offsets. `Ready=True`, no errors, zero replicas, nothing processed, indefinitely. Leave `scaleToZeroOnInvalidOffset` at `false` on any group that might be new, or seed the offsets first at `minReplicaCount: 1`.

**59 · L2 — Why does a fresh 12-partition topic report lag 12 rather than 0?** With the default pair `latest` + `scaleToZeroOnInvalidOffset: false`, each uncommitted partition contributes a **placeholder lag of 1** — deliberately conservative, so one consumer stays alive long enough to join, get an assignment and start committing.

**60 · L2 — A workload idle for nine days wakes up and reprocesses everything. What happened?** Kafka expired the group's committed offsets after `offsets.retention.minutes` (default **10080** = 7 days), and the clock starts **when the group becomes empty** — which is exactly what scaling to zero does. On wake-up the position is re-derived from `auto.offset.reset`: `earliest` replays the log, `latest` silently skips the backlog including the message that woke it.

**61 · L2 — Your drained 12-partition transactional topic reports lag 12 and never sleeps. Fix?** Transaction markers occupy offsets, compaction leaves offsets valid, and an in-flight transaction holds a `read_committed` consumer below the log end — so there's a permanent lag floor of roughly one per partition. `activationLagThreshold: 0` (the default) is therefore always exceeded. **Measure the floor**, then set the threshold above it; around the partition count is a good start.

**62 · L2 — Why must `offsetResetPolicy` match the consumer's `auto.offset.reset`?** Because they decide the same thing for two different readers, and disagreeing gives you either a phantom replay-sized backlog (scaler `earliest`, consumer `latest`) or a workload that never wakes (the reverse). It is the most common Kafka-on-KEDA bug; the `-` rows in `--describe` are the tell.

**63 · L2 — What's the catch with `excludePersistentLag`?** KEDA holds the comparison baseline **in memory**, so after any operator restart — upgrade, eviction, node drain — the first check has nothing to compare against and reports full lag once, which can spike your pod count. Treat it as a damper; the real fix for a poison message is a **dead-letter queue** so processing always moves the offset forward.

**64 · L2 — Name three costs of scaling to zero.** Cold-start latency (up to one `pollingInterval`, plus scheduling, image pull, runtime start, group join and a rebalance — well over a minute for a JVM consumer on a new node); the offset-expiry clock that starts when the group empties; and **you stop learning about failures** — a broken consumer at zero replicas is indistinguishable from a healthy idle one until traffic arrives.

---

## 5. Tuning, multiple triggers, fallback

Source modules: [[10 - Tuning Scaling Behaviour]], [[11 - Multiple Triggers, Scaling Modifiers & Fallback]]

**65 · L1 ⚠ — Your consumer oscillates between 4 and 6 pods. Which field do you change?** `advanced.horizontalPodAutoscalerConfig.behavior.scaleDown.stabilizationWindowSeconds` (and the scale-up window). ✗ *Wrong:* `cooldownPeriod` — it isn't consulted anywhere in that transition. The diagnostic shortcut: **is either end of the oscillation zero?** If not, you're in the HPA's territory.

**66 · L2 — Kubernetes' default scale-up stabilisation window is what, and why does it matter?** **Zero.** Scale-up is immediate by design — up to a doubling *or* +4 pods every 15 s, `selectPolicy: Max` — while scale-down has a 300 s rolling-maximum window. So the flapping people report is almost always **upward**, and the defaults have nothing damping it. On Kafka, where every step costs a rebalance, add `scaleUp.stabilizationWindowSeconds: 60` and cap steps with a `Pods` policy.

**67 · L2 — How do you derive `lagThreshold`?** Measure it: sum `CURRENT-OFFSET` across partitions, wait 60 s, sum again; divide by seconds and by pod count to get per-pod throughput; multiply by `pollingInterval`. One metric unit then means "one pod's worth of work", and `ceil(totalLag / lagThreshold)` becomes "how many pods would clear this in one interval". Round to something legible — precision here is false comfort.

**68 · L2 — Why does `lagThreshold: 1` feel twitchy and `lagThreshold: 50000` feel dead?** The HPA's ~10% tolerance band is **relative** to the target, and the target is `lagThreshold × replicas`. At `lagThreshold: 10` with 4 pods the dead band is ±4 messages; at 50,000 it's ±20,000. Neither is a bug — the number you chose *defined* the sensitivity.

**69 · L2 — Budget the worst-case reaction time from burst to new pod.** `pollingInterval` (default 30 s, and the two loops are unsynchronised) + one HPA sync (~15 s) + `scaleUp.stabilizationWindowSeconds` + pod scheduling and image pull + runtime start + group join and rebalance. Write that sum down before promising anyone a number — and note `useCachedMetrics: true` makes the `pollingInterval` staleness a guaranteed worst case rather than an occasional one.

**70 · L2 ⚠ — You set `pollingInterval: 5` and nothing changed.** `minReplicaCount >= 1` and no cached metrics, so KEDA's polling loop isn't in the scaling path at all. Since v2.20 the webhook emits a **non-blocking warning** saying so — the object is accepted, `kubectl apply` succeeds, and the warning scrolls past. Read apply output, don't just check the exit code.

**71 · L1 ⚠ — Two triggers propose 3 and 8 replicas. How many pods?** **8.** The HPA computes a count per metric and takes the **largest**. ✗ *Wrong:* 11. Expecting a sum is expensive in both directions — the under-provisioning is invisible at low volume and appears only when both sources are busy at once.

**72 · L2 — Your pool drains two topics and both are busy. Why is max-of-triggers wrong, and what fixes it?** The real workload is the **sum**, so max under-provisions: 2,600 + 900 lag against `lagThreshold: 1000` gives 3 pods under max and 4 under a sum. Fix with `advanced.scalingModifiers` — `formula: "orders_lag + shipments_lag"` plus a `target` — or split into two Deployments.

**73 · L2 — What does the HPA see once `scalingModifiers` is active?** **One** external metric named `composite-metric`. The per-trigger metrics (`s0-…`, `s1-…`) are gone, so dashboard panels and runbook `kubectl get --raw` recipes keyed on them go blank while the scaling itself works fine — which is how a broken runbook gets discovered mid-incident. Adopting a formula is an observability change too.

**74 · L2 — When does `fallback` engage?** After `failureThreshold` consecutive scaler **errors** — unreachable broker, auth failure, DNS, timeout. A scaler that successfully reports "lag is 0" has not failed; that's a healthy reading of an empty topic. Fallback is about *not knowing*, never about *knowing there's no work*.

**75 · L2 ⚠ — Your Kafka cluster is unreachable and you have no `fallback`. What does the HPA do?** Whenever the readable metrics suggest scaling **down**, it skips scaling entirely — it does not act on partial data. So one unreachable cluster **pins the whole workload at its current size, silently and indefinitely**: pods Running and Ready, no alert, dashboards normal, and if the outage began at peak you pay peak capacity round the clock. ✗ *Wrong:* "it falls back to the metrics it can read."

**76 · L2 — Which `fallback.behavior` for a Kafka consumer that must never shrink while blind, and why not `Value`-type triggers?** `currentReplicasIfHigher` — hold position, but come up to a floor if the outage caught you at 1 pod. A `Value`-type fallback is computed against the **ready replica count**, so at zero ready replicas the computation is meaningless and the fallback is abandoned; `AverageValue` (the default) has no such dependency.

**77 · L2 ⚠ — You added a CPU trigger as a safety net and the workload stopped reaching zero.** A cpu/memory trigger is **always active** — there is no CPU reading that means "no work" — and KEDA only deactivates when *every* trigger is inactive. So `minReplicaCount: 0` quietly stops having any effect. ✗ *Wrong:* "cooldown must be too long." (A cpu/memory-**only** ScaledObject with `minReplicaCount: 0` is at least rejected outright; mixing gets you past that guard with no warning.)

**78 · L3 — When should two signals become two Deployments rather than two triggers?** Different scaling shapes; different per-message cost (no single `lagThreshold` is correct for 5 ms and 5 s events); different latency requirements (opposite `behavior` blocks, and you get one per HPA); different partition counts (`maxReplicaCount` is one number); different failure domains (either cluster's outage freezes scale-down for both). The heuristic: **one ScaledObject per scaling *shape*, not per workload.**

---

## 6. Authentication, and ScaledJob

Source modules: [[12 - Authentication — TriggerAuthentication, SASL, TLS & MSK IAM]], [[13 - ScaledJob — When a Pod per Message Beats a Consumer Pool]]

**79 · L1 — Why can't a Kafka password live in the trigger's `metadata`?** It's plain text in a custom resource that gets treated as ordinary app config — readable by everyone who can `kubectl get so -o yaml`, which is a far wider group than those who can read Secrets, and it lands in Git, in `describe` output and in logs. There are no `metadata` fields for `username`/`password`; unrecognised keys are **ignored**, so the scaler connects unauthenticated and Kafka's error looks like a broker problem.

**80 · L2 — What's the real risk of a `ClusterTriggerAuthentication`?** There is no namespace allow-list. Any tenant who can create a ScaledObject can point `authenticationRef` at it and have KEDA use those credentials on their behalf — they can't *read* the credential, but they can aim it at a `consumerGroup` and `topic` of their choosing. Invisible in the tenant's own RBAC. Default to namespaced.

**81 · L2 ⚠ — Your Secret has `data: {password: "s3cr3t"}` and auth fails.** `data` requires base64; use **`stringData`**. ✗ *Wrong:* trusting the example — KEDA's own Kafka scaler docs show unencoded values under `data:`, and when a value happens to be valid base64 it's accepted and decoded into garbage, producing an auth error that points at Kafka rather than at your YAML.

**82 · L2 — Which SASL mechanisms force you onto the stable scaler?** `oauthbearer` (rejected outright on `apache-kafka` — "not implemented yet") and `gssapi`/Kerberos, which is worse: the value is **in the experimental scaler's schema**, so `kubectl apply` succeeds and the object reports `Ready`, then every poll errors because there's no implementation and no `keytab`/`realm`/`kerberosConfig` parameters at all. Validation passing is not evidence a mechanism works.

**83 · L2 — What does MSK IAM require beyond the role?** `awsRegion` **and** `tls: enable` — both validated, and the region feels redundant because the bootstrap address embeds it. On `type: kafka` also `sasl: oauthbearer` + `saslTokenProvider: aws_msk_iam`. And MSK IAM listens on port **9098**, not 9096 or 9094, so aiming it at the SASL_SSL port produces what looks like a network fault.

**84 · L2 — What Secret permissions does a default KEDA install have, and what breaks if you restrict them?** Cluster-wide `get`/`list`/`watch` on Secrets — it must read the tenant-namespace Secret a `TriggerAuthentication` points at. `KEDA_RESTRICT_SECRET_ACCESS: true` (on **both** operator and metrics server) removes that, and in exchange **namespaced `TriggerAuthentication` with `secretTargetRef` stops working entirely**, leaving only `ClusterTriggerAuthentication` with the Secret in KEDA's namespace. You've traded "one controller reads all Secrets" for "all credentials in one namespace, referenceable by any tenant." A trade, not a fix — the actual fix is pod identity.

**85 · L2 — You rotated the Secret, revoked the old credential, and scaling froze. Why?** KEDA caches the built scaler including its resolved credentials and open connection, rebuilding it when the ScaledObject changes or the scaler errors — so an in-place Secret update isn't re-resolved immediately. Force a rebuild by touching the ScaledObject (a throwaway annotation is enough), rotate with an overlap window, and pair it with `spec.fallback`.

**86 · L2 — Where do `sasl` and `tls` belong, given they're accepted in two places?** In the **`TriggerAuthentication`** only. Set in both, the stable `kafka` scaler lets the **ScaledObject silently win** — so a stale `sasl: plaintext` in `metadata` quietly defeats the `scram_sha512` in your Secret — while `apache-kafka` hard-errors. One home, and the config stays portable.

**87 · L1 — Structural difference between a ScaledObject and a ScaledJob?** A ScaledObject adjusts the replica count of an existing workload through an HPA it owns. A ScaledJob **creates Kubernetes Jobs** — one per unit of work — with `jobTargetRef` holding a full Job spec, and there is **no HPA at all**. KEDA decides the count itself on each poll.

**88 · L2 — Why is there no `cooldownPeriod`, `idleReplicaCount`, `fallback` or `advanced` on a ScaledJob?** They're all HPA concepts and there's no HPA. The consequence is real: when your brokers are unreachable, a ScaledObject with `fallback` holds a defined replica count, while a ScaledJob simply **creates no Jobs** — "no lag reported" and "can't reach Kafka" produce the same silent outcome, so you must alert on scaler errors yourself.

**89 · L2 — KEDA's own criterion for choosing ScaledJob?** **Long-running executions** — minutes to hours per unit of work: transcoding, an inference batch, a monthly report. Plus three others: you want `backoffLimit` retry semantics per item, per-message resource isolation, or traffic spiky enough that warm consumers are waste. For sub-second work a warm consumer is orders of magnitude better and no tuning closes the gap.

**90 · L2 — You bumped the image and 40-minute Jobs vanished mid-run.** The default rollout strategy **deletes** Jobs from the previous version and recreates them. Set `rollout.strategy: gradual` on any ScaledJob whose Jobs run longer than a few seconds — and accept that two versions then run concurrently. (The old top-level `rolloutStrategy` field is deprecated in favour of `rollout.strategy`.)

**91 · L2 — Which `scalingStrategy` fixes "messages waiting while Job slots are free", and why is `accurate` usually wrong for Kafka?** `eager` — it fills free slots up to `maxReplicaCount` while work remains, instead of subtracting long-running Jobs every poll. `accurate` assumes the metric **excludes** work already claimed, but Kafka lag doesn't fall until offsets are committed. `default` is the right start for Kafka.

**92 · L2 — Your namespace has 3,000 completed Pods.** `successfulJobsHistoryLimit` and `failedJobsHistoryLimit` both default to **100**, per ScaledJob. Set successes to 3–5, keep failures higher (10–20) as debugging evidence, and alert on failed Jobs rather than relying on the history to notice.

**93 · L2 — Your ScaledJob container loops until the topic is empty. What's wrong with that?** It's a consumer pool paying pod-per-message start-up cost with none of a Deployment's benefits: no HPA smoothing, no `fallback`, constant group-membership churn, unbounded per-Job work. A ScaledJob container must **read one unit of work, process it, and exit non-zero on failure**. Enforce it with `activeDeadlineSeconds` sized to one unit of work.

---

## 7. Observability and production operations

Source modules: [[14 - Observability & Debugging — Why Isn't It Scaling]], [[15 - Production Operations]]

**94 · L1 — Give the debugging drill in path order.** Is the ScaledObject `Ready`? → is it `Active` (and is `Active=False` actually correct)? → does the HPA exist and can it read its metric? → is `v1beta1.external.metrics.k8s.io` `Available`? → fetch the metric by hand → ask Kafka directly → *then* read operator logs. 80% of tickets die at the first two steps, and both are ten-second checks. **Debug in path order, not in panic order.**

**95 · L1 ⚠ — `kubectl get hpa` shows `<unknown>/100`. What does that tell you?** The metric could not be **retrieved at all** — the path is broken: APIService, KEDA metrics server, the hop to the operator, or the scaler. ✗ *Wrong:* "lag is zero, nothing to do." Real zero renders as `0/100`. `0` is a data point; `<unknown>` is an outage.

**96 · L2 — The single most decisive command when scaling isn't happening?** `kubectl get --raw` against `/apis/external.metrics.k8s.io/v1beta1/namespaces/<ns>/<metric>` with `labelSelector=scaledobject.keda.sh/name=<so>` — it asks exactly what the HPA asks, cutting KEDA's own reporting out of the loop. The selector is **mandatory**, and the metric name comes from `.status.externalMetricNames`, never from your memory.

**97 · L2 — Why does the HPA's `current` value look smaller than Kafka's lag even when nothing is clamped?** Because the default `metricType` is `AverageValue`, so `current` is **lag per replica**. `347` with 4 pods means roughly 1,388 total. Comparing it directly against `kafka-consumer-groups.sh` and concluding KEDA is wrong is a very common false alarm.

**98 · L2 ⚠ — Why must you never alert on `keda_scaler_metrics_value` for a Kafka trigger?** It's clamped at `partitions × lagThreshold` by design, so it **flatlines and looks healthy** during a real backlog — the alert is silently un-triggerable above the ceiling. ✗ *Wrong:* "it's KEDA's lag metric, so it's the obvious SLI." Alert on real lag from Kafka or a dedicated exporter. Rule of thumb: **alert on the event source and on KEDA's health; never on KEDA's reported value.**

**99 · L2 ⚠ — You enabled Prometheus and see no `keda_` series at all.** The **Helm chart defaults `prometheus.operator.enabled` (and `metricServer`, `webhooks`) to `false`**, even though the binary's own `--enable-prometheus-metrics` default is `true`. The endpoint is scrapeable and returns Go and controller-runtime metrics — just nothing `keda_`-prefixed — so people blame their scrape config for hours. Also note the `ServiceMonitor`/`PodMonitor`/`PrometheusRules` sub-trees each have their own `enabled: false`.

**100 · L2 ⚠ — Which KEDA metric names would you refuse to put in an alert rule?** Any you haven't confirmed against a live scrape. `keda_scaler_errors`, `keda_scaled_object_errors`, `keda_trigger_totals`, `keda_resource_totals` and `keda_internal_scale_loop_latency` are all widely cited and **not current names** — the real ones are `keda_scaler_metrics_value`, `keda_scaler_active`, `keda_scaler_detail_errors_total`, `keda_scaled_object_errors_total`, `keda_scaled_object_paused`. A rule on a name that doesn't exist evaluates to "no data" forever, which is indistinguishable from "everything is fine."

**101 · L2 — Which KEDA Grafana dashboard ID do you import?** None — **there is no KEDA dashboard on grafana.com.** Paste the first-party JSON from `config/grafana/keda-dashboard.json` in the KEDA repo, pinned to your version. Treat it as a diagnosis panel, not a monitoring dashboard: it shows what KEDA thinks, which for Kafka deliberately differs from what your broker thinks.

**102 · L3 — What are the four alerts worth having, and which one has no KEDA metric behind it?** Real consumer-group lag (your SLI); `rate(keda_scaler_detail_errors_total[5m])` sustained above zero; **ScaledObject not `Ready`**; and sustained time at `maxReplicaCount`. The third has no KEDA metric — you need a custom-resource exporter or a scripted `kubectl get so -A` check. Add `keda_scaled_object_paused` on a long window, because nothing else catches "someone paused it during an incident and never un-paused it."

**103 · L2 ⚠ — What does `helm uninstall keda` destroy?** The **CRDs** — and with them, by garbage collection, **every ScaledObject, ScaledJob, TriggerAuthentication, ClusterTriggerAuthentication and CloudEventSource in the entire cluster, in every namespace, belonging to every team.** The chart ships CRDs as ordinary templates with no keep policy, so ✗ `--skip-crds` **has no effect** (Helm's CRD flags only apply to a `crds/` directory the chart doesn't use). Install with `crds.install=false` and manage CRDs from the `-crds.yaml` asset; back up all CRs before touching the release. Workloads sitting at zero when the CRDs vanish **stay at zero**.

**104 · L2 — What breaks after upgrading to v2.20 with a hand-maintained restricted RBAC role?** Event recording. v2.20 moved to the `events.k8s.io` API group; nothing crashes and scaling still works, you just stop getting `KEDAScaleTargetActivated`/`Deactivated` events — precisely what you need during the *next* incident, and easy to misdiagnose as "KEDA stopped scaling." Grant `create`/`patch` on `events.k8s.io/events` **before** upgrading, and diff `helm template`'s ClusterRole against yours every upgrade.

**105 · L2 — What's the Kubernetes compatibility window, and why does it move without you?** Roughly three minors, "N-2" — v2.20 is tested against **v1.33–v1.35**. Managed providers force cluster minor upgrades on their own schedule, so a cluster upgrade can push you *out* of your pinned KEDA version's tested range without you touching KEDA. Nothing breaks loudly when that happens, which is the problem.

**106 · L2 ⚠ — A team's ScaledObject in a new namespace does nothing at all — no HPA, no events, no errors.** `watchNamespace` is set and their namespace isn't in the list. Resources outside it are ignored completely and **silently**: to the team KEDA is broken, to the platform team KEDA is healthy, and both are right. Prefer `watchNamespace: ""` and control access with RBAC on the CRDs instead. ✗ *Wrong:* "give each team its own KEDA" — one external-metrics provider per cluster makes that impossible.

**107 · L2 — Everything is green: `Ready`, `Active`, metric flowing, HPA says 20 replicas — and lag keeps climbing.** Pods are `Pending`. **KEDA scales pods, not nodes**, and both KEDA and the HPA consider their job done once the replica count is written. `kubectl get pods` versus the replica count, then `describe pod` → `FailedScheduling`. This belongs in your drill.

---

## 8. Senior signal — judgment, not recall

These are the ones that separate candidates. None has a lookup answer.

**108 · L3 — Name situations where you would *not* use KEDA.** Five. The workload is genuinely CPU- or memory-bound (a plain HPA is free and universally understood); the partition count is so low that horizontal scaling is pointless; a latency-critical path where a cold start is unacceptable; a very small deployment where operating another cluster-scoped controller with breaking changes in minor releases costs more than the compute saved; and a cluster already committed to another external-metrics provider, where adopting KEDA is a migration project rather than an install. Being able to argue *against* the sophisticated option is most of what judgment means here.

**109 · L3 — Your topic has 2 partitions and 2 pods can't keep up. What do you do?** Not more pods — the ceiling is hard. Either raise the partition count (a producer-side decision that reshuffles key→partition mapping and breaks per-key ordering across the change) or raise **concurrency inside each pod**. The latter is also the one legitimate reason to set `allowIdleConsumers: true`, because the partition count then no longer describes your parallelism.

**110 · L3 — Does a ScaledJob reduce Kafka rebalancing?** **No — it usually increases it, substantially.** A ScaledObject's consumers are long-lived; a ScaledJob's are maximally short-lived, joining on start and leaving on exit, so a thousand messages means roughly two thousand membership changes, each pausing the whole group. You didn't remove rebalancing; you made it the group's dominant activity. The workarounds cost real things: manual assignment means *you* own distribution (nothing stops two Jobs on one partition, or a partition with no owner), a distinct `group.id` per Job means offsets aren't shared and groups accumulate, and **either way the partition ceiling stops protecting you** because `allowIdleConsumers: false` is premised on group-managed assignment. The shape that actually works is a **dispatcher**: a small warm ScaledObject consumer group that turns messages into work items, with the ScaledJob triggering on *those*.

**111 · L3 — How do KEDA and Cluster Autoscaler / Karpenter interact?** As complementary layers, never alternatives. KEDA (via its HPA) answers "how many pods"; the node autoscaler answers "is there anywhere to put them" — and it only reacts once pods are already **Pending**, which is the whole source of the latency. The serial budget is: `pollingInterval` → HPA sync and policies → a failed scheduling attempt → node autoscaler notices → the cloud provider creates an instance (tens of seconds to minutes) → node joins, CNI ready, image pulled → pod starts and joins the group → rebalance. Tuning `pollingInterval` to 5 s to "react faster" is pointless if node provisioning takes two minutes. Best mitigation by a distance: **over-provisioning headroom** — low-priority pause pods holding real capacity that the scheduler evicts instantly — then right-sized requests, then smaller images.

**112 · L3 — What does scale-to-zero do to node consolidation, and vice versa?** Both directions bite. KEDA taking a workload to zero empties the node, so Karpenter or Cluster Autoscaler removes it — the saving you wanted, but the next activation now pays full node-provisioning cost, so a workload that wakes every ten minutes may spend most of its wake-up latency waiting for an instance. And consolidation cuts back: Karpenter can disrupt a node your consumer is happily running on, killing pods mid-batch and triggering a rebalance for reasons unrelated to load. Protect with a `PodDisruptionBudget`, `karpenter.sh/do-not-disrupt` where warranted, a grace period above worst-case batch time, and a SIGTERM handler that commits — a consolidation and a scale-down look identical to your consumer, so one piece of work covers both.

**113 · L3 — What does a prime partition count do to `ensureEvenDistributionOfPartitions`, and how do you avoid it?** It reduces the ladder to two rungs, 1 and N — a 13-partition topic goes straight from 1 pod to 13 the moment lag crosses the threshold. Pick composite counts at topic-creation time: **12, 24, 48 and 60 are excellent; 13, 17 and 31 are unusable here.** This is a good concrete reason to prefer 12 over 10 or 13 for anything you intend to autoscale, and it's free to get right up front and painful afterwards, because partitions can never be removed.

**114 · L3 — The flagship question: a Kafka consumer is at 40 replicas on a 12-partition topic and lag isn't falling. Walk the diagnosis.** Two problems, and name both. **The 40 is wrong:** `maxReplicaCount` was left at the default 100 and someone set `allowIdleConsumers: true` (or left the ceiling unpinned), so 28 pods hold connections and no partitions. **The lag not falling is a different bug**, and 40 pods proves it isn't capacity — so check whether `CURRENT-OFFSET` is advancing at all. If it isn't, the consumer isn't committing and KEDA is faithfully scaling on a signal that can never improve. If it is advancing, look for a hot partition (one owner, no help from replicas), an `offsetResetPolicy` mismatch inflating the number, or `Pending` pods meaning your 40 replicas are really 12 running ones.

**115 · L3 — Would you enable `KEDA_RESTRICT_SECRET_ACCESS` in a multi-tenant cluster?** Only with the trade stated out loud, because it isn't a strict improvement: you remove cluster-wide Secret read from the operator and in exchange force every credential into KEDA's own namespace behind a `ClusterTriggerAuthentication`, which **any tenant who can create a ScaledObject can then reference**. If the audit requirement is absolute and tenants are trusted, take it. Otherwise the answer that actually removes the problem is **pod identity** — no stored credential to read, rotate or leak — with an external secret store behind a workload identity where pod identity isn't available.

**116 · L3 — When is "just over-provision" the right answer?** When the running cost of autoscaling exceeds what it saves: a controller to version-pin and upgrade two to three times a year, a rebalance on every scale event, cold-start latency at the edges, and one more component in every incident's suspect list. For a consumer that peaks at three pods, four always-on pods and a README note explaining why is the senior answer, not the lazy one.

**117 · L3 — Someone proposes Cruise Control to fix consumer lag. Your response?** Separate the diagnoses first. Cruise Control rebalances **partition replicas across brokers** — broker capacity management. The rebalance that affects your lag moves **partitions across consumers**. If broker CPU, network or disk is saturated or one broker leads far more partitions than its peers, that's a Cruise Control problem; if the brokers are comfortable while committed offsets fall behind the log end, it's consumer capacity — KEDA, more partitions, or a faster consumer. `--describe` tells you which in about ten seconds. Same shape of confusion: Strimzi runs Kafka, it doesn't scale your consumer.

**118 · L3 — You're asked for a 5-second end-to-end SLO on an event-driven consumer. Can you scale to zero?** No, and say so plainly rather than tuning toward it. Cold start alone — up to one `pollingInterval`, plus scheduling, possible node provisioning, image pull, runtime start, group join and rebalance — exceeds the budget before any work happens. Also note that `activationLagThreshold: 20` means up to 20 messages can wait *indefinitely* under the threshold, which is its own SLO violation. The answer is `minReplicaCount` of 1 or 2, a short `pollingInterval`, `lagThreshold` sized to a *fraction* of a polling interval's work so a modest queue already demands a pod, and scale-down one pod at a time.

**119 · L3 — What do you check before a KEDA upgrade?** Confirm and pin the target version; read the release notes for **removals and deprecations**, not features (the most disruptive v2.20 change wasn't even in the Breaking Changes section); check the Kubernetes compatibility window against where your cluster will be in three months; grep your whole CR inventory for every removed field; upgrade non-production first and confirm every ScaledObject is still `Ready`; and finally `kubectl apply --dry-run=server` your entire inventory, because **new validation applies on CREATE and UPDATE** — existing objects keep running and only fail weeks later during an unrelated change. Upgrade with `-f values.yaml` from git, never `--reuse-values`, which silently skips new chart defaults.

---

## 9. Red flags in your own answer

Phrasings that tell an interviewer you've read about KEDA rather than run it. If you hear yourself saying one, stop and correct it in the same breath — self-correction reads as experience.

- **"KEDA scales the pods"** / "KEDA replaces the HPA." It reports a number; the HPA divides. KEDA owns only the zero crossing.
- **"cooldownPeriod controls scale-down."** It governs one transition: the final step to 0 or `idleReplicaCount`.
- **"We'll just raise `maxReplicaCount`."** On a partition-capped topic that buys idle pods and extra rebalances, nothing else.
- **"We scale on lag, so we're covered."** Lag is *committed*-offset based, ignores message size and cost, and has a permanent floor on transactional and compacted topics. Name where the proxy breaks.
- **"Cooperative rebalancing is about 20× faster."** Apache publishes no figure. Say "it removes the global synchronisation barrier."
- **"Scale-to-zero, so it's basically serverless."** Without naming cold-start latency and the 7-day offset-expiry clock, this says you've never run it.
- **"We use the `apache-kafka` scaler because it's the current one."** It's Experimental; the confusingly-named doc page describes `type: kafka`.
- **"We added a CPU trigger as a safety net."** Fine — but if you don't add "which means we gave up scale-to-zero," you didn't know.
- **"We alert on KEDA's lag metric."** It's clamped by design and flatlines during exactly the emergency you're alerting for.
- **"Karpenter/VPA instead of KEDA."** Different objects entirely: nodes, pod size, pod count. Treating them as alternatives is the tell.
- **Naming a field you haven't verified** — `excludeIdlePartitions`, `topicPartitionRebalance`, `keda_scaler_errors`. Fabricated names are the fastest way to lose credibility, and "I'd check the scaler docs for that version" is a strong answer.
- **"We'll pin the version later."** With `v1alpha1` CRDs and breaking changes in minors, this is the answer that gets followed up on.
- **Answering "why isn't it scaling" with a field name.** The right first move is "which transition am I looking at, and therefore which component owns it."

---

## 10. If you keep only five sentences

1. **KEDA owns 0↔1; the HPA owns 1↔N** — identify the transition before you touch a field.
2. **The scaler never computes replicas.** It reports one number, the HPA divides by `lagThreshold`, and the partition ceiling is enforced by **deliberately under-reporting** that number as `partitions × lagThreshold`.
3. **Every scale event is a Kafka rebalance**, so aggressive scale-up has a throughput cost — and Kubernetes applies no scale-up stabilisation by default.
4. **Lag is a proxy**: committed-offset based, blind to message cost and skew, with a floor on transactional and compacted topics — so alert on real broker-side lag, never on KEDA's metric.
5. **KEDA is a cluster-level singleton with a moving `v1alpha1` API**: one external-metrics provider per cluster, one active operator replica, breaking changes in minor releases, and `helm uninstall` takes the CRDs with it.

> [!tip] Drill it against something real
> Everything above is reproducible on a laptop. [[09 - LAB 3 — Break It on Purpose]] triggers the flapping, rebalance-churn, offset-mismatch, non-committing-consumer and never-reaches-zero failures deliberately, and [[17 - LAB 4 — Production-Grade Kafka Autoscaling Capstone]] makes you defend a config against an SLO. Answers you've *watched happen* come out at a different speed from answers you've read.

---

## 11. Sources

- [KEDA — ScaledObject specification](https://keda.sh/docs/2.20/reference/scaledobject-spec/) · [ScaledJob specification](https://keda.sh/docs/2.20/reference/scaledjob-spec/)
- [KEDA — Apache Kafka scaler (documents `type: kafka`)](https://keda.sh/docs/2.20/scalers/apache-kafka/) · [Apache Kafka (Experimental) scaler (documents `type: apache-kafka`)](https://keda.sh/docs/2.20/scalers/apache-kafka-go/)
- [KEDA — Scaling Deployments: activation vs scaling, caching, pausing](https://keda.sh/docs/2.20/concepts/scaling-deployments/) · [Scaling Jobs](https://keda.sh/docs/2.20/concepts/scaling-jobs/)
- [KEDA — Authentication and `TriggerAuthentication`](https://keda.sh/docs/2.20/concepts/authentication/)
- [KEDA — Cluster operations: HA limits, compatibility, ports, `KEDA_RESTRICT_SECRET_ACCESS`](https://keda.sh/docs/2.20/operate/cluster/) · [Admission webhooks](https://keda.sh/docs/2.20/operate/admission-webhooks/)
- [KEDA — Prometheus integration and metric names](https://keda.sh/docs/2.20/integrations/prometheus/) · [first-party Grafana dashboard JSON](https://github.com/kedacore/keda/blob/v2.20.1/config/grafana/keda-dashboard.json)
- [KEDA — FAQ (one external metrics server per cluster; don't pair a ScaledObject with your own HPA)](https://keda.sh/docs/2.20/reference/faq/) · [Troubleshooting](https://keda.sh/docs/2.20/troubleshooting/)
- [KEDA — Deploying KEDA (Helm, manifests, OLM)](https://keda.sh/docs/2.20/deploy/) · [Helm chart values](https://github.com/kedacore/charts/tree/v2.20.1/keda)
- [KEDA — CHANGELOG](https://github.com/kedacore/keda/blob/main/CHANGELOG.md) · [ROADMAP](https://github.com/kedacore/keda/blob/main/ROADMAP.md) · [governance/SUPPORT.md (the "N-2" window)](https://github.com/kedacore/governance/blob/main/SUPPORT.md) · [governance/DEPRECATIONS.md](https://github.com/kedacore/governance/blob/main/DEPRECATIONS.md)
- [Kubernetes — HorizontalPodAutoscaler walkthrough: the formula, multiple metrics, skipped scale-down, stabilisation, tolerance](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/)
- [Kubernetes — HorizontalPodAutoscaler v2 API (`minReplicas`, `behavior`, the `HPAScaleToZero` gate)](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/horizontal-pod-autoscaler-v2/)
- [Kubernetes — API aggregation layer](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/apiserver-aggregation/) · [Secrets (`data` vs `stringData`)](https://kubernetes.io/docs/concepts/configuration/secret/) · [Pod lifecycle and termination](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/) · [Jobs](https://kubernetes.io/docs/concepts/workloads/controllers/job/)
- [Apache Kafka 4.3 — consumer configuration (`partition.assignment.strategy`, `session.timeout.ms`, `max.poll.records`, `group.protocol`, `auto.offset.reset`)](https://kafka.apache.org/43/configuration/consumer-configs/)
- [Apache Kafka 4.3 — `KafkaConsumer` javadoc (one partition, one consumer; rebalance triggers)](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
- [Apache Kafka 4.3 — the new consumer rebalance protocol (KIP-848)](https://kafka.apache.org/43/operations/consumer-rebalance-protocol/) · [monitoring (`records-lag-max` is published by the consumer)](https://kafka.apache.org/43/operations/monitoring/)
- [Apache Kafka — `offsets.retention.minutes`](https://kafka.apache.org/documentation/#brokerconfigs_offsets.retention.minutes) · [KIP-211 (the expiry clock starts when the group empties)](https://cwiki.apache.org/confluence/display/KAFKA/KIP-211%3A+Revise+Expiration+Semantics+of+Consumer+Group+Offsets)
- [KIP-429 — incremental cooperative rebalancing (the "stop-the-world effect")](https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol) · [KIP-345 — static membership](https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A+Introduce+static+membership+protocol+to+reduce+consumer+rebalances)
- [Karpenter — disruption and consolidation](https://karpenter.sh/docs/concepts/disruption/) · [Kubernetes — Cluster Autoscaler](https://github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler) · [Pod priority and preemption (over-provisioning)](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-priority-preemption/)
- [Prometheus Adapter — custom and external metrics](https://github.com/kubernetes-sigs/prometheus-adapter) · [Knative Serving — autoscaling](https://knative.dev/docs/serving/autoscaling/) · [LinkedIn Cruise Control](https://github.com/linkedin/cruise-control) · [Strimzi — overview](https://strimzi.io/docs/operators/latest/overview.html)
- [AWS — IAM access control for Amazon MSK](https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html)
