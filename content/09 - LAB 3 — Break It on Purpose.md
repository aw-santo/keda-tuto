---
title: LAB 3 — Break It on Purpose
tags:
  - keda
  - kafka
  - autoscaling
  - kubernetes
  - lab
  - senior
aliases:
  - LAB 3
  - Break It on Purpose
  - KEDA failure modes lab
status: ready
created: 2026-07-25
---

# LAB 3 — Break It on Purpose

> [!abstract] Scope
> Six deliberate failures, run against the stack you already have. Each is a real production incident reproduced on your laptop in minutes, and each follows the same cycle: **Symptom → 🔴 Break it → 👀 Observe → 🔍 Diagnose → 🟢 Fix it → Lesson.** The fixes are all in [[07 - The Kafka Scaler — Every Field & the Replica Math]] and [[08 - Scale-to-Zero & Activation]] already. The point is that **you will have seen each symptom once**, so next time you recognise it in seconds instead of hours. The [[#8. Symptom → cause → fix]] table at the end is the artefact to keep.
>
> Assumes you finished [[05 - LAB 1 — Local Cluster, Kafka & KEDA from Scratch]] and [[06 - LAB 2 — Your First Kafka ScaledObject]] and the stack is running.

Related: [[00 - KEDA Index]], [[02 - Kafka Consumer Groups, Partitions & Rebalancing]], [[05 - LAB 1 — Local Cluster, Kafka & KEDA from Scratch]], [[06 - LAB 2 — Your First Kafka ScaledObject]], [[07 - The Kafka Scaler — Every Field & the Replica Math]], [[08 - Scale-to-Zero & Activation]], [[10 - Tuning Scaling Behaviour]], [[13 - ScaledJob — When a Pod per Message Beats a Consumer Pool]], [[14 - Observability & Debugging — Why Isn't It Scaling]]

> [!warning] KEDA v2.20 · Kubernetes 1.35 (kind node pinned) · Apache Kafka 4.3
> Everything below runs in namespace `lab`, against topic `orders` (12 partitions), Deployment `orders-consumer`, ScaledObject `orders-so`, broker pod `kafka-0` — exactly as Lab 1 built them.

---

## 0. Setup

### 0a. Make the consumer configurable

`lab/consumer.py` hard-codes the three settings you need to break. One small edit turns them into environment variables, so every experiment becomes a `kubectl set env` instead of a rebuild. Add these beside the other environment reads:

```python
RESET     = os.environ.get("AUTO_OFFSET_RESET", "earliest")
ASSIGNOR  = os.environ.get("ASSIGNOR", "cooperative-sticky")
COMMIT    = os.environ.get("ENABLE_AUTO_COMMIT", "true").lower() == "true"
LOG_EVERY = int(os.environ.get("LOG_EVERY", "20"))
```

and use them in three places:

```python
# in conf = { ... }, replacing the hard-coded values
    "auto.offset.reset": RESET,
    "enable.auto.commit": COMMIT,
    "partition.assignment.strategy": ASSIGNOR,

# first two lines of safe_commit() — without this, revoke and shutdown
# would still commit and Experiment 4 wouldn't break
    if not COMMIT:
        log.warning("commits DISABLED - not committing at %s", where)
        return

# the progress-report condition
    if processed % LOG_EVERY == 0 or time.monotonic() - last_report > 10:
```

Rebuild and load. **Never tag it `:latest`** — Kubernetes then defaults to `imagePullPolicy: Always` and kind-loaded images fail to pull:

```bash
docker build -t keda-lab-client:0.5 .
kind load docker-image keda-lab-client:0.5 --name keda-lab
kubectl -n lab set image deploy/orders-consumer consumer=keda-lab-client:0.5
kubectl -n lab rollout status deploy/orders-consumer
```

### 0b. Four shell helpers

Paste these into your working shell. They keep every experiment below to one or two lines.

```bash
tm() {   # tm <trigger-metadata-field> <value>   e.g. tm lagThreshold 20
  kubectl -n lab patch scaledobject orders-so --type=json \
    -p "[{\"op\":\"replace\",\"path\":\"/spec/triggers/0/metadata/$1\",\"value\":\"$2\"}]"
}

so() {   # so <spec-field> <value>               e.g. so cooldownPeriod 30
  kubectl -n lab patch scaledobject orders-so --type=json \
    -p "[{\"op\":\"replace\",\"path\":\"/spec/$1\",\"value\":$2}]"
}

produce() {   # produce <count> [msgs-per-sec, 0 = flat out]
  kubectl -n lab run "producer-$RANDOM" --rm -i --restart=Never \
    --image=keda-lab-client:0.5 --image-pull-policy=IfNotPresent \
    --env=KAFKA_BOOTSTRAP=kafka-0.kafka.lab.svc.cluster.local:9092 \
    --env=KAFKA_TOPIC=orders --env=MESSAGE_COUNT="$1" \
    --env=MESSAGES_PER_SEC="${2:-0}" -- /app/producer.py
}

lag() {       # lag [consumer-group]
  kubectl -n lab exec kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --group "${1:-orders-workers}"
}
```

`tm` and `so` use a JSON-patch `replace`, which only works on fields that already exist — every field used below is present in Lab 2's ScaledObject, so that's fine, and it fails loudly rather than silently if you typo a path.

### 0c. Three windows

```bash
# W1 — the control loop
watch -n2 'kubectl -n lab get scaledobject,hpa,deploy'

# W2 — every consumer's log, prefixed with its pod name
kubectl -n lab logs -l app=orders-consumer -f --prefix --tail=5 --max-log-requests=15

# W3 — where you type everything below
```

> [!tip] `--max-log-requests=15`
> The default is 5. You will run up to 12 consumers, and `kubectl logs -l` refuses to follow more than the limit — which looks exactly like "the new pods aren't logging."

### 0d. The reset ritual

Run `reset` between experiments. Resetting offsets **requires the group to have no active members**, hence the pause and scale-down first.

```bash
reset() {
  kubectl -n lab annotate scaledobject orders-so autoscaling.keda.sh/paused="true" --overwrite
  kubectl -n lab scale deploy/orders-consumer --replicas=0
  kubectl -n lab wait --for=delete pod -l app=orders-consumer --timeout=90s
  kubectl -n lab exec kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --group "${1:-orders-workers}" \
    --topic orders --reset-offsets --to-latest --execute
}
resume() { kubectl -n lab annotate scaledobject orders-so autoscaling.keda.sh/paused- ; }
```

> [!warning] Removing the pause annotation needs the trailing dash
> `autoscaling.keda.sh/paused-` **deletes** the annotation. Setting it to `"false"` also works for `paused`, but KEDA treats **any non-boolean value as `true`** — so `paused: "no"` still pauses. Build the habit of deleting.

---

## 1. Flapping

> **Symptom:** the replica count oscillates every few seconds, the consumer logs are nothing but rebalances, and throughput is worse than with one pod.

### 🔴 Break it

```bash
so pollingInterval 5
tm lagThreshold 1
produce 900 6          # a TRICKLE, not a burst — a burst just pins you at the ceiling
```

### 👀 Observe

```bash
kubectl -n lab get hpa keda-hpa-orders-so -w
```

```
NAME                 TARGETS      MINPODS   MAXPODS   REPLICAS
keda-hpa-orders-so   3/1 (avg)    1         12        3
keda-hpa-orders-so   11/1 (avg)   1         12        11
keda-hpa-orders-so   1/1 (avg)    1         12        2
keda-hpa-orders-so   6/1 (avg)    1         12        7
```

Each of those changes produces a burst of `REBALANCE` lines across *every* pod in W2, and the `processed=` counters visibly stall while it happens. `kubectl -n lab describe hpa keda-hpa-orders-so | tail -20` shows the same story as events.

### 🔍 Diagnose

The HPA computes `ceil(totalLag / lagThreshold)`. With `lagThreshold: 1`, **one message equals one pod** — nine messages arriving between polls is a nine-pod scale-up request. And Kubernetes applies **no stabilisation window to scale-up by default** (only to scale-down), so every request is acted on immediately. The count stops at 12 only because of the partition ceiling from [[07 - The Kafka Scaler — Every Field & the Replica Math]]; everything below 12 is free to jitter.

### 🟢 Fix it

Size `lagThreshold` to **one pod's work per polling interval** — and measure it, don't guess. The lab consumer sleeps `PROCESS_DELAY_MS=250`, so one pod clears ~4 msg/s; at `pollingInterval: 5` that's **20 messages per pod per interval**. Then damp scale-up:

```bash
tm lagThreshold 20
kubectl -n lab patch scaledobject orders-so --type=json -p \
 '[{"op":"replace","path":"/spec/advanced/horizontalPodAutoscalerConfig/behavior/scaleUp/stabilizationWindowSeconds","value":30}]'
produce 900 6
```

The count now moves in steps of one or two and settles.

> [!tip] Lesson
> `lagThreshold` is not a sensitivity dial — it is **the unit the HPA counts in**. Set it so "one unit of metric" means "one pod's worth of work" and the replica count becomes readable instead of noisy. Then remember scale-up has **no** default damping: you add `scaleUp.stabilizationWindowSeconds` yourself.

---

## 2. Rebalance churn

> **Symptom:** scaling "works" — pods come and go as expected — but total throughput drops, and consumption pauses completely for a second or two on every change.

### 🔴 Break it

Switch every consumer to the eager `range` assignor, then force churn by hand:

```bash
kubectl -n lab set env deploy/orders-consumer ASSIGNOR=range
kubectl -n lab rollout status deploy/orders-consumer
kubectl -n lab annotate scaledobject orders-so autoscaling.keda.sh/paused="true" --overwrite

produce 20000 &
for n in 2 6 2 8 3; do kubectl -n lab scale deploy/orders-consumer --replicas=$n; sleep 20; done
```

### 👀 Observe

The rebalance logs change shape completely. Cooperative — Lab 1's default — gives up only what it must:

```
[orders-consumer-a] REBALANCE -revoked=[0, 1, 2, 3, 4, 5] -> owns 6 partitions [6, 7, 8, 9, 10, 11]
[orders-consumer-b] REBALANCE +added=[0, 1, 2, 3, 4, 5]   -> owns 6 partitions [0, 1, 2, 3, 4, 5]
```

Eager makes **everyone drop everything first**:

```
[orders-consumer-a] REBALANCE -revoked=[0, 1, ... 11] -> owns 0 partitions []
[orders-consumer-b] REBALANCE -revoked=[0, 1, ... 5]  -> owns 0 partitions []
[orders-consumer-a] REBALANCE +added=[0, 1, 2, 3]     -> owns 4 partitions [0, 1, 2, 3]
```

Note the gap between the revoke lines and the add lines: **nobody owns anything in between**, and no `processed=` line appears from any pod during it. Confirm from the broker while it's happening:

```bash
kubectl -n lab exec kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group orders-workers --state
```

```
GROUP           COORDINATOR       ASSIGNMENT-STRATEGY  STATE                #MEMBERS
orders-workers  kafka-0:9092 (1)  range                PreparingRebalance   6
```

`PreparingRebalance` / `CompletingRebalance` instead of `Stable` *is* the stall.

### 🔍 Diagnose

The eager protocol requires every consumer to revoke all its partitions before reassignment, because Kafka must guarantee each partition has exactly one owner at all times. That is a **synchronisation barrier across the whole group** — the cost KIP-429 named the "stop-the-world effect." The cooperative protocol keeps partitions that aren't moving. And the trigger list is short and unavoidable: a rebalance happens whenever a member joins or leaves. **Every KEDA scale event is a rebalance.**

### 🟢 Fix it

```bash
kubectl -n lab set env deploy/orders-consumer ASSIGNOR=cooperative-sticky
kubectl -n lab rollout status deploy/orders-consumer
resume
```

Re-run the churn loop: incremental revokes, and `processed=` lines that never stop entirely.

> [!warning] Gotcha — one member on the old assignor keeps the whole group eager
> A group's effective protocol is the one **common to all members**. This is why Kafka's documented default `partition.assignment.strategy` is a list, `[RangeAssignor, CooperativeStickyAssignor]`, which "will use the RangeAssignor by default" — the list exists so you can upgrade with a single rolling bounce that *removes* RangeAssignor. Until every member has dropped it you get eager behaviour, with no error telling you so.
> **Mitigation:** set the assignor to cooperative **only** (not a list) and roll **all** pods; during a partial rollout you are still eager, so the improvement appears only when the last old pod goes. Then reduce the number of scale events — that's the other half of the fix ([[10 - Tuning Scaling Behaviour]]).

> [!tip] Lesson
> Autoscaling a consumer group is not free, and KEDA's documentation never mentions the cost. Every replica change buys throughput and charges a rebalance. **Make the scale events fewer, and each one cheaper.**

---

## 3. `offsetResetPolicy` mismatch

> **Symptom A:** lag reads absurdly high on a topic you know is nearly empty, and KEDA scales to max to process nothing.
> **Symptom B:** there is a real backlog and KEDA never scales up at all.

The single most common Kafka-on-KEDA bug. Both halves are the same mismatch.

### 🔴 Break it — A: `earliest` scaler, `latest` consumer

Both changes must land while the group has no members, so KEDA sees a genuinely fresh group:

```bash
reset
kubectl -n lab set env deploy/orders-consumer \
  KAFKA_GROUP=mismatch-workers AUTO_OFFSET_RESET=latest   # consumer: skip the backlog
so minReplicaCount 0
tm consumerGroup mismatch-workers
tm offsetResetPolicy earliest                              # scaler: count the whole log
produce 6000
resume
```

### 👀 Observe

```bash
lag mismatch-workers
```

```
GROUP             TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
mismatch-workers  orders  0          -               512             -
mismatch-workers  orders  1          -               498             -
mismatch-workers  orders  2          -               503             -
```

**Every `CURRENT-OFFSET` is `-`.** That is the tell for this entire class of bug: the group has never committed, its lag is undefined, and `offsetResetPolicy` is inventing a number. With `earliest`, the invented number is the *whole log* — about 6,000.

W1 shows the workload jump from 0 straight to 12 replicas. Within seconds of the pods joining, lag collapses to 0 and they scale back down, because a `latest` consumer commits at the log end and reads nothing:

```bash
kubectl -n lab logs -l app=orders-consumer --prefix --tail=20 | grep processed
```

Nothing. Twelve pods, a full rebalance, **zero messages processed.**

### 🔴 Break it — B: `latest` scaler, `earliest` consumer

```bash
reset mismatch-workers
kubectl -n lab set env deploy/orders-consumer \
  KAFKA_GROUP=mismatch2-workers AUTO_OFFSET_RESET=earliest
tm consumerGroup mismatch2-workers
tm offsetResetPolicy latest
tm activationLagThreshold 20
produce 6000
resume
```

### 👀 Observe

Nothing happens. Read the metric KEDA is publishing:

```bash
kubectl -n lab get scaledobject orders-so -o jsonpath='{.status.externalMetricNames}{"\n"}'
kubectl get --raw "/apis/external.metrics.k8s.io/v1beta1/namespaces/lab/s0-kafka-orders?labelSelector=scaledobject.keda.sh/name=orders-so" | jq '.items[0].value'
```

```
"12"
```

Twelve — on a topic with 6,000 unread messages. With `offsetResetPolicy: latest`, an uncommitted partition contributes a **placeholder lag of 1**, so 12 partitions report 12, and `12 > 20` is false: the workload never even activates.

### 🔍 Diagnose

Always the same two steps:

1. `kafka-consumer-groups.sh --describe` — **are there `-` rows in `CURRENT-OFFSET`?** If yes, `offsetResetPolicy` is deciding your lag, not real data.
2. Compare the ScaledObject's `offsetResetPolicy` with the consumer's actual `auto.offset.reset`. They must be the same word.

### 🟢 Fix it

```bash
reset mismatch2-workers
kubectl -n lab set env deploy/orders-consumer \
  KAFKA_GROUP=orders-workers AUTO_OFFSET_RESET=earliest
tm consumerGroup orders-workers
tm offsetResetPolicy earliest
resume
```

> [!tip] Lesson
> `offsetResetPolicy` in the ScaledObject is **not a setting for your consumer** — it tells KEDA how to *guess* lag for partitions with no committed offset. If the guess and your consumer's real behaviour disagree, you get phantom lag or phantom calm. Once the group has committed everywhere the field stops mattering, which is why the mismatch only bites on new groups — exactly when you're least likely to be watching. Full matrix in §5 of [[08 - Scale-to-Zero & Activation]].

---

## 4. The non-committing consumer

> **Symptom:** lag climbs steadily, KEDA scales to `maxReplicaCount`, the pods look busy — and lag never falls.

### 🔴 Break it

```bash
reset
kubectl -n lab set env deploy/orders-consumer ENABLE_AUTO_COMMIT=false
resume
produce 6000
```

### 👀 Observe

W2 shows the pods working perfectly happily:

```
[orders-consumer-x] INFO processed=340 last=(p3 o1204) assignment=[0, 1, 2, 3]
[orders-consumer-x] WARNING commits DISABLED - not committing at revoke
```

W1 shows replicas climbing to 12 and staying there. And the broker's view, twice, thirty seconds apart:

```bash
lag; sleep 30; lag
```

```
GROUP           TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
orders-workers  orders  0          6012            6524            512
...
orders-workers  orders  0          6012            6524            512     <- identical
```

`CURRENT-OFFSET` is **frozen**, and LAG is frozen with it — or growing, if the producer is still running.

### 🔍 Diagnose

One question settles it: **is `CURRENT-OFFSET` advancing between two `--describe` calls?**

- Advancing, lag falling → healthy.
- Advancing too slowly → genuinely under-provisioned, or `lagThreshold` too high.
- **Not advancing while the pods report `processed=` climbing → the consumer isn't committing.** An application bug, not an autoscaling problem, and no KEDA field will fix it.

### 🟢 Fix it

```bash
kubectl -n lab set env deploy/orders-consumer ENABLE_AUTO_COMMIT=true
```

Within a couple of polling intervals lag collapses and the pod count follows it down.

> [!tip] Lesson
> **KEDA reads *committed* offsets from the broker** — not your consumer's in-memory position, not a client-side metric. A consumer that processes without committing is invisible to KEDA, so KEDA does the only thing it can: add more pods, which also don't commit. You hit the ceiling with a healthy-looking consumer and an idle Kafka cluster. Corollary for real systems: keep the commit interval **well below `pollingInterval`** so KEDA sees progress between polls.

---

## 5. In-flight message loss on scale-down

> **Symptom:** downstream sees duplicate side effects — the same order charged twice, the same email sent twice — correlated with scale-in events rather than with load.

### 🔴 Break it

Make processing slow, logging verbose, and the grace period shorter than one message takes:

```bash
kubectl -n lab set env deploy/orders-consumer PROCESS_DELAY_MS=5000 LOG_EVERY=1
kubectl -n lab patch deploy/orders-consumer --type=merge \
  -p '{"spec":{"template":{"spec":{"terminationGracePeriodSeconds":5}}}}'
kubectl -n lab rollout status deploy/orders-consumer

produce 400
sleep 60                                                   # let it scale out
kubectl -n lab scale deploy/orders-consumer --replicas=1    # force a scale-in
```

### 👀 Observe

```bash
kubectl -n lab logs -l app=orders-consumer --prefix --tail=40 --previous 2>/dev/null
kubectl -n lab get events --sort-by=.lastTimestamp | tail -15
```

The doomed pod's log ends mid-flight:

```
[orders-consumer-p] INFO processed=7 last=(p7 o0134) assignment=[6, 7]
[orders-consumer-p] WARNING signal SIGTERM received -> draining and leaving group
```

and then **stops**. The two lines a clean exit would print — `committing final offsets` and `closed cleanly` — never appear, because SIGKILL arrived 5 seconds later while the consumer was still inside its 5-second `sleep`. Now watch the survivor take over partition 7:

```
[orders-consumer-q] REBALANCE +added=[6, 7] -> owns 12 partitions [0, ... 11]
[orders-consumer-q] INFO processed=41 last=(p7 o0131) assignment=[0, ... 11]
```

**Offset 131 on partition 7 again** — messages the dead pod had already processed. Duplicate delivery caused purely by autoscaling. There's a second cost in the timing: with no `close()` there was no LeaveGroup, so the group waited out `session.timeout.ms` (10 s here) before rebalancing. A clean exit rebalances in about a second.

### 🔍 Diagnose

```bash
kubectl -n lab logs -l app=orders-consumer --prefix --tail=200 | grep -c "closed cleanly"
```

If terminating pods aren't logging a clean shutdown, they're being killed mid-batch. Compare `terminationGracePeriodSeconds` against your **worst-case** time to finish one batch — not your average.

### 🟢 Fix it

```bash
kubectl -n lab patch deploy/orders-consumer --type=merge \
  -p '{"spec":{"template":{"spec":{"terminationGracePeriodSeconds":25}}}}'
kubectl -n lab rollout status deploy/orders-consumer
```

Repeat the scale-in. The log now completes, and the survivor resumes at the next unprocessed offset instead of re-covering old ground:

```
[orders-consumer-p] WARNING signal SIGTERM received -> draining and leaving group
[orders-consumer-p] INFO committing final offsets (processed=9)
[orders-consumer-p] INFO closed cleanly
```

> [!warning] Gotcha — the grace period is only half of it
> Raising `terminationGracePeriodSeconds` does nothing if the application ignores SIGTERM. The lab consumer does the right thing: stops polling, commits what it finished, and calls `close()` so Kafka gets a LeaveGroup and rebalances promptly. Most consumers written without autoscaling in mind do none of that and simply die where they stand.
> **Mitigation:** four things together — grace period above worst-case batch time; a SIGTERM handler that finishes, commits and closes; small batches so "worst case" stays bounded; and **idempotent processing**, because at-least-once delivery means you will reprocess sooner or later regardless. For work measured in minutes the right answer is a different shape entirely — see [[13 - ScaledJob — When a Pod per Message Beats a Consumer Pool]].

> [!tip] Lesson
> Autoscaling doesn't *create* at-least-once delivery — it **makes it visible**, constantly, in production, at a rate proportional to how well your autoscaling works. A consumer that only restarts during deploys can get away with sloppy shutdown. One that scales in ten times an hour cannot.

---

## 6. Never reaching zero

> **Symptom:** `minReplicaCount: 0`, the topic is completely drained, and one pod runs forever.

Two distinct causes needing different responses. Learn to tell them apart from the `Active` condition alone.

### 🔴 Break it — A: an idle lag floor

In production the floor comes from transaction markers or compaction, which occupy offsets and leave lag permanently non-zero. You can reproduce **the same reported floor** by pointing the trigger at a group that has never committed — different mechanism, identical number and identical fix.

```bash
reset
kubectl -n lab set env deploy/orders-consumer KAFKA_GROUP=orders-workers
so minReplicaCount 0
so cooldownPeriod 30
tm consumerGroup ghost-workers          # a group nothing ever consumes
tm offsetResetPolicy latest
tm activationLagThreshold 0
resume
```

Produce nothing. Wait two minutes.

### 👀 Observe

```bash
kubectl -n lab get deploy orders-consumer     # 1/1, forever
kubectl get --raw "/apis/external.metrics.k8s.io/v1beta1/namespaces/lab/s0-kafka-orders?labelSelector=scaledobject.keda.sh/name=orders-so" | jq '.items[0].value'
```

```
"12"
```

An idle topic reporting lag 12 — one per partition. And:

```bash
kubectl -n lab get scaledobject orders-so \
  -o jsonpath='{range .status.conditions[*]}{.type}={.status}  {.reason}{"\n"}{end}'
```

```
Ready=True    ScaledObjectReady
Active=True   ScalerActive
Fallback=False
Paused=False
```

**`Active=True` on a drained topic** is the whole diagnosis. `12 > 0` is true, so the trigger is active, so KEDA will never deactivate. One pod, zero messages, indefinitely.

### 🟢 Fix it

Measure the floor, then set the threshold above it — twelve partitions, floor of 12, so give it headroom:

```bash
tm activationLagThreshold 20
```

Within `cooldownPeriod` the condition flips to `Active=False / ScalerNotActive` and the pod goes:

```bash
kubectl -n lab describe scaledobject orders-so | tail -8
# Normal  KEDAScaleTargetDeactivated  Deactivated apps/v1.Deployment lab/orders-consumer from 1 to 0
```

### 🔴 Break it — B: the cooldown simply hasn't elapsed

```bash
tm consumerGroup orders-workers
so cooldownPeriod 300
produce 500        # then let it drain completely and watch
```

### 👀 Observe

Lag is 0, the trigger is inactive, and the pod is still there:

```
Active=False   ScalerCooldown
```

with the message *"Scaler cooling down because triggers are not active."* Five minutes later — `cooldownPeriod: 300` — it goes to zero.

### 🔍 Diagnose

| `Active` | Reason | What it means |
|---|---|---|
| `True` | `ScalerActive` | **the trigger still reports work** — your threshold is below the topic's lag floor (cause A) |
| `False` | `ScalerCooldown` | trigger idle, KEDA is **waiting out `cooldownPeriod`** (cause B) — nothing is wrong |
| `False` | `ScalerNotActive` | idle and past cooldown; it should already be at zero. Check for a `paused` annotation |
| `Unknown` / `Ready=False` | `TriggerError`, `HPAMetricsUnavailable` | the scaler can't read Kafka. Fix that first — [[14 - Observability & Debugging — Why Isn't It Scaling]] |

That one column tells you whether to change a threshold, wait, or go debug connectivity.

> [!tip] Lesson
> "It won't scale to zero" has two completely different answers and the `Active` condition distinguishes them in one command. **`Active=True` on an idle topic means lag never actually reaches zero** — measure the floor and set `activationLagThreshold` above it, never `0` on a transactional or compacted topic. **`Active=False` with `ScalerCooldown` means it's working correctly and you're impatient.**

---

## 7. Checkpoint

Put the stack back to a sane state:

```bash
kubectl -n lab set env deploy/orders-consumer \
  KAFKA_GROUP=orders-workers AUTO_OFFSET_RESET=earliest ASSIGNOR=cooperative-sticky \
  ENABLE_AUTO_COMMIT=true PROCESS_DELAY_MS=250 LOG_EVERY=20
kubectl -n lab apply -f lab/consumer-deploy.yaml
```

**You should now reach for the right diagnostic reflexively, before touching any config:**

- [ ] `kafka-consumer-groups.sh --describe`, run **twice** — is `CURRENT-OFFSET` advancing, and are there `-` rows?
- [ ] `--describe --state` — is the group `Stable`, or stuck in `PreparingRebalance`?
- [ ] The ScaledObject's **`Active` condition and its reason** — active, cooling down, or erroring?
- [ ] The **raw external-metrics API** — what number is KEDA actually publishing, as opposed to what Kafka reports?
- [ ] Terminating pods' logs — did they shut down cleanly, or vanish mid-batch?
- [ ] And the reflex you should *not* have: reaching for `cooldownPeriod` to fix anything above one replica.

## 8. Symptom → cause → fix

The most useful thing in this module. Learn the middle column.

| Symptom | Likely cause | Fix |
|---|---|---|
| Replica count oscillates every few seconds | `lagThreshold` too small — one message ≈ one pod; scale-up has no default damping | `lagThreshold` = messages one pod clears per `pollingInterval`; add `scaleUp.stabilizationWindowSeconds` |
| Adding pods reduces throughput; consumption pauses on every change | Eager rebalancing (`RangeAssignor`) — a barrier across the whole group | `partition.assignment.strategy` = cooperative-sticky **only**, on **every** member; then reduce scale events |
| Assignor change had no effect | Some members still list the range assignor; the group's protocol is the common one | Roll **all** pods; verify with `--describe --state` |
| Lag absurdly high on a near-empty topic; scales to max, processes nothing | Fresh group + `offsetResetPolicy: earliest` while the consumer uses `latest` — whole log counted as lag | Match `offsetResetPolicy` to the consumer's `auto.offset.reset` |
| Real backlog, KEDA never scales up | Fresh group + `offsetResetPolicy: latest` — uncommitted partitions report a placeholder lag of 1 each | Match them; check `activationLagThreshold` isn't above the placeholder total |
| `-` in the `CURRENT-OFFSET` column | Group has never committed for that partition; lag is being invented by `offsetResetPolicy` | Seed offsets by running once at `minReplicaCount: 1`, and match the policies |
| At `maxReplicaCount`, pods busy, lag flat or climbing, Kafka idle | Consumer isn't committing — KEDA reads **committed** offsets only | Fix the commit path; commit interval well below `pollingInterval` |
| Duplicate processing correlated with scale-in | Pod SIGKILLed mid-batch before committing | `terminationGracePeriodSeconds` > worst-case batch; SIGTERM handler that commits and `close()`s; idempotent processing |
| Rebalance takes ~`session.timeout.ms` after every scale-in | Pods dying without a clean `close()`, so no LeaveGroup | Same fix — graceful shutdown rebalances in ~1 s |
| Won't scale to zero; `Active=True` on a drained topic | Permanent lag floor (transaction markers, compaction, uncommitted partitions) with `activationLagThreshold: 0` | Measure the floor via the raw metrics API; set `activationLagThreshold` above it (≈ partition count) |
| Won't scale to zero; `Active=False`, reason `ScalerCooldown` | Nothing is wrong — `cooldownPeriod` (default 300 s) hasn't elapsed | Wait, or lower `cooldownPeriod` |
| Sits at N pods long after the backlog cleared | HPA `scaleDown.stabilizationWindowSeconds` (default 300 s) — **not** `cooldownPeriod` | Tune the HPA behaviour ([[10 - Tuning Scaling Behaviour]]) |
| Deleted the ScaledObject while at zero; workload stayed at zero | Replicas aren't restored on delete by default | `advanced.restoreToOriginalReplicaCount: true`, or pause instead of deleting |
| `TARGETS` shows `<unknown>` in `kubectl get hpa` | The metrics APIService or the trigger is broken — **not** "lag is zero" | Check `kubectl get apiservice v1beta1.external.metrics.k8s.io` and the operator logs |
| Pods scale past the partition count | `allowIdleConsumers: true` | Leave it `false`; pin `maxReplicaCount` to the partition count |

---

## 9. Questions you should be able to answer

- You see `-` in the `CURRENT-OFFSET` column. What does that tell you about the lag KEDA reports? *(It's invented by `offsetResetPolicy`, not measured — either the whole log or a placeholder 1 per partition.)*
- Pods at max, busy, lag not falling. What's the one command that settles it? *(`kafka-consumer-groups.sh --describe` twice — is `CURRENT-OFFSET` advancing?)*
- You switched one pod to the cooperative assignor and nothing improved. Why? *(The group's protocol is the one common to all members; it stays eager until every member drops the range assignor.)*
- Duplicates appear only during scale-in. Where do you look first? *(`terminationGracePeriodSeconds` versus worst-case batch time, and whether terminating pods log a clean shutdown.)*
- `Active=True` on a completely drained topic. What now? *(Lag has a floor. Measure it from the raw external-metrics API and set `activationLagThreshold` above it.)*
- `Active=False`, reason `ScalerCooldown`, one pod still running. What's broken? *(Nothing. `cooldownPeriod` hasn't elapsed.)*
- Why does `lagThreshold: 1` make things worse rather than more responsive? *(One message becomes one pod, scale-up has no default stabilisation window, and every scale event costs a rebalance.)*

---

## 10. Sources

- [KEDA — Apache Kafka scaler (`lagThreshold`, `activationLagThreshold`, `offsetResetPolicy`)](https://keda.sh/docs/2.20/scalers/apache-kafka/)
- [KEDA — ScaledObject specification (`cooldownPeriod`, `pollingInterval`, `advanced`)](https://keda.sh/docs/2.20/reference/scaledobject-spec/)
- [KEDA — Scaling Deployments: pause annotations, activation vs scaling](https://keda.sh/docs/2.20/concepts/scaling-deployments/)
- [KEDA — Events emitted by KEDA (`KEDAScaleTargetDeactivated`)](https://keda.sh/docs/2.20/reference/events/)
- [KEDA — Troubleshooting](https://keda.sh/docs/2.20/concepts/troubleshooting/)
- [Apache Kafka — basic operations, including `kafka-consumer-groups.sh`](https://kafka.apache.org/43/operations/basic-kafka-operations/)
- [Apache Kafka — consumer configuration (`partition.assignment.strategy`, `auto.offset.reset`, `session.timeout.ms`)](https://kafka.apache.org/43/configuration/consumer-configs/)
- [Apache Kafka — the next generation consumer rebalance protocol (KIP-848)](https://kafka.apache.org/43/operations/consumer-rebalance-protocol/)
- [KIP-429 — incremental cooperative rebalancing and the "stop-the-world effect"](https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol)
- [librdkafka configuration reference (what `confluent-kafka` actually accepts)](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
- [Kubernetes — Pod lifecycle and termination grace periods](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/)
- [Kubernetes — HorizontalPodAutoscaler walkthrough (stabilisation windows and policies)](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/)
