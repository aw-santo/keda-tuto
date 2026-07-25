---
title: 06 - LAB 2 — Your First Kafka ScaledObject
tags:
  - keda
  - kafka
  - kubernetes
  - lab
  - autoscaling
aliases:
  - LAB 2
  - First ScaledObject
  - Kafka ScaledObject Lab
status: ready
created: 2026-07-25
---

# LAB 2 — Your First Kafka ScaledObject

> [!abstract] Scope
> Put a real consumer on the 12-partition `orders` topic, wrap it in a **ScaledObject**, burst the topic, and watch the whole chain move: **lag → KEDA's metric → the HPA's arithmetic → pods → partition reassignment.** The payoff is the rebalance log — scaling 1 → 2 → 3 consumers on 12 partitions gives you **12 → 6/6 → 4/4/4**, printed by the consumers themselves. Then you scale to zero, and finally you reproduce the **single most common Kafka-on-KEDA bug** (`offsetResetPolicy` mismatched against the consumer) so you recognise it the next time lag reads absurdly huge or refuses to trigger at all. Assumes [[05 - LAB 1 — Local Cluster, Kafka & KEDA from Scratch]] is running. The arithmetic behind all of it is in [[07 - The Kafka Scaler — Every Field & the Replica Math]].

Related: [[00 - KEDA Index]], [[05 - LAB 1 — Local Cluster, Kafka & KEDA from Scratch]], [[07 - The Kafka Scaler — Every Field & the Replica Math]], [[02 - Kafka Consumer Groups, Partitions & Rebalancing]], [[08 - Scale-to-Zero & Activation]], [[09 - LAB 3 — Break It on Purpose]], [[10 - Tuning Scaling Behaviour]], [[14 - Observability & Debugging — Why Isn't It Scaling]]

> [!important] Same honesty note as Lab 1
> The consumer, the producer, cooperative rebalancing and SIGTERM handling here were **run for real**, and `lab/consumer-deploy.yaml` is **schema-validated against the real KEDA 2.20.1 CRDs** — but the **KEDA → Kafka scaling loop has not yet been executed end-to-end on a single cluster**. Expect it to work as written; if it doesn't, [§13 Troubleshooting](#13-troubleshooting) covers every failure mode the research turned up, and §11 covers the most likely one.

---

## 1. Goal

Working from `lab/`, you will:

- build `keda-lab-client:0.4` from `lab/Dockerfile` and load it into kind
- deploy the consumer and a `ScaledObject` from `lab/consumer-deploy.yaml`
- burst the topic and watch replicas track lag
- read the external-metrics API directly, and read the HPA's `TARGETS` column correctly
- watch 12 partitions redistribute as pods come and go
- scale to zero and wake it up again
- break `offsetResetPolicy` on purpose, diagnose it from `-` rows, and fix it

---

## 2. Prerequisites

Lab 1's checkpoint passing. Re-verify in one line:

```bash
kubectl get apiservice v1beta1.external.metrics.k8s.io && kubectl -n lab get pods
```

Expected — `AVAILABLE=True`, and `kafka-0` at `1/1 Running`. If the broker restarted since Lab 1, `emptyDir` wiped it; re-create the topic:

```bash
kubectl -n lab exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic orders
```

Expected `PartitionCount: 12`. If it says the topic doesn't exist, re-run the create command from [[05 - LAB 1 — Local Cluster, Kafka & KEDA from Scratch]] §7.

---

## 3. Build the client image and load it into kind

One image contains both the consumer and the producer.

```bash
cd lab
docker build -t keda-lab-client:0.4 .
```

Expected — `Successfully installed confluent-kafka-2.15.0`, then `Successfully tagged keda-lab-client:0.4`, in well under a minute. The pip step pulls a **prebuilt aarch64 wheel** (`confluent_kafka-2.15.0-cp313-cp313-manylinux_2_28_aarch64.whl`, librdkafka 2.15.0 bundled), which is why this lab uses `confluent-kafka` rather than a pure-Python client: on an arm64 Mac it installs in seconds with no compiler.

```bash
kind load docker-image keda-lab-client:0.4 --name keda-lab
```

Expected — one `not yet present on node ..., loading...` line **per node**, all three, because KEDA will schedule pods anywhere. Confirm with `docker exec keda-lab-worker crictl images | grep keda-lab-client`.

> [!warning] Gotcha — two image traps that produce `ErrImagePull` on an image that is demonstrably present
> `kind load docker-image` copies the image into each node's containerd store. There is no registry involved, so **any attempt to pull will fail.** Two things cause a pull:
> 1. **`imagePullPolicy` not set to `IfNotPresent`.** Kubernetes' default policy is `IfNotPresent` *except* when the tag is `:latest` or absent, in which case it becomes `Always`.
> 2. **Tagging the image `:latest`.** This is the subtle one — it silently flips the default policy to `Always`, so even a manifest that never mentions `imagePullPolicy` starts pulling.
>
> The symptom is maddening: `crictl images` on the node lists your image, and the pod still reports `ErrImagePull: failed to resolve reference ... not found`.
> **Mitigation:** do both. `lab/consumer-deploy.yaml` sets `imagePullPolicy: IfNotPresent` explicitly, and the tag is `0.4` — a real version, never `latest`. When you change the code, **bump the tag** (`0.5`) and update the Deployment; re-loading the same tag doesn't reliably replace an image the node has already cached.

---

## 4. Deploy the consumer

`lab/consumer-deploy.yaml` holds both the Deployment and the ScaledObject. Apply the whole thing in a moment; first, understand the two config decisions that determine whether this lab works at all.

### 4a. The commit settings — this is the one that silently ruins the demo

From `lab/consumer.py`:

```python
"enable.auto.commit": True,        # commit on a timer...
"auto.commit.interval.ms": 1000,   # ...every second, so lag visibly falls
"enable.auto.offset.store": False, # ...but only for messages we FINISHED
"auto.offset.reset": "earliest",   # MUST match the ScaledObject's offsetResetPolicy
```

and then, *after* the artificial processing delay:

```python
time.sleep(DELAY_MS / 1000.0)
processed += 1
consumer.store_offsets(msg)
```

Read that as two independent decisions:

- **Commit on a short timer** (1 s). KEDA reads **committed offsets from the broker**, not your consumer's in-memory position. If offsets only reach the broker occasionally, KEDA sees a consumer that isn't making progress.
- **Only store offsets for work you've actually finished.** `enable.auto.offset.store: False` stops the client from marking a message done the instant `poll()` returns it; `store_offsets(msg)` after processing means a commit can never claim progress you haven't made. That's what makes the timer safe.

> [!warning] Gotcha — a consumer that doesn't commit while running makes KEDA look broken
> The first draft of this lab used `enable.auto.commit: False`, committing only on revoke and close. Offsets therefore reached the broker only when a pod shut down or the group rebalanced — so while consumers were happily processing, **KEDA's reported lag never fell.** KEDA scaled up, the new pods also didn't commit, lag kept climbing, and the deployment marched to `maxReplicaCount` with an idle Kafka cluster and a perfectly healthy consumer. Every symptom pointed at KEDA. The bug was in the client.
> **Mitigation:** keep your commit interval **well below `pollingInterval`** (here: 1 s commits against a 5 s poll) so KEDA sees progress between every poll. And when lag won't fall, check `CURRENT-OFFSET` with `kafka-consumer-groups.sh --describe` *before* you touch any KEDA field — if it isn't advancing, it's a consumer bug.

> [!warning] Gotcha — `auto.offset.reset` and `offsetResetPolicy` must agree
> The consumer's `auto.offset.reset` decides where *it* starts on a partition with no committed offset. The ScaledObject's `offsetResetPolicy` decides how **KEDA counts lag** on that same partition. They're two independent settings describing one assumption, and nothing validates that they match. Disagreement produces either phantom enormous lag or a workload that never wakes up — you'll reproduce both in §11.
> **Mitigation:** set them together, in the same review. Here both say `earliest`.

### 4b. The graceful-shutdown setting

```yaml
terminationGracePeriodSeconds: 20    # must exceed PROCESS_DELAY_MS (250 ms)
```

Scale-down sends SIGTERM. `lab/consumer.py` handles it: stop polling, commit, `close()`. `close()` sends a **LeaveGroup**, which triggers an immediate rebalance instead of making the group wait out `session.timeout.ms` (10 s). Verified behaviour: the consumer reported `closed cleanly` in **0.3 s**, and the surviving consumers picked up the freed partitions about **1 s later**. Without that handler you'd get a SIGKILL, no LeaveGroup, an uncommitted batch redelivered, and a group stalled for the full session timeout.

### 4c. Apply it

```bash
kubectl apply -f lab/consumer-deploy.yaml
kubectl -n lab rollout status deploy/orders-consumer
kubectl -n lab logs deploy/orders-consumer
```

Expected — the consumer joins and is handed **all twelve partitions**, because it's the only member:

```
... INFO started bootstrap=kafka-0.kafka.lab.svc.cluster.local:9092 topic=orders group=orders-workers delay=250ms
... INFO REBALANCE +added=[0,1,2,3,4,5,6,7,8,9,10,11] -> owns 12 partitions [0,1,2,3,4,5,6,7,8,9,10,11]
```

If you see `REBALANCE +added=[]` or nothing at all, jump to §13.

---

## 5. The ScaledObject, and why the lab values differ from production defaults

The second half of `lab/consumer-deploy.yaml`:

```yaml
spec:
  scaleTargetRef:
    name: orders-consumer
  pollingInterval: 5          # default 30
  cooldownPeriod: 30          # default 300
  minReplicaCount: 1
  maxReplicaCount: 12         # == partition count
  advanced:
    horizontalPodAutoscalerConfig:
      behavior:
        scaleUp:
          stabilizationWindowSeconds: 0
          policies: [{type: Percent, value: 100, periodSeconds: 10}]
        scaleDown:
          stabilizationWindowSeconds: 15   # default 300
          policies: [{type: Percent, value: 100, periodSeconds: 15}]
  triggers:
    - type: kafka             # the stable scaler — NOT "apache-kafka", which is Experimental
      metadata:
        bootstrapServers: kafka-0.kafka.lab.svc.cluster.local:9092
        consumerGroup: orders-workers
        topic: orders
        lagThreshold: "100"
        activationLagThreshold: "0"
        offsetResetPolicy: earliest
```

Three of these are **lab values, chosen so you can see things happen** — and they're the wrong defaults for production:

| Field | Lab | Default | Why the default makes the demo look broken |
|---|---|---|---|
| `pollingInterval` | **5** | 30 | Half a minute between broker queries. You'd stare at an unchanged `kubectl get hpa` and conclude nothing works |
| `cooldownPeriod` | **30** | 300 | Governs *only* the final 1 → 0 step. At the default you'd wait five minutes for scale-to-zero |
| `scaleDown.stabilizationWindowSeconds` | **15** | 300 | This is the usual answer to "why won't it scale in". The HPA takes the highest recommendation over the whole window, so after a burst it holds peak replicas for five minutes |

`cooldownPeriod` and `scaleDown.stabilizationWindowSeconds` are frequently confused. They govern **different transitions**: the HPA's stabilization window governs N → 1, and KEDA's `cooldownPeriod` governs the final 1 → 0 that only KEDA can perform. Overriding one and not the other leaves half the path slow. [[10 - Tuning Scaling Behaviour]] takes this apart properly.

Two values are *not* lab-specific and should be copied to production: `maxReplicaCount: 12` pinned to the partition count, and `type: kafka` rather than the Experimental `apache-kafka`.

Check that KEDA accepted it:

```bash
kubectl -n lab get scaledobject orders-so
```

Expected — `READY   True` and, with an empty topic, `ACTIVE   False`:

```
NAME        SCALETARGETKIND      SCALETARGETNAME   MIN   MAX   TRIGGERS   READY   ACTIVE   FALLBACK   AGE
orders-so   apps/v1.Deployment   orders-consumer   1     12    kafka      True    False    False      20s
```

And KEDA created an HPA it owns:

```bash
kubectl -n lab get hpa
```

```
NAME                 REFERENCE                    TARGETS       MINPODS   MAXPODS   REPLICAS   AGE
keda-hpa-orders-so   Deployment/orders-consumer   0/100 (avg)   1         12        1          22s
```

You don't create that HPA and you must never edit it — KEDA owns it and reconciles your changes away.

---

## 6. The observation toolkit

Open these in separate terminals before producing anything. Watching the transition is the entire point; reading the end state teaches much less.

**Terminal 1 — the whole picture, every 2 seconds:**

```bash
watch -n2 'kubectl -n lab get scaledobject,hpa,deploy,pods'
```

**Terminal 2 — pod churn as it happens:**

```bash
kubectl -n lab get pods -w
```

**Terminal 3 — the consumers' own rebalance logs (the payoff, §9):**

```bash
kubectl -n lab logs -l app=orders-consumer --all-containers -f --prefix --tail=5
```

**Terminal 4 — ground truth from the broker, which is exactly what KEDA reads:**

```bash
watch -n3 'kubectl -n lab exec kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group orders-workers'
```

**On demand — the external metric KEDA is actually publishing.** Don't guess the metric name; KEDA publishes it:

```bash
kubectl -n lab get scaledobject orders-so -o jsonpath='{.status.externalMetricNames}'
kubectl -n lab get scaledobject orders-so -o jsonpath='{.status.hpaName}'
```

Expected: `["s0-kafka-orders"]` and `keda-hpa-orders-so`. The pattern is `s<triggerIndex>-kafka-<topic>` — so a second trigger would be `s1-…`. Then query the aggregated API directly:

```bash
kubectl get --raw "/apis/external.metrics.k8s.io/v1beta1/namespaces/lab/s0-kafka-orders?labelSelector=scaledobject.keda.sh/name=orders-so" | jq
```

Expected — an `ExternalMetricValueList` with one item: `metricName: s0-kafka-orders`, a timestamp, and a `value` string ("0" on an empty topic).

This is the single most useful debugging command in the whole track: it is **literally what the HPA sees**. If this returns a number, the metrics path is fine and any problem is in the HPA's configuration. If it errors, the problem is KEDA or the trigger, and nothing downstream can work.

**On demand — KEDA's own decisions:**

```bash
kubectl -n keda logs deploy/keda-operator -f | grep -Ei "orders-so|scaledobject|scaling|metric"
kubectl -n lab describe scaledobject orders-so     # conditions + events at the bottom
```

---

## 7. Produce a burst and watch it scale

The producer is the same image with a different entrypoint argument — which is exactly why `lab/Dockerfile` ends with `ENTRYPOINT ["python"]` and `CMD ["/app/consumer.py"]` rather than baking the script into the entrypoint. (During verification, an entrypoint with the script baked in silently ran the *consumer* when asked for the producer.)

Start small, so you can watch the ladder rather than a jump:

```bash
kubectl -n lab run producer --rm -it --restart=Never \
  --image=keda-lab-client:0.4 --image-pull-policy=IfNotPresent \
  --env=KAFKA_BOOTSTRAP=kafka-0.kafka.lab.svc.cluster.local:9092 \
  --env=KAFKA_TOPIC=orders \
  --env=MESSAGE_COUNT=300 \
  -- /app/producer.py
```

Expected — `producing count=300 rate=unlimited/s …`, then `done delivered=300 failed=0 in 0.1s (…msg/s)`, then `pod "producer" deleted`. (Measured throughput during verification was 27.9k–121k msg/s, so 300 messages is instantaneous.)

**Do the arithmetic before you look.** 300 messages of backlog, `lagThreshold: 100`, so `ceil(300 / 100) = 3` replicas. One consumer with `PROCESS_DELAY_MS=250` clears about 4 messages/second, so the backlog persists long enough to watch.

Within one `pollingInterval` (5 s), Terminal 1 should move like this — `ACTIVE` flips `True`, then replicas climb:

```
scaledobject.keda.sh/orders-so                ...  READY True  ACTIVE True
horizontalpodautoscaler/keda-hpa-orders-so    300/100 (avg)   1   12   1
deployment.apps/orders-consumer               1/1   1   1
                      ... ~10s later ...
horizontalpodautoscaler/keda-hpa-orders-so    150/100 (avg)   1   12   2
deployment.apps/orders-consumer               2/2   2   2
```

then 3 replicas. The `scaleUp` policy is `Percent: 100 / periodSeconds: 10` — **at most a doubling every 10 seconds** — so from 1 replica the path is 1 → 2 → 3, not an instant jump. That policy is why the ladder is visible at all; with no policy the HPA would go straight to the computed target.

Now slam it, to see the ceiling:

```bash
kubectl -n lab run producer --rm -it --restart=Never \
  --image=keda-lab-client:0.4 --image-pull-policy=IfNotPresent \
  --env=KAFKA_BOOTSTRAP=kafka-0.kafka.lab.svc.cluster.local:9092 \
  --env=MESSAGE_COUNT=5000 \
  -- /app/producer.py
```

5000 messages implies `ceil(5000 / 100) = 50` replicas — far beyond 12 partitions. Watch what the metric does:

```bash
kubectl get --raw "/apis/external.metrics.k8s.io/v1beta1/namespaces/lab/s0-kafka-orders?labelSelector=scaledobject.keda.sh/name=orders-so" | jq -r '.items[0].value'
```

Expected: **`1200`**, not `5000`. And it will *stay* near 1200 however far behind you fall. That's not a stuck exporter. `allowIdleConsumers` defaults to `false`, and KEDA enforces the partition ceiling by **reporting `partitions × lagThreshold`** (12 × 100) — a number chosen so the HPA's own division lands exactly on 12. Meanwhile Terminal 4 shows Kafka's real total lag climbing past 4000. The two disagree **on purpose**; [[07 - The Kafka Scaler — Every Field & the Replica Math]] §4 explains why this means you must never use KEDA's metric as your lag alert.

Replicas climb 1 → 2 → 4 → 8 → 12 (doubling per 10 s, capped at `maxReplicaCount`), then drain and scale back in after the 15 s stabilization window.

---

## 8. Read the HPA output properly

```bash
kubectl -n lab get hpa keda-hpa-orders-so
```

```
NAME                 REFERENCE                    TARGETS         MINPODS   MAXPODS   REPLICAS   AGE
keda-hpa-orders-so   Deployment/orders-consumer   150/100 (avg)   1         12        2          4m
```

Two things people misread here.

**`TARGETS` is average lag *per replica*, not total lag.** KEDA publishes its metric with `metricType: AverageValue` (the default), so the HPA divides the metric by the current replica count before comparing it to the target. At 2 replicas and 300 total lag, `TARGETS` reads `150/100`. The consequence worth internalising: at 12 replicas with `lagThreshold: 100`, scale-up only continues above **1200 total lag** — the target is per-pod, so your total headroom grows with the pod count. If you expect `TARGETS` to show total lag, every reading will look wrong by a factor of the replica count.

**`<unknown>` does not mean lag is zero.** It means the HPA **could not read the metric at all**:

```
keda-hpa-orders-so   Deployment/orders-consumer   <unknown>/100 (avg)   1   12   1   4m
```

Zero lag reads `0/100`. `<unknown>` is a broken metrics path or a failing trigger — the APIService not Available, the metrics apiserver down, or the Kafka scaler erroring (bad bootstrap address, missing topic, unreachable broker). Diagnose in this order:

```bash
kubectl get apiservice v1beta1.external.metrics.k8s.io           # AVAILABLE=True?
kubectl get --raw ".../namespaces/lab/s0-kafka-orders?..." | jq  # does the metric resolve?
kubectl -n lab describe scaledobject orders-so                   # trigger errors in the events
kubectl -n keda logs deploy/keda-operator --tail=50
```

> [!warning] Gotcha — with several triggers, one broken metric freezes scale-*down* entirely
> When the HPA can't read one metric and the others recommend scaling down, it **skips scaling altogether** rather than acting on partial information. So a single failing scaler doesn't merely stop that trigger working — it pins the whole workload at its current replica count indefinitely.
> **Mitigation:** configure `spec.fallback` so a scaler failure resolves to a defined replica count instead of an unreadable metric ([[11 - Multiple Triggers, Scaling Modifiers & Fallback]]), and alert on KEDA's scaler error metrics rather than trusting `REPLICAS` to look sane.

---

## 9. The payoff — watch 12 partitions move

This is the part worth doing slowly. Reset to a clean single consumer, then step up.

```bash
kubectl -n lab logs -l app=orders-consumer --all-containers -f --prefix --tail=20
```

With one consumer, then two, then three, the logs read like this — **real captured output**:

```
[consumer-1] REBALANCE +added=[0,1,2,3,4,5,6,7,8,9,10,11] -> owns 12 partitions [0,1,2,3,4,5,6,7,8,9,10,11]
[consumer-1] REBALANCE -revoked=[0,1,2,3,4,5]             -> owns 6 partitions  [6,7,8,9,10,11]
[consumer-2] REBALANCE +added=[0,1,2,3,4,5]               -> owns 6 partitions  [0,1,2,3,4,5]
[consumer-1] REBALANCE -revoked=[6,7]                     -> owns 4 partitions  [8,9,10,11]
[consumer-2] REBALANCE -revoked=[0,1]                     -> owns 4 partitions  [2,3,4,5]
[consumer-3] REBALANCE +added=[0,1,6,7]                   -> owns 4 partitions  [0,1,6,7]
```

**12 → 6/6 → 4/4/4.** Every partition has exactly one owner at every moment, the total is always 12, and each step moves the minimum number of partitions needed. This is the concrete form of the constraint that makes Kafka autoscaling different from autoscaling a web service: parallelism is bounded by partitions, and adding a pod *takes work away* from an existing one.

**Why the consumer tracks a running total.** With `partition.assignment.strategy: cooperative-sticky`, the rebalance callbacks receive the **incremental delta** — only what changed — not the full assignment. A naive `on_assign` that just prints its argument therefore logs `ASSIGNED []` on a rebalance where nothing was added, and teaches nothing. `lab/consumer.py` keeps an `owned` set, adding in `on_assign` and removing in `on_revoke`, so every line ends with the pod's true current assignment:

```python
owned = set()

def on_assign(consumer, partitions):
    owned.update(p.partition for p in partitions)
    log.info("REBALANCE +added=%s -> owns %d partitions %s",
             fmt(partitions), len(owned), sorted(owned))
```

Notice too that `on_revoke` calls `safe_commit()` **before** giving up its partitions — committing what it finished so the next owner doesn't reprocess it. `safe_commit` swallows exactly one error code, `_NO_OFFSET` ("no offset stored"), because `close()` re-fires `on_revoke` after the final commit and there is legitimately nothing left to commit. Any other commit error is logged.

If kafbat-ui is running, open **http://127.0.0.1:30080** → Consumers → `orders-workers` while this happens. The member list and per-partition assignment there should match the logs exactly — two independent views of the same rebalance.

To force the steps by hand instead of via lag, scale the Deployment directly:

```bash
kubectl -n lab patch scaledobject orders-so -p '{"spec":{"paused":"true"}}' --type=merge  # optional: stop KEDA fighting you
kubectl -n lab scale deploy/orders-consumer --replicas=2
kubectl -n lab scale deploy/orders-consumer --replicas=3
kubectl -n lab patch scaledobject orders-so -p '{"spec":{"paused":null}}' --type=merge
```

> [!warning] Every scale event is a Kafka rebalance
> The logs make the cost visible: each replica change pauses consumption on the affected partitions while ownership moves. That's the hidden price of aggressive autoscaling, and KEDA's documentation never mentions it. A `lagThreshold` small enough to make the replica count twitch turns into a consumer group that spends its time rebalancing instead of consuming.
> **Mitigation:** size `lagThreshold` to roughly **the messages one pod clears in one `pollingInterval`**, so one unit of the metric means one pod's worth of work, and add a `scaleUp.stabilizationWindowSeconds` in production (the Kubernetes default is 0 — no damping on scale-up at all). [[10 - Tuning Scaling Behaviour]].

---

## 10. Scale to zero, and waking up again

`minReplicaCount: 1` in the lab file. Drop it to 0:

```bash
kubectl -n lab patch scaledobject orders-so --type=merge -p '{"spec":{"minReplicaCount":0}}'
```

Let the topic drain, then watch both objects:

```bash
kubectl -n lab get scaledobject orders-so -w      # ACTIVE: True -> False when lag <= activationLagThreshold
kubectl -n lab get deploy orders-consumer -w      # 1/1 -> 0/0, cooldownPeriod (30s) after that
```

Expected: `ACTIVE` flips `False` within a `pollingInterval` of the topic emptying, and the deployment reaches `0/0` about 30 s later.

Two distinct mechanisms just fired, and confusing them is the source of most scale-to-zero questions:

- **`ACTIVE=False`** is *activation*: lag fell to or below `activationLagThreshold` (`0` here), so KEDA considers there to be no work.
- **The 1 → 0 step itself is KEDA's**, not the HPA's, and it waits `cooldownPeriod` (30 s) after the last active reading. An HPA cannot set replicas to 0 — its `minReplicas` can't be zero outside an alpha feature gate — so KEDA writes the `scale` subresource directly.

Check who did it with `kubectl -n lab describe scaledobject orders-so | tail -20`: the events show KEDA's own scale decision, not an HPA event. And the HPA is still there at `minReplicas: 1` — it simply has nothing to do while the deployment is at 0.

Now wake it:

```bash
kubectl -n lab run producer --rm -it --restart=Never \
  --image=keda-lab-client:0.4 --image-pull-policy=IfNotPresent \
  --env=KAFKA_BOOTSTRAP=kafka-0.kafka.lab.svc.cluster.local:9092 \
  --env=MESSAGE_COUNT=1000 \
  -- /app/producer.py
```

Expected — within one `pollingInterval`, `ACTIVE` flips `True` and one pod appears; the HPA takes over from there and scales toward `ceil(lag/100)`. Note the asymmetry: **waking up is immediate** (activation has no cooldown), while going to sleep waits out `cooldownPeriod`. That's deliberate — being slow to release capacity is cheap, being slow to acquire it is not.

Restore the lab default when you're done: `kubectl -n lab patch scaledobject orders-so --type=merge -p '{"spec":{"minReplicaCount":1}}'`. [[08 - Scale-to-Zero & Activation]] covers the traps this lab is too clean to show — the invalid-offset matrix, the permanent lag floor on transactional topics, and the 7-day offset-retention trap that can strand a scaled-to-zero workload.

---

## 11. 🔴 Break it — mismatch `offsetResetPolicy` against the consumer

**This is the single most common Kafka-on-KEDA bug.** Do it once deliberately and you'll recognise it instantly in the wild.

Set up a **fresh consumer group** with a real backlog and nobody consuming, so no partition has a committed offset:

```bash
# clean slate: remove the working ScaledObject, stop consuming, rename the group
kubectl -n lab delete scaledobject orders-so
kubectl -n lab scale deploy/orders-consumer --replicas=0
kubectl -n lab set env deploy/orders-consumer KAFKA_GROUP=orders-workers-v2

# 2000 messages of backlog that nobody has touched
kubectl -n lab run producer --rm -it --restart=Never \
  --image=keda-lab-client:0.4 --image-pull-policy=IfNotPresent \
  --env=KAFKA_BOOTSTRAP=kafka-0.kafka.lab.svc.cluster.local:9092 \
  --env=MESSAGE_COUNT=2000 \
  -- /app/producer.py
```

Now apply a ScaledObject whose `offsetResetPolicy` says **`latest`**, while the consumer's `auto.offset.reset` still says `earliest`:

```bash
cat <<'EOF' | kubectl apply -f -
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: orders-so
  namespace: lab
spec:
  scaleTargetRef:
    name: orders-consumer
  pollingInterval: 5
  cooldownPeriod: 30
  minReplicaCount: 0
  maxReplicaCount: 12
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: kafka-0.kafka.lab.svc.cluster.local:9092
        consumerGroup: orders-workers-v2
        topic: orders
        lagThreshold: "100"
        offsetResetPolicy: latest        # <-- WRONG: consumer uses earliest
EOF
```

**What you should see: nothing happens.** 2000 messages sitting in the topic, and the workload stays at zero replicas. `kubectl -n lab get scaledobject orders-so` reports `READY True` / `ACTIVE False` — the trigger is *working*, and KEDA believes there is no work. The metric agrees:

```bash
kubectl get --raw "/apis/external.metrics.k8s.io/v1beta1/namespaces/lab/s0-kafka-orders?labelSelector=scaledobject.keda.sh/name=orders-so" | jq -r '.items[0].value'
```

Expected: **`0`**. Everything reports healthy, and the workload is pinned at zero on top of a full topic — the worst kind of failure, because there is nothing to alert on.

### The diagnostic: the `-` rows

```bash
kubectl -n lab exec kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group orders-workers-v2
```

Expected — every partition has **no committed offset**, so `CURRENT-OFFSET` and `LAG` are literally `-`:

```
GROUP                TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
orders-workers-v2    orders  0          -               167             -
orders-workers-v2    orders  1          -               172             -
orders-workers-v2    orders  2          -               158             -
...
```

**Those `-` rows are the whole story.** Lag on a partition with no committed offset is *undefined* — Kafka can't tell you, and `offsetResetPolicy` is how KEDA decides what to assume:

| `offsetResetPolicy` | KEDA counts an uncommitted partition as | Symptom |
|---|---|---|
| `earliest` | the **whole** log-end offset — every message ever written | "my lag is absurdly huge", instant jump to `maxReplicaCount` |
| `latest` | **0** | "KEDA never scales up", workload pinned at zero on a full topic |

You just produced the second row. The first is the mirror image: leave `offsetResetPolicy: earliest` while the consumer uses `auto.offset.reset: latest`, and KEDA counts the entire retained log as backlog for partitions the consumer intends to skip — so it slams to 12 replicas, the consumers jump to the end and commit, and lag collapses to zero in one poll. Flapping, with no explanation in any log.

Once a real commit exists on a partition, `offsetResetPolicy` stops mattering for it — which is why this bug bites hardest on **new groups, renamed groups, new partitions added to an existing topic**, and groups whose offsets expired.

### 🟢 Fix it

Match the policy to the consumer, and restore the tested manifest:

```bash
kubectl -n lab delete scaledobject orders-so
kubectl -n lab set env deploy/orders-consumer KAFKA_GROUP=orders-workers
kubectl apply -f lab/consumer-deploy.yaml
```

Expected — within a `pollingInterval`, `ACTIVE=True`, the metric reports **1200** (the partition ceiling, because 2000+ messages of real lag exceeds 12 × 100), and `kubectl -n lab get scaledobject,hpa,deploy` shows replicas climbing with `TARGETS` at `1200/100 (avg)`.

And the group now has real committed offsets — no more `-` rows:

```
GROUP           TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
orders-workers  orders  0          167             167             0     <- caught up
orders-workers  orders  3          88              172             84    <- working
orders-workers  orders  5          41              158             117   <- behind
```

**The rule:** `offsetResetPolicy` and the consumer's `auto.offset.reset` are one decision expressed in two files. Change them together, and when lag looks impossible in either direction, run `--describe` and look for `-` before you touch anything else.

---

## 12. Checkpoint

You're done when all of these hold:

```bash
# 1. the ScaledObject is Ready, and Active while there's lag
kubectl -n lab get scaledobject orders-so

# 2. the HPA KEDA owns exists, with a numeric TARGETS (never <unknown>)
kubectl -n lab get hpa keda-hpa-orders-so

# 3. the external metric resolves — literally what the HPA reads
kubectl -n lab get scaledobject orders-so -o jsonpath='{.status.externalMetricNames}'
kubectl get --raw "/apis/external.metrics.k8s.io/v1beta1/namespaces/lab/s0-kafka-orders?labelSelector=scaledobject.keda.sh/name=orders-so" | jq -r '.items[0].value'

# 4. committed offsets are advancing — no `-` rows once consumers are running
kubectl -n lab exec kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group orders-workers

# 5. the group has as many members as you have pods
kubectl -n lab exec kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group orders-workers --state
```

And conceptually, you should have **seen** each of these at least once:

- replicas rise after a burst and fall after it drains
- the KEDA metric flatten at `1200` while Kafka's real lag kept climbing — the partition ceiling
- `TARGETS` showing average lag per replica, not total
- the `12 → 6/6 → 4/4/4` rebalance in the consumer logs
- the deployment reach 0 and wake from 0
- `-` rows in `--describe`, and what `offsetResetPolicy` does with them

---

## 13. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Pod `ErrImagePull` on an image `crictl images` clearly lists | `imagePullPolicy: Always` — either set explicitly or defaulted because the tag is `:latest`/absent | `imagePullPolicy: IfNotPresent` **and** a non-`latest` tag. Re-run `kind load docker-image` after any rebuild, with a bumped tag |
| Code change has no effect | The node cached that tag already | Bump the tag (`0.5`), reload, update the Deployment |
| `TARGETS: <unknown>` | Metrics path or trigger broken — **not** zero lag | `kubectl get apiservice v1beta1.external.metrics.k8s.io`, then the `--raw` query, then `describe scaledobject` |
| `ACTIVE=False` with a full topic | `offsetResetPolicy: latest` on a group with no committed offsets (§11), or `activationLagThreshold` too high | Check for `-` rows in `--describe`; match `offsetResetPolicy` to the consumer |
| Lag reads absurdly huge; instant jump to `maxReplicaCount` | `offsetResetPolicy: earliest` against a consumer using `latest` | Same fix, opposite direction (§11) |
| Replicas climb, lag never falls | Consumer isn't committing while running | `--describe` and watch `CURRENT-OFFSET`. If it's static, it's a client bug — check `auto.commit.interval.ms` and that `store_offsets` is called |
| KEDA's metric flat at 1200 while Kafka says 4000+ | Working as designed — the partition ceiling reports `partitions × lagThreshold` | Nothing to fix. Never alert on KEDA's metric; alert on Kafka's ([[07 - The Kafka Scaler — Every Field & the Replica Math]]) |
| Won't scale in for ~5 minutes | HPA `scaleDown.stabilizationWindowSeconds` default is 300 | Set it (15 in the lab file). Separately, `cooldownPeriod` (default 300) governs only 1 → 0 |
| Scales in fast but never reaches 0 | `minReplicaCount: 1` | Patch to 0 (§10) |
| Consumer logs `REBALANCE +added=[]` and nothing else | Not a bug — cooperative-sticky reports the **delta**. Also expected if pods outnumber partitions | Read the `-> owns N partitions` tail of the line, which is the running total |
| `maxReplicaCount` reached with idle pods | More replicas than partitions | Keep `maxReplicaCount` == partition count; leave `allowIdleConsumers: false` |
| Pods killed mid-batch, messages reprocessed | `terminationGracePeriodSeconds` below the worst-case batch time → SIGKILL, no LeaveGroup, group stalls for `session.timeout.ms` | Raise it above `PROCESS_DELAY_MS` (20 s here) and handle SIGTERM: stop polling → commit → `close()` |
| `ScaledObject` rejected on apply | The admission webhook found a conflict — usually two ScaledObjects targeting one Deployment, or a stale HPA on the same target | Read the rejection message; delete the conflicting object |
| ScaledObject hangs on delete | Stuck finalizer | `kubectl -n lab patch scaledobject orders-so -p '{"metadata":{"finalizers":null}}' --type=merge` |
| Broker restarted → topic and offsets gone | `emptyDir` volume | Re-create the topic ([[05 - LAB 1 — Local Cluster, Kafka & KEDA from Scratch]] §7) |

---

## Next

[[09 - LAB 3 — Break It on Purpose]] takes the failure modes further, on this same stack: rebalance churn from a too-small `lagThreshold`, lost offsets from an ungraceful shutdown, a poison message driving endless scale-up, and the `excludePersistentLag` baseline reset after an operator restart. The reference modules to read alongside are [[07 - The Kafka Scaler — Every Field & the Replica Math]] for the arithmetic, [[10 - Tuning Scaling Behaviour]] for the timing knobs, and [[14 - Observability & Debugging — Why Isn't It Scaling]] for a systematic debugging drill.

---

## Questions you should be able to answer

- Your kind-loaded image is on the node and the pod still says `ErrImagePull`. Why? *(`imagePullPolicy: Always`, often because the tag is `:latest`.)*
- Why must the consumer commit *while running*, not just on shutdown? *(KEDA reads committed offsets from the broker; without commits, lag never falls and KEDA scales to max against an idle cluster.)*
- Why `enable.auto.offset.store: False` alongside `enable.auto.commit: True`? *(Commit on a timer, but only offsets for messages actually finished — so a commit can't claim progress you haven't made.)*
- `TARGETS` says `150/100` with 2 replicas. What's the total lag? *(300 — the external metric uses `AverageValue`, so the HPA divides by replicas.)*
- `TARGETS: <unknown>`. Is lag zero? *(No. Zero reads `0/100`. `<unknown>` means the metric couldn't be read at all.)*
- Kafka says lag is 4000; KEDA's metric says 1200 and won't move. Bug? *(No — the partition ceiling: `12 × 100`. Which is why you never alert on KEDA's metric.)*
- Which component performs the 1 → 0 step, and which timer gates it? *(KEDA, writing the `scale` subresource; `cooldownPeriod`. An HPA can't set 0.)*
- Which timer gates N → 1? *(The HPA's `scaleDown.stabilizationWindowSeconds` — not `cooldownPeriod`.)*
- Three consumers, 12 partitions, and a log line reads `+added=[0,1,6,7]`. Why not all four of that pod's partitions in one line? *(cooperative-sticky reports the delta; the pod's total is tracked by the consumer itself.)*
- A brand-new group on a topic with 2 M retained messages, `offsetResetPolicy: earliest`. What does KEDA report? *(≈2 M — capped at `partitions × lagThreshold` — so instant max replicas.)*
- Same setup with `latest`? *(0 — the workload never wakes.)*
- What in `kafka-consumer-groups.sh --describe` tells you you're looking at this bug? *(`-` in `CURRENT-OFFSET` and `LAG`: no committed offset, so lag is undefined and `offsetResetPolicy` decides.)*

---

## Sources

- [KEDA — Apache Kafka scaler (documents `type: kafka`)](https://keda.sh/docs/2.20/scalers/apache-kafka/) · [ScaledObject spec](https://keda.sh/docs/2.20/reference/scaledobject-spec/) · [scaling deployments: activation vs scaling, `cooldownPeriod`](https://keda.sh/docs/2.20/concepts/scaling-deployments/) · [FAQ](https://keda.sh/docs/2.20/reference/faq/)
- [Kubernetes — HPA walkthrough (the formula, `AverageValue`)](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/) · [configurable scaling behaviour: policies and stabilization windows](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/#configurable-scaling-behavior)
- [Kubernetes — images and `imagePullPolicy` defaults](https://kubernetes.io/docs/concepts/containers/images/#image-pull-policy) · [Pod termination, SIGTERM and the grace period](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-termination)
- [kind — loading an image into your cluster](https://kind.sigs.k8s.io/docs/user/quick-start/#loading-an-image-into-your-cluster)
- [Apache Kafka 4.3 — consumer configuration (`auto.offset.reset`, `enable.auto.commit`, assignment strategies)](https://kafka.apache.org/43/configuration/consumer-configs/)
- [confluent-kafka-python — Consumer API (`store_offsets`, rebalance callbacks)](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#consumer)
- [KIP-429 — incremental cooperative rebalancing](https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol)
