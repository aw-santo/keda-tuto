---
title: 17 - LAB 4 — Production-Grade Kafka Autoscaling Capstone
tags:
  - keda
  - kafka
  - kubernetes
  - autoscaling
  - lab
  - senior
aliases:
  - KEDA Lab 4
  - Kafka Autoscaling Capstone
  - Production ScaledObject
status: ready
created: 2026-07-25
---

# LAB 4 — Production-Grade Kafka Autoscaling Capstone

> [!abstract] Scope
> The capstone. You have a working stack from [[05 - LAB 1 — Local Cluster, Kafka & KEDA from Scratch]] and [[06 - LAB 2 — Your First Kafka ScaledObject]], and you broke it deliberately in [[09 - LAB 3 — Break It on Purpose]]. Now you build the version you'd actually ship — and, more importantly, you **derive every setting from a stated requirement** instead of copying a config. That is the whole skill: nobody can tell you what `lagThreshold` should be, but a measurement and a deadline can. Shape: **goal → requirements → design decisions → build → verify → operate → checkpoint.**

Related: [[00 - KEDA Index]], [[05 - LAB 1 — Local Cluster, Kafka & KEDA from Scratch]], [[06 - LAB 2 — Your First Kafka ScaledObject]], [[07 - The Kafka Scaler — Every Field & the Replica Math]], [[08 - Scale-to-Zero & Activation]], [[09 - LAB 3 — Break It on Purpose]], [[10 - Tuning Scaling Behaviour]], [[11 - Multiple Triggers, Scaling Modifiers & Fallback]], [[12 - Authentication — TriggerAuthentication, SASL, TLS & MSK IAM]], [[14 - Observability & Debugging — Why Isn't It Scaling]], [[15 - Production Operations]]

> [!warning] Written against KEDA v2.20 · Kubernetes 1.33–1.35 · Apache Kafka 4.3
> Same honesty note as [[05 - LAB 1 — Local Cluster, Kafka & KEDA from Scratch]]: the Kafka broker, the consumer, the rebalance behaviour and the SIGTERM path in `lab/` were **exercised for real** on a laptop, and every manifest here is schema-validated against the real v2.20.1 CRDs — but **you are running the full KEDA→Kafka loop yourself.** Budget time to smoke-test rather than assuming first-try success, and when something doesn't move, go straight to the diagnostic ladder in [[14 - Observability & Debugging — Why Isn't It Scaling]].

---

## 1. Goal

Take the toy `ScaledObject` from Lab 2 and turn it into one you would defend in a design review: every field traceable to a requirement, a failure mode covered for each thing that broke in Lab 3, and enough observability that the person on call at 3 a.m. can tell in under a minute whether the problem is Kafka, KEDA, the consumer or the cluster.

You will end up with roughly 60 lines of YAML. The value is not the YAML — it's that you can say *why* each line has the value it has.

---

## 2. The requirements

Treat these as handed to you by a product owner and an SRE. They are the only inputs; everything downstream is derived.

| # | Requirement | What it constrains |
|---|---|---|
| **R1** | The `orders` topic has **12 partitions** and won't be repartitioned this quarter | the hard ceiling on useful replicas |
| **R2** | A **50,000-message burst** must be fully drained **within 10 minutes** | `lagThreshold`, `maxReplicaCount`, the scale-up ramp |
| **R3** | The workload must **idle to zero overnight** (no traffic 01:00–06:00) | `minReplicaCount`, `cooldownPeriod`, `activationLagThreshold` |
| **R4** | A **2-minute broker outage** must not collapse the workload to zero | `fallback` |
| **R5** | Scale-in must not **cause duplicate processing** | `terminationGracePeriodSeconds` + SIGTERM handling |
| **R6** | On-call must be able to tell **within a minute** why it isn't scaling | KEDA Prometheus metrics, a real lag alert, a runbook |

> [!important] R1 and R2 together decide whether the SLO is even achievable — before KEDA is involved
> Maximum useful throughput = **partition count × per-pod throughput**. Nothing KEDA does can exceed it, because a 13th consumer would sit idle. So the first thing to do is arithmetic, not YAML:
> - You will measure roughly **10 messages/second/pod** with the lab consumer at `PROCESS_DELAY_MS=100`.
> - Ceiling = 12 × 10 = **120 msg/s**.
> - 50,000 ÷ 120 ≈ **417 s ≈ 7 minutes** of drain at full width.
> - R2 allows 10 minutes, so you have about **3 minutes of slack** to spend on the ramp-up.
>
> If per-pod throughput had been 7 msg/s, the ceiling would be 84 msg/s → 595 s ≈ 9.9 minutes of drain with **no** slack, and the honest answer to R2 would be "more partitions or a faster consumer", not "better autoscaling". Being able to say that in a design review is the point of this lab.

---

## 3. Design decisions, with reasoning

Each row walks a requirement to a field. This table *is* the deliverable; the YAML in §4 is just its serialisation.

| Requirement | Decision | Reasoning |
|---|---|---|
| R1 | `maxReplicaCount: 12` | equal to the partition count. `allowIdleConsumers: false` (the default) already caps the metric, but stating the max documents the intent and protects you if someone flips that flag while debugging something else |
| R2 | `lagThreshold: "300"` | measured throughput × `pollingInterval` = 10 msg/s × 30 s. One unit of the metric then means "one pod's worth of work per poll", so `ceil(lag / 300)` moves in meaningful steps |
| R2 | `pollingInterval: 30` | the default, stated explicitly. Faster polling reacts sooner but queries the brokers more often; 30 s costs at most half a minute of reaction time against a 10-minute budget |
| R2 | `scaleUp.stabilizationWindowSeconds: 60` | Kubernetes applies **no** stabilisation to scale-up by default, and every replica change is a rebalance. 60 s means a spike must persist before it costs the group a stop-the-world pause — it buys ~1 minute of the 3-minute slack |
| R3 | `minReplicaCount: 0` | the requirement, literally |
| R3 | `cooldownPeriod: 300` | how long all triggers must be inactive before the final **1→0** step. Long enough that a gap between overnight trickles doesn't flap the workload on and off |
| R3 | `activationLagThreshold: "20"` | must sit **above the topic's idle lag floor**, or the workload never reaches zero. Measure the floor (§5, V3); on a plain non-transactional topic it's 0, so 20 is margin. Trade-off named below |
| R4 | `fallback: {failureThreshold: 3, replicas: 4, behavior: currentReplicasIfHigher}` | after a few consecutive failed metric reads (~90 s at a 30 s interval) KEDA serves a defined number instead of an error. `currentReplicasIfHigher` = `max(current, 4)`, so a busy workload isn't cut down mid-incident. KEDA also deliberately avoids scaling to zero while a fallback is configured and a scaler is erroring — which is exactly R4 |
| R5 | `terminationGracePeriodSeconds: 90` | must exceed worst-case in-flight processing **plus** the final commit **plus** `close()`. Kubernetes' default is 30 s; a consumer killed mid-batch loses its commit and those messages get redelivered after the rebalance |
| R5 | SIGTERM handling in the consumer | `lab/consumer.py` already does the right thing: stop polling → finish the current message → commit synchronously → `close()`, which sends LeaveGroup and triggers a prompt rebalance instead of a session-timeout stall |
| R5 | `scaleDown` policy of 2 pods per 60 s | scale-in one or two members at a time rather than all at once: fewer rebalances, and more time for in-flight work to land |
| R6 | KEDA Prometheus metrics + a **real** Kafka lag alert | KEDA's reported metric is capped at `partitions × lagThreshold` by design, so it *cannot* be your lag alert. Alert on lag from Kafka; alert on KEDA's scaler errors separately |

> [!warning] Gotcha — `activationLagThreshold: 20` means up to 20 messages can wait indefinitely
> Activation is strictly greater-than, so with a threshold of 20 a trickle of 15 overnight messages leaves the workload at zero until more arrive. That is a real latency trade-off you are choosing on purpose, not a bug: you traded worst-case latency for a guaranteed idle-at-zero.
> **Mitigation:** set the threshold from the *measured* floor plus a small margin, not from a round number — measure it in V3 below. If a message must never wait, `minReplicaCount: 1` is the honest answer and R3 has to change. For a one-off wake-up, the `autoscaling.keda.sh/force-activation: "true"` annotation activates the workload regardless of trigger state (remove it afterwards — the HPA still governs 1→N from real metrics).

> [!tip] Why not just crank everything for speed?
> Because every knob you turn toward "faster" is paid for in rebalances. `lagThreshold: 1` gives you a pod count that recomputes on every few messages; no scale-up stabilisation gives you a group that pauses whenever a spike blips. The production config is the one that reaches the deadline with the fewest scale events, not the one that reacts fastest. See [[10 - Tuning Scaling Behaviour]].

---

## 4. Build it

Everything below assumes the Lab 1 stack: a `kind` cluster with KEDA 2.20.1 in namespace `keda`, the `apache/kafka:4.3.1` StatefulSet and the `orders` topic in namespace `lab`, and the `keda-lab-client:0.4` image loaded into the cluster.

### 4.0 Preflight

```bash
# the one check that predicts everything else
kubectl get apiservice v1beta1.external.metrics.k8s.io      # must be AVAILABLE=True
kubectl -n keda get deploy                                   # operator, metrics-apiserver, admission
kubectl -n lab get sts kafka
kubectl -n lab exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic orders   # PartitionCount: 12
```

Remove Lab 2's `ScaledObject` so the two don't compete for the same Deployment — KEDA's webhook rejects a second `ScaledObject` on a workload, and it also rejects a `ScaledObject` on a workload that already has a foreign HPA:

```bash
kubectl -n lab delete scaledobject orders-so --ignore-not-found
```

### 4.1 The Deployment

Save as `lab/capstone-consumer.yaml`. Three differences from Lab 2's Deployment, all traceable to a requirement.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orders-consumer
  namespace: lab
spec:
  replicas: 1                     # KEDA/HPA owns this field from here on
  selector:
    matchLabels:
      app: orders-consumer
  template:
    metadata:
      labels:
        app: orders-consumer
    spec:
      # R5: worst-case in-flight work + final commit + close()/LeaveGroup, with margin.
      # Kubernetes' default is 30s. A preStop hook that overruns gets only a
      # one-off 2-second extension, so size this field, not the hook.
      terminationGracePeriodSeconds: 90
      containers:
        - name: consumer
          image: keda-lab-client:0.4
          imagePullPolicy: IfNotPresent      # REQUIRED for kind-loaded local images
          env:
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: KAFKA_BOOTSTRAP
              value: "kafka-0.kafka.lab.svc.cluster.local:9092"
            - name: KAFKA_TOPIC
              value: "orders"
            - name: KAFKA_GROUP
              value: "orders-workers"
            - name: PROCESS_DELAY_MS
              value: "100"                   # ~10 msg/s/pod — the number R2 is built on
          # R2 + node autoscaling: requests are how the scheduler and any node
          # autoscaler reason about your 12 pods. A pod with no requests tells
          # them nothing, and 12 of them will land wherever they fit.
          resources:
            requests:
              cpu: 50m
              memory: 64Mi
            limits:
              cpu: 300m
              memory: 192Mi
```

**On probes.** A Kafka consumer receives no inbound traffic, so *readiness* buys you less than usual — it gates rollouts and `PodDisruptionBudget` maths, not routing. *Liveness* is the valuable one: a consumer that has stopped polling is invisible to Kubernetes and looks perfectly healthy while its partitions rot. The shape you want:

```yaml
          # Requires a two-line change to consumer.py: touch /tmp/healthy after each
          # successful poll. Without that heartbeat, this probe has nothing to read —
          # this is the SHAPE, and the reason liveness matters more than readiness here.
          livenessProbe:
            exec:
              command: ["sh", "-c", "test $(( $(date +%s) - $(stat -c %Y /tmp/healthy) )) -lt 60"]
            initialDelaySeconds: 20
            periodSeconds: 20
            failureThreshold: 3
```

The mistake to avoid is a liveness probe that only proves the process exists (`pgrep python`) — a wedged consumer passes that forever. Prove *progress*, not existence.

### 4.2 The ScaledObject

Save as `lab/capstone-scaledobject.yaml`. This is the artefact.

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: orders-consumer            # keep it short: "keda-hpa-" + this must fit in 63 chars
  namespace: lab
spec:
  scaleTargetRef:
    name: orders-consumer

  pollingInterval: 30              # R2: how often KEDA asks Kafka. Default, stated on purpose
  cooldownPeriod: 300              # R3: governs ONLY the final 1 -> 0 step
  minReplicaCount: 0               # R3: idle to zero
  maxReplicaCount: 12              # R1: = partition count

  fallback:                        # R4: survive a broker outage without collapsing
    failureThreshold: 3            # ~3 consecutive failed reads before it engages
    replicas: 4
    behavior: currentReplicasIfHigher    # max(current, 4) — never cuts a busy workload down

  advanced:
    horizontalPodAutoscalerConfig:
      behavior:
        scaleUp:
          stabilizationWindowSeconds: 60   # R2: k8s default is 0. Damp rebalance churn
          policies:                        # the k8s defaults, written out to document intent
            - {type: Percent, value: 100, periodSeconds: 15}
            - {type: Pods,    value: 4,   periodSeconds: 15}
          selectPolicy: Max
        scaleDown:
          stabilizationWindowSeconds: 300  # the k8s default; a rolling maximum over 5 min
          policies:
            - {type: Pods, value: 2, periodSeconds: 60}   # R5: shed members gently

  triggers:
    - type: kafka                  # the STABLE Sarama scaler, not "apache-kafka"
      metadata:
        bootstrapServers: kafka-0.kafka.lab.svc.cluster.local:9092
        consumerGroup: orders-workers
        topic: orders              # always explicit
        lagThreshold: "300"        # R2: measured 10 msg/s x 30 s pollingInterval
        activationLagThreshold: "20"   # R3: above the measured idle lag floor
        offsetResetPolicy: earliest     # MUST match the consumer's auto.offset.reset
        excludePersistentLag: "true"    # damp a wedged partition driving endless scale-up
      # Uncomment ONLY against a SASL/TLS broker — see 4.3.
      # authenticationRef:
      #   name: kafka-credentials
```

Apply and read back what KEDA decided, rather than assuming:

```bash
kubectl apply -f lab/capstone-consumer.yaml -f lab/capstone-scaledobject.yaml
kubectl -n lab get scaledobject orders-consumer
kubectl -n lab get scaledobject orders-consumer -o jsonpath='{.status.hpaName}{"\n"}'
kubectl -n lab get scaledobject orders-consumer -o jsonpath='{.status.externalMetricNames}{"\n"}'
kubectl -n lab describe scaledobject orders-consumer | tail -25   # conditions + events
```

Expect `hpaName: keda-hpa-orders-consumer` and one external metric named `s0-kafka-orders`. If `Ready` isn't `True`, stop and fix that before running any test below.

### 4.3 A TriggerAuthentication — the shape, not the security

The lab broker is **PLAINTEXT with no SASL**, so there are no real credentials to supply. Build it anyway, because the shape is what you'll reuse, and getting the shape wrong in production is a bad afternoon. Save as `lab/capstone-auth.yaml`:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-credentials
  namespace: lab
type: Opaque
stringData:                        # stringData, NOT data — data expects base64
  sasl: "scram_sha512"
  tls: "enable"
  username: "orders-consumer"
  password: "not-a-real-password-this-broker-has-no-auth"
---
apiVersion: keda.sh/v1alpha1
kind: TriggerAuthentication
metadata:
  name: kafka-credentials
  namespace: lab
spec:
  secretTargetRef:
    - {parameter: sasl,     name: kafka-credentials, key: sasl}
    - {parameter: tls,      name: kafka-credentials, key: tls}
    - {parameter: username, name: kafka-credentials, key: username}
    - {parameter: password, name: kafka-credentials, key: password}
```

```bash
kubectl apply -f lab/capstone-auth.yaml
kubectl -n lab get triggerauthentication kafka-credentials
```

> [!warning] This is a demonstration of structure, not of security — and wiring it up here will break the trigger
> The `authenticationRef` above is commented out deliberately. Point a `sasl: scram_sha512` + `tls: enable` credential at a plaintext broker and the scaler fails to connect; the `ScaledObject` goes not-Ready and you've manufactured an incident to debug. What this object teaches is the **shape**: credentials live in a Secret, the `TriggerAuthentication` maps Secret keys to *parameter names the scaler understands*, and the trigger references it by name. Nothing about it makes the lab secure.
> **Mitigation:** in a real cluster, treat KEDA's secret access as part of your blast radius — the operator reads any Secret a `TriggerAuthentication` names, so scope it, prefer an external secret store or workload identity over long-lived passwords, and remember `ClusterTriggerAuthentication` is cluster-scoped by design. Full treatment in [[12 - Authentication — TriggerAuthentication, SASL, TLS & MSK IAM]].

### 4.4 Turn KEDA's own metrics on (R6)

The operator binary defaults Prometheus metrics **on**, but the Helm chart renders the flag from a value that defaults to **false** — so a plain `helm install` gives you an operator with metrics disabled. People lose hours to this.

```bash
helm upgrade keda kedacore/keda -n keda --version 2.20.1 \
  --set prometheus.operator.enabled=true \
  --set prometheus.metricServer.enabled=true

kubectl -n keda rollout status deploy/keda-operator
# confirm the flag actually landed
kubectl -n keda get deploy keda-operator -o jsonpath='{.spec.template.spec.containers[0].args}{"\n"}'

# scrape it by hand — no Prometheus needed to prove it works
kubectl -n keda port-forward deploy/keda-operator 8080:8080 &
curl -s localhost:8080/metrics | grep -E '^keda_(scaler_metrics_value|scaler_active|scaled_object_errors_total)'
```

Metric names to remember (they carry `_total` / `_seconds` suffixes; several commonly-cited shorter names don't exist): `keda_scaler_metrics_value`, `keda_scaler_active`, `keda_scaler_detail_errors_total`, `keda_scaled_object_errors_total`, `keda_scaler_metrics_latency_seconds`. KEDA also ships a first-party Grafana dashboard JSON in its own repository — there is none published on grafana.com, so don't go looking for a dashboard ID.

---

## 5. Verify against each requirement

One test per requirement, each with the observation that counts as a pass. Run them in order; V1 produces the number the rest depend on.

### V1 — Measure per-pod throughput (the input to R2)

Pin the workload at exactly one replica using the pause annotation, so the measurement isn't contaminated by scaling:

```bash
kubectl -n lab annotate scaledobject orders-consumer autoscaling.keda.sh/paused-replicas="1" --overwrite

kubectl -n lab run burst --rm -i --restart=Never \
  --image=keda-lab-client:0.4 --image-pull-policy=IfNotPresent \
  --env=KAFKA_BOOTSTRAP=kafka-0.kafka.lab.svc.cluster.local:9092 \
  --env=KAFKA_TOPIC=orders --env=MESSAGE_COUNT=3000 --env=KEY_CARDINALITY=1200 \
  -- /app/producer.py

# time the drain with a lag poll loop
start=$(date +%s)
while true; do
  lag=$(kubectl -n lab exec kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server localhost:9092 --describe --group orders-workers 2>/dev/null \
        | awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}')
  echo "$(( $(date +%s) - start ))s lag=$lag"
  [ "$lag" = "0" ] && break
  sleep 10
done
```

**Expect** roughly 300 seconds for 3,000 messages → **~10 msg/s/pod**, consistent with `PROCESS_DELAY_MS=100`. That is the number behind `lagThreshold: 300` and behind the 7-minute drain estimate. If your machine gives 6 msg/s, recompute — and notice that the 10-minute SLO gets uncomfortable, which is the lesson.

```bash
# resume: for paused-replicas, REMOVING the annotation is required — setting it "false" won't do
kubectl -n lab annotate scaledobject orders-consumer autoscaling.keda.sh/paused-replicas-
```

### V2 — The 50,000-message burst (R2)

```bash
# watch in one terminal
watch -n2 'kubectl -n lab get scaledobject,hpa,deploy; kubectl -n lab get pods --no-headers | wc -l'

# fire in another
kubectl -n lab run burst --rm -i --restart=Never \
  --image=keda-lab-client:0.4 --image-pull-policy=IfNotPresent \
  --env=KAFKA_BOOTSTRAP=kafka-0.kafka.lab.svc.cluster.local:9092 \
  --env=KAFKA_TOPIC=orders --env=MESSAGE_COUNT=50000 --env=KEY_CARDINALITY=1200 \
  -- /app/producer.py
```

**Expect, in this order:** lag jumps to ~50,000 → KEDA's reported metric **stops at 3,600** (`12 × 300`, the partition clamp — this is correct, not a stuck exporter) → the HPA ramps 1 → 5 → 10 → 12 over roughly 100 seconds once the 60-second stabilisation window is satisfied → each new pod logs a `REBALANCE +added=` line and existing pods log `-revoked=` → total drain **7–9 minutes**. Re-run the lag poll loop from V1 to time it.

The `TARGETS` column on `kubectl get hpa` shows *average lag per replica* against 300, because `metricType` defaults to `AverageValue`. `<unknown>` there means the metrics path or the trigger is broken — **not** that lag is zero.

### V3 — Idle to zero, and measure the real floor (R3)

Stop producing and watch it come down:

```bash
kubectl -n lab get pods -w
kubectl -n lab get scaledobject orders-consumer -o jsonpath='{.status.lastActiveTime}{"\n"}'
```

**Expect** the HPA to walk down two pods per minute (your `scaleDown` policy) to **1**, not 0 — the generated HPA's `minReplicas` is 1 no matter what you set, because an HPA can't express zero. Then **KEDA** takes the last step 1 → 0, `cooldownPeriod` (300 s) after `lastActiveTime`. Total: several minutes. If you never reach zero, read the idle floor first:

```bash
kubectl -n lab exec kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group orders-workers
```

Fully drained, a plain topic reports 0 across the board. A **transactional or compacted** topic keeps a permanent floor of roughly one per partition, because transaction markers occupy offsets and compaction leaves offsets valid — which is exactly why `activationLagThreshold` must sit above the floor. Measure yours; don't inherit the number 20 from this note.

### V4 — Survive a 2-minute broker outage (R4)

Get to a scaled-out state first (rerun a smaller burst, wait for 4+ pods), then take Kafka away:

```bash
kubectl -n lab scale sts kafka --replicas=0
date; kubectl -n lab get pods -w        # watch the CONSUMER pods, not Kafka
# ... after ~2 minutes ...
kubectl -n lab scale sts kafka --replicas=1
kubectl -n lab rollout status sts/kafka
```

**Expect** the consumer replica count to be **held**, not driven to zero. Three mechanisms combine, and it's worth being able to name all three:

1. The HPA can't read its external metric, and when a metric is unreadable while others suggest scaling down, the HPA **skips scaling entirely**.
2. After `failureThreshold` consecutive failures, KEDA's `fallback` resolves the metric to `max(current, 4)` instead of an error.
3. KEDA deliberately avoids the scale-to-zero path while a `fallback` is configured and a scaler is failing.

Confirm from the resource itself:

```bash
kubectl -n lab get scaledobject orders-consumer -o jsonpath='{.status.health}{"\n"}'   # numberOfFailures / status
kubectl -n lab describe scaledobject orders-consumer | grep -A3 -i fallback
kubectl -n keda logs deploy/keda-operator --tail=50 | grep -i -E "orders-consumer|fallback|error"
```

> [!warning] Gotcha — `fallback` can't lift a workload that's already at zero as reliably as it holds one that isn't
> Test V4 from a scaled-out state. Starting the test at zero replicas mixes two different questions (does fallback hold a count / can activation happen with a dead broker) and you'll misread the result. Also note `fallback` requires at least one non-CPU/memory trigger with the `AverageValue` metric type — which is the default, and is why the config above just works.
> **Mitigation:** verify `fallback` deliberately, from a known replica count, and alert on `keda_scaler_detail_errors_total` so that in production you learn the broker is unreachable from a page rather than from a lag graph three hours later.

### V5 — No duplicate processing on scale-in (R5)

The measurable definition: after a scale-in, **no partition's committed offset goes backwards.** A regression means messages were redelivered.

```bash
# snapshot committed offsets
kubectl -n lab exec kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group orders-workers \
  | awk 'NR>1 && $4 ~ /^[0-9]+$/ {print $3, $4}' | sort -n > /tmp/before.txt

# force a graceful scale-in of one member
kubectl -n lab delete pod -l app=orders-consumer --field-selector=status.phase=Running \
  --wait=true $(: pick one) 2>/dev/null || kubectl -n lab delete pod \
  "$(kubectl -n lab get pods -l app=orders-consumer -o name | head -1 | cut -d/ -f2)"

kubectl -n lab logs "<the deleted pod>" --previous 2>/dev/null | tail -5    # if it restarted
# ... re-snapshot and compare
```

**Expect** in the terminating pod's logs, in this order: `signal SIGTERM received -> draining and leaving group` → `committing final offsets` → `REBALANCE -revoked=` → `closed cleanly`, all well inside 90 seconds; and surviving pods picking up the freed partitions within a second or two rather than after a session timeout. Committed offsets only move forward.

**Now break it on purpose**, because the contrast is the lesson:

```bash
kubectl -n lab delete pod "<a running consumer>" --grace-period=0 --force
```

SIGKILL with no drain: no final commit, no LeaveGroup. The group stalls until the session timeout expires, and the partitions' committed offsets are re-read from the last successful auto-commit — so everything processed since then is delivered again to whoever inherits them. That is duplicate processing *caused by autoscaling*, and the only reason V5 passes normally is the grace period plus the consumer's signal handler.

> [!important] "No duplicates on scale-in" is not "exactly once"
> Kafka is at-least-once. A crashed pod, a network partition, a rebalance during an uncommitted batch — all of them redeliver, and no autoscaler configuration changes that. What R5 actually buys you is that **routine scale-in isn't a source of duplicates**. Genuine safety comes from idempotent processing on the consumer side, and for long per-message work from a pod-per-message design ([[13 - ScaledJob — When a Pod per Message Beats a Consumer Pool]]).

### V6 — One-minute diagnosis (R6)

Time yourself. From "it isn't scaling", these four commands should tell you which layer owns the problem:

```bash
kubectl -n lab get scaledobject orders-consumer          # READY / ACTIVE / FALLBACK / PAUSED
kubectl -n lab get hpa keda-hpa-orders-consumer          # TARGETS: <current>/<target>, REPLICAS
kubectl -n lab get pods -l app=orders-consumer           # Running vs Pending
kubectl -n lab exec kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group orders-workers   # real lag, is CURRENT-OFFSET moving?
```

If those four don't localise it, the next rung is the raw external-metrics API, and the label selector is mandatory:

```bash
kubectl get --raw "/apis/external.metrics.k8s.io/v1beta1/namespaces/lab/s0-kafka-orders?labelSelector=scaledobject.keda.sh%2Fname%3Dorders-consumer" | jq
```

Full ladder in [[14 - Observability & Debugging — Why Isn't It Scaling]].

---

## 6. Operate it

### 6.1 The four alerts worth having

Save as `lab/capstone-alerts.yaml`. Alert **1** needs a real lag exporter (see §7); alerts **3** and **4** need kube-state-metrics.

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: orders-consumer-autoscaling
  namespace: lab
spec:
  groups:
    - name: orders-consumer
      rules:
        # 1. REAL Kafka lag. KEDA's own metric is capped at partitions x lagThreshold,
        #    so it looks healthy during a genuine backlog emergency. Never alert on it.
        - alert: OrdersConsumerLagHigh
          expr: sum(kafka_consumergroup_lag{consumergroup="orders-workers",topic="orders"}) > 20000
          for: 10m
          labels: {severity: warning}
          annotations:
            summary: "orders lag above 20k for 10m — is the group at max replicas?"

        # 2. The scaler can't read Kafka. This is the alert that would have paged you
        #    during V4 instead of leaving you to notice hours later.
        - alert: KedaScalerErrors
          expr: increase(keda_scaler_detail_errors_total[5m]) > 0
          for: 5m
          labels: {severity: warning}
          annotations:
            summary: "KEDA scaler errors — broker unreachable, auth, or a bad trigger"

        # 3. Sustained at max: autoscaling has done all it can. Either the consumer is
        #    too slow or you need more partitions.
        - alert: OrdersConsumerPinnedAtMax
          expr: |
            kube_horizontalpodautoscaler_status_current_replicas{horizontalpodautoscaler="keda-hpa-orders-consumer"}
            >= kube_horizontalpodautoscaler_spec_max_replicas{horizontalpodautoscaler="keda-hpa-orders-consumer"}
          for: 15m
          labels: {severity: warning}

        # 4. KEDA asked for pods the cluster can't place. Not a KEDA problem — capacity.
        - alert: OrdersConsumerPodsPending
          expr: sum(kube_pod_status_phase{namespace="lab",phase="Pending"}) > 0
          for: 5m
          labels: {severity: warning}
```

> [!warning] Gotcha — there is no KEDA metric for "ScaledObject is not Ready"
> The obvious fifth alert doesn't have an obvious metric behind it. KEDA exposes error counters and a paused gauge, not a gauge for the `Ready` condition, so `keda_scaledobject_ready` (and similar names people write from memory) doesn't exist. Label names on the metrics that *do* exist also shift between versions.
> **Mitigation:** scrape the operator once (`curl` on port 8080, as in §4.4) and copy the exact names and labels you see into your rules. For the condition itself, use one of the two real routes: kube-state-metrics' custom-resource-state configuration to expose `.status.conditions`, or a **CloudEventSource** that pushes `keda.scaledobject.failed.v1` to your alerting endpoint ([[14 - Observability & Debugging — Why Isn't It Scaling]]).

### 6.2 Runbook — symptom to first action

| Symptom | First action | Usual cause |
|---|---|---|
| Lag climbing, replicas at `maxReplicaCount` | `kafka-consumer-groups.sh --describe`: is `CURRENT-OFFSET` advancing? | if not, the consumer isn't committing — a consumer bug, not an autoscaling one. If yes, you're at the partition ceiling |
| Lag climbing, replicas **not** rising | `kubectl get hpa` — is `TARGETS` `<unknown>`? | the metrics path or the trigger is broken. Check the `APIService`, then `describe scaledobject` |
| Desired replicas high, pods `Pending` | `kubectl describe pod` → scheduling events | node capacity. KEDA never provisions nodes ([[16 - Alternatives & Trade-offs]]) |
| Won't wake from zero | `.status.conditions`, then real lag vs `activationLagThreshold` | lag below the activation threshold, a failing scaler, or a paused annotation |
| Won't go **to** zero | is any trigger still active? Has `cooldownPeriod` elapsed since `lastActiveTime`? | a transactional/compacted lag floor above `activationLagThreshold`, or a CPU trigger that's always active |
| Replica count flapping | `lagThreshold` vs measured throughput; `scaleUp.stabilizationWindowSeconds` | threshold too small for the traffic shape |
| Duplicate side effects after a scale event | terminating pod's logs: did it commit and close cleanly inside the grace period? | grace period too short, or no SIGTERM handling |
| Replicas frozen at an odd number, nothing in logs | `kubectl get scaledobject -o yaml` → check annotations | a leftover `paused` / `paused-replicas` / `paused-scale-in` annotation. Note that *any* value pauses — even `"maybe"` — and `paused-replicas` must be **removed**, not set to `false` |
| Everything unreadable, cluster-wide | `kubectl get apiservice v1beta1.external.metrics.k8s.io` | the adapter is down, or something else claimed the external-metrics API group |

### 6.3 Before a KEDA upgrade

KEDA's CRDs are permanently `v1alpha1` by maintainer decision, so **breaking changes ship in minor releases**. Treat every upgrade as a change, not a patch.

- [ ] Pin the chart version explicitly (`--version 2.20.1`), never track "latest".
- [ ] Read the CHANGELOG's Breaking Changes **and** the release's upgrade notes — v2.20's most disruptive change (Kubernetes events moving to the `events.k8s.io` API group, which needs new RBAC if you run restricted RBAC) is *not* in the Breaking Changes section.
- [ ] Check the Kubernetes support window: KEDA supports N-2, and v2.20 is tested against 1.33–1.35 only.
- [ ] Check your `ScaledObject` names: v2.20 rejects names whose derived HPA name exceeds 63 characters, so a name over ~54 characters that used to be accepted can now be refused on update.
- [ ] Grep your manifests for fields removed in recent minors, and for deprecations announced two releases ago.
- [ ] Back up your CRs: `kubectl get scaledobject,scaledjob,triggerauthentication -A -o yaml > keda-backup.yaml`.
- [ ] **Never run `helm uninstall keda` as part of an upgrade.** The chart ships CRDs as ordinary templates with no retention policy, so uninstalling deletes them — cascading to every `ScaledObject`, `ScaledJob` and `TriggerAuthentication` in the cluster. Use `helm upgrade`.
- [ ] Upgrade in a non-production cluster first and confirm `Ready=True` on a sample of `ScaledObject`s afterwards, plus `AVAILABLE=True` on the `APIService`.

More in [[15 - Production Operations]].

---

## 7. What would change in a real cluster

This lab is deliberately a single-node, single-broker, no-auth toy. The differences that matter:

| Lab | Production |
|---|---|
| PLAINTEXT broker, credentials for shape only | real SASL/TLS or workload identity, with the `authenticationRef` actually wired in; secrets from an external store, not `stringData` |
| 1 broker, `replication-factor 1`, `emptyDir` | ≥3 brokers, RF ≥ 3, `min.insync.replicas: 2`, real PersistentVolumes. RF 1 means a broker restart loses data — fine for a lab, indefensible otherwise |
| Nothing provisions nodes; 12 pods happen to fit | Cluster Autoscaler or Karpenter, and node provisioning time counted **inside** your drain-time budget |
| No `PodDisruptionBudget` | a PDB so a node drain can't take the whole consumer group down at once — and note it interacts with scale-to-zero, since a PDB with `minAvailable: 1` and a workload that wants 0 replicas need a deliberate decision |
| Requests guessed (`50m` / `64Mi`) | requests derived from real usage over a representative week — VPA in recommendation mode is a good way to get the numbers without letting it evict your consumers |
| Lag read by hand via `kubectl exec` | a real lag exporter feeding Prometheus (Kafka Exporter, Burrow, KMinion or your provider's equivalent), because KEDA's own metric is capped and can't serve as the alert |
| Default `pollingInterval: 30` against one broker | consider `useCachedMetrics: true` on the trigger so the brokers are queried on the polling interval only, instead of additionally on every HPA sync — at the cost of the HPA acting on data up to one interval old |
| Classic rebalance protocol | on Kafka ≥4.0, evaluate `group.protocol=consumer` (KIP-848) on the consumer: it removes the global synchronisation barrier from rebalancing, which is the single highest-leverage mitigation for scale-churn pain |
| One namespace, one topic | `watchNamespace` scoping, separate `ScaledObject`s per workload, and a naming convention that keeps `keda-hpa-<name>` under 63 characters |

---

## 8. Checkpoint

You're done when you can do all of these without looking anything up:

- [ ] Derive `lagThreshold` from a measurement you took yourself, and explain the units.
- [ ] State the throughput ceiling of your topic and say whether the SLO is achievable at all.
- [ ] Explain what the HPA does with your metric, and why KEDA's reported value stopped at 3,600.
- [ ] Point at the one field that owns each of R1–R6.
- [ ] Watch the workload reach 12 pods, drain 50,000 messages, walk down to 1, and then be taken to 0 by KEDA rather than the HPA — and narrate who did what.
- [ ] Kill Kafka for two minutes and predict the replica count before you look.
- [ ] Kill a consumer pod gracefully and then with `--grace-period=0`, and explain the difference in the committed offsets.
- [ ] Diagnose "it isn't scaling" in four commands.
- [ ] List what you'd change before shipping this to a real cluster.

Teardown:

```bash
kubectl -n lab delete scaledobject orders-consumer
kubectl -n lab delete -f lab/capstone-consumer.yaml -f lab/capstone-auth.yaml
kind delete cluster --name keda-lab
```

Note that deleting the `ScaledObject` does **not** restore the original replica count unless you set `advanced.restoreToOriginalReplicaCount: true` — delete one while it's scaled to zero and the workload stays at zero. That has caused real incidents; it's on the checklist in [[15 - Production Operations]] for a reason.

---

## 9. Questions you should be able to answer

- Where did `lagThreshold: 300` come from? *(Measured per-pod throughput × `pollingInterval` — 10 msg/s × 30 s. One metric unit = one pod's work per poll.)*
- Is a 10-minute SLO for 50,000 messages achievable on a 12-partition topic? *(Only if per-pod throughput × 12 clears it with room for the ramp. 12 × 10 msg/s = 120 msg/s → ~7 min of drain, so yes with ~3 min of slack. At 7 msg/s it isn't, and the fix is partitions or a faster consumer.)*
- Which field satisfies "don't collapse during a 2-minute broker outage", and what else helps? *(`fallback`. Plus the HPA skipping scale-down on an unreadable metric, and KEDA declining the scale-to-zero path while a fallback is configured and a scaler is erroring.)*
- Why is `terminationGracePeriodSeconds: 90` a correctness setting rather than a nicety? *(Below worst-case processing time the pod is SIGKILLed mid-batch, loses its commit, and those messages are redelivered after the rebalance — duplicates caused by autoscaling.)*
- Who performs the 1 → 0 step, and after how long? *(KEDA's operator, directly on the scale subresource, `cooldownPeriod` after `lastActiveTime`. The HPA can only go down to 1.)*
- Why can't you alert on KEDA's reported metric? *(It's clamped to `partitions × lagThreshold`, so it flatlines at a healthy-looking value during a genuine backlog. Alert on lag from Kafka.)*
- Your `ScaledObject` is Ready and Active, the HPA wants 12 pods, and lag isn't falling. Next check? *(Pod status. Pending pods mean node capacity, not autoscaling.)*
- Why does `activationLagThreshold` need to be greater than zero on some topics? *(Transaction markers and compaction leave a permanent lag floor of roughly one per partition, so a threshold of 0 means the workload never idles at zero.)*
- What single command tells you whether the whole external-metrics path is alive? *(`kubectl get apiservice v1beta1.external.metrics.k8s.io` — if it isn't Available, nothing else matters.)*

---

## 10. Sources

- [KEDA — ScaledObject specification (`pollingInterval`, `cooldownPeriod`, `fallback`, `advanced`)](https://keda.sh/docs/2.20/reference/scaledobject-spec/)
- [KEDA — Apache Kafka scaler (documents `type: kafka`)](https://keda.sh/docs/2.20/scalers/apache-kafka/)
- [KEDA — Scaling Deployments: activation vs scaling, pause annotations](https://keda.sh/docs/2.20/concepts/scaling-deployments/)
- [KEDA — Authentication & TriggerAuthentication](https://keda.sh/docs/2.20/concepts/authentication/)
- [KEDA — Prometheus metrics exposed by the operator and metrics adapter](https://keda.sh/docs/2.20/integrations/prometheus/)
- [KEDA — Cluster operations, firewall requirements and the Kubernetes compatibility matrix](https://keda.sh/docs/2.20/operate/cluster/)
- [KEDA — FAQ (one external metrics server per cluster)](https://keda.sh/docs/2.20/reference/faq/)
- [KEDA — Helm chart values (`prometheus.operator.enabled`, `crds.install`)](https://github.com/kedacore/charts/tree/v2.20.1/keda)
- [KEDA — CHANGELOG (breaking changes ship in minor releases)](https://github.com/kedacore/keda/blob/main/CHANGELOG.md)
- [KEDA — first-party Grafana dashboard JSON](https://github.com/kedacore/keda/blob/v2.20.1/config/grafana/keda-dashboard.json)
- [Kubernetes — HorizontalPodAutoscaler walkthrough (stabilisation windows, scaling policies, skipped scale-down on unreadable metrics)](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/)
- [Kubernetes — Pod lifecycle and termination (grace period, the 2-second preStop extension)](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/)
- [Kubernetes — Configure liveness, readiness and startup probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)
- [Kubernetes — PodDisruptionBudget](https://kubernetes.io/docs/tasks/run-application/configure-pdb/)
- [Apache Kafka 4.3 — basic operations (`kafka-consumer-groups.sh --describe`)](https://kafka.apache.org/43/operations/basic-kafka-operations/)
- [Apache Kafka 4.3 — consumer configuration (`auto.offset.reset`, `max.poll.records`, assignors)](https://kafka.apache.org/43/configuration/consumer-configs/)
- [Apache Kafka 4.3 — the next-generation consumer rebalance protocol (`group.protocol=consumer`)](https://kafka.apache.org/43/operations/consumer-rebalance-protocol/)
- [Apache Kafka 4.3 — monitoring](https://kafka.apache.org/43/operations/monitoring/)
- [Prometheus Operator — PrometheusRule API](https://prometheus-operator.dev/docs/api-reference/api/)
- [kube-state-metrics — HPA and Pod metrics, plus custom-resource-state configuration](https://github.com/kubernetes/kube-state-metrics/tree/main/docs)
- [Kafka Exporter (`kafka_consumergroup_lag`)](https://github.com/danielqsj/kafka_exporter)
- [LinkedIn Burrow — consumer lag monitoring](https://github.com/linkedin/Burrow)
