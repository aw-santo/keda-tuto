---
title: ScaledJob — When a Pod per Message Beats a Consumer Pool
tags:
  - keda
  - kafka
  - autoscaling
  - kubernetes
  - jobs
  - senior
aliases:
  - ScaledJob
  - jobTargetRef
  - scalingStrategy
  - KEDA Jobs
status: ready
created: 2026-07-25
---

# ScaledJob — When a Pod per Message Beats a Consumer Pool

> [!abstract] Scope
> KEDA's other CRD. A `ScaledObject` scales a pool of long-lived consumers; a `ScaledJob` creates **one Kubernetes Job per unit of work** that runs to completion and exits. This module covers what's structurally different (no HPA, therefore no `cooldownPeriod`, no `fallback`, no `advanced`), every field worth setting, and — most importantly — the two places people go wrong: reaching for it for sub-second work, and believing it avoids Kafka rebalancing. **It usually causes more rebalancing, not less.** Builds on [[07 - The Kafka Scaler — Every Field & the Replica Math]] and the rebalance cost from [[02 - Kafka Consumer Groups, Partitions & Rebalancing]].

Related: [[00 - KEDA Index]], [[02 - Kafka Consumer Groups, Partitions & Rebalancing]], [[04 - The ScaledObject CRD, End to End]], [[07 - The Kafka Scaler — Every Field & the Replica Math]], [[08 - Scale-to-Zero & Activation]], [[11 - Multiple Triggers, Scaling Modifiers & Fallback]], [[12 - Authentication — TriggerAuthentication, SASL, TLS & MSK IAM]], [[14 - Observability & Debugging — Why Isn't It Scaling]], [[16 - Alternatives & Trade-offs]]

> [!warning] Written against KEDA v2.20 · Kubernetes 1.33–1.35 · Apache Kafka 4.3
> `ScaledJob` is `v1alpha1` like the rest of KEDA's API, and it has already deprecated one field in place (`rolloutStrategy` → `rollout.strategy`). Check the [ScaledJob spec](https://keda.sh/docs/2.20/reference/scaledjob-spec/) for the version you run.

---

## 1. The core distinction

A `ScaledObject` points at a workload that already exists and adjusts its replica count. Consumers stay warm, hold a partition assignment, and process message after message.

A `ScaledJob` doesn't scale anything that exists. **It creates Kubernetes Jobs.** Every `pollingInterval`, KEDA asks the trigger how much work is waiting, works out how many Jobs it wants running, and creates that many. Each Job starts, does one unit of work, and exits. KEDA cleans up the finished ones.

KEDA's own criterion for choosing it is one sentence, and it's the right filter:

> "The primary reason to consider this option is to handle processing **long-running executions**. Rather than processing multiple events within a deployment, for each detected event a single Kubernetes Job is scheduled. That job will initialize, pull a single event from the message source, and process to completion and terminate."

**"Long-running" means minutes to hours, not milliseconds.** Video transcoding. A model inference batch. A monthly report. If your per-message work is measured in milliseconds, everything in this module is a downgrade from a warm consumer, and §7 explains exactly why.

The lifecycle for a Kafka-triggered ScaledJob:

1. No lag → no Jobs. Nothing runs, nothing costs anything.
2. Lag appears → KEDA creates Jobs on the next poll.
3. Each Job starts, reads its work, processes to completion, exits.
4. More lag → more Jobs, up to `maxReplicaCount`.
5. Finished Jobs are pruned to `successfulJobsHistoryLimit` / `failedJobsHistoryLimit`.

---

## 2. What's structurally different

This is the table to internalise. Most `ScaledJob` confusion is someone reaching for a `ScaledObject` field that simply doesn't exist here.

| | `ScaledObject` | `ScaledJob` |
|---|---|---|
| What it scales | an existing Deployment/StatefulSet/anything with a `scale` subresource | nothing — it **creates** Jobs |
| How you name the work | `scaleTargetRef` (a reference) | `jobTargetRef` (a **full Job spec**, `template` required) |
| Creates an HPA? | **yes**, one per object, and owns it | **no. None at all** |
| Who decides the count | the HPA divides the metric | **KEDA**, on each poll |
| `cooldownPeriod` | ✓ | ✗ doesn't exist |
| `idleReplicaCount` | ✓ | ✗ doesn't exist |
| `fallback` | ✓ | ✗ **doesn't exist** |
| `advanced.horizontalPodAutoscalerConfig` | ✓ | ✗ doesn't exist — there's no HPA to configure |
| Scaling shape control | HPA `behavior` (stabilization windows, policies) | `scalingStrategy` |
| Scale to zero | KEDA writes the `scale` subresource | zero is just "create no Jobs" — the natural state |
| Short name | `so` | `sj` |

The single fact that explains the whole right-hand column: **there is no HPA.** KEDA creates and deletes Job objects directly. That's why there's no stabilization window to tune, no cooldown to wait out, and no fallback replica count to fall back *to* — the concepts belong to the HPA, and the HPA isn't in the picture.

> [!tip] Scale-to-zero is free here
> The whole [[08 - Scale-to-Zero & Activation]] mechanism exists because an HPA cannot set `minReplicas: 0`. A `ScaledJob` has no HPA and no baseline workload, so "zero" is simply the absence of Jobs. Activation thresholds still matter for deciding *whether* there's enough work to bother, but the 0↔1 machinery is irrelevant.

---

## 3. A complete worked example

A Kafka-triggered ScaledJob for work that takes several minutes per message.

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledJob
metadata:
  name: transcode-worker
  namespace: media
spec:
  # ---- the Job that gets created, one per unit of work ----------------
  jobTargetRef:
    parallelism: 1                 # one pod per Job. Almost always 1 here
    completions: 1                 # one successful pod ends the Job
    backoffLimit: 4                # Kubernetes-level retries before the Job is failed
    activeDeadlineSeconds: 3600    # hard kill after an hour — a stuck Job can't run forever
    template:
      spec:
        restartPolicy: Never       # MUST be Never or OnFailure. Never Always
        containers:
        - name: transcode
          image: registry.example.com/transcode-worker:2.4.1
          env:
          - name: KAFKA_BOOTSTRAP
            value: kafka-bootstrap.kafka:9092
          - name: KAFKA_TOPIC
            value: video-uploads
          - name: KAFKA_GROUP
            value: transcode-workers
          envFrom:
          - secretRef:
              name: kafka-client-secrets
          resources:               # per-message isolation: this is a real reason to use ScaledJob
            requests:
              cpu: "2"
              memory: 4Gi
            limits:
              cpu: "4"
              memory: 8Gi
        terminationGracePeriodSeconds: 120

  # ---- how KEDA drives it --------------------------------------------
  pollingInterval: 15              # default 30. How often it checks Kafka
  minReplicaCount: 0               # default 0. Nothing warm
  maxReplicaCount: 12              # default 100 (!). = partition count here
  successfulJobsHistoryLimit: 5    # default 100 — lower it, see §5
  failedJobsHistoryLimit: 5        # default 100 — but keep enough to debug
  envSourceContainerName: transcode  # which container KEDA reads env auth from

  rollout:
    strategy: gradual              # let in-flight Jobs finish across an update
    propagationPolicy: background

  scalingStrategy:
    strategy: default

  triggers:
  - type: kafka
    metadata:
      bootstrapServers: kafka-bootstrap.kafka:9092
      consumerGroup: transcode-workers
      topic: video-uploads
      lagThreshold: "1"            # one Job per message
      activationLagThreshold: "0"
    authenticationRef:
      name: kafka-scram-auth       # the TriggerAuthentication from module 12
```

Watch it work:

```bash
kubectl get scaledjob transcode-worker -n media
kubectl get jobs -n media -l scaledjob.keda.sh/name=transcode-worker
kubectl get pods -n media -w
kubectl describe scaledjob transcode-worker -n media   # events + conditions
```

`lagThreshold: "1"` is what makes it one Job per message. The arithmetic is the same shape as a ScaledObject's — a desired count derived by dividing the metric by the threshold and capping at `maxReplicaCount` — but KEDA then subtracts what's already running before creating anything, so Jobs already chewing through messages aren't double-counted.

---

## 4. Pausing

Same annotation as a ScaledObject, and it's the correct way to stop Job creation for maintenance without deleting the object:

```bash
kubectl annotate scaledjob transcode-worker -n media autoscaling.keda.sh/paused=true
kubectl annotate scaledjob transcode-worker -n media autoscaling.keda.sh/paused=false --overwrite
```

Pausing stops *new* Jobs. It doesn't touch Jobs already running.

---

## 5. The fields that matter

**`pollingInterval`** — default **30** seconds. On a ScaledJob this is also your **latency floor**: work sitting in Kafka waits up to one interval before a Job even gets created, plus scheduling and image-pull time on top. For work that takes an hour, 30 s is noise. For work that takes 20 s, it's the dominant cost. Lower it (10–15 s) when latency matters; remember every poll is a round trip to your brokers.

**`minReplicaCount`** — default **0**. Setting it above zero keeps that many Jobs permanently running to avoid start-up cost. Useful, but it partly defeats the point; if you need warm capacity, ask whether you wanted a Deployment.

**`maxReplicaCount`** — default **100**. This is a genuinely dangerous default: a backlog can produce a hundred concurrent Jobs, and with the resource requests in §3 that's 200 CPUs of scheduling pressure. Always set it explicitly.

**`successfulJobsHistoryLimit` / `failedJobsHistoryLimit`** — both default **100**. See the gotcha below.

**`envSourceContainerName`** — defaults to the *first* container in the Job template. It tells KEDA which container's environment to read when a `TriggerAuthentication` uses the `env` source ([[12 - Authentication — TriggerAuthentication, SASL, TLS & MSK IAM]]). Set it explicitly the moment your pod has a sidecar, or KEDA reads the wrong container's env.

**`backoffLimit`** (inside `jobTargetRef`) — default **6**. This is the headline feature: Kubernetes-level retry, per unit of work, with the Job marked failed and kept for inspection when the retries run out. That's a much better story than Kafka redelivery for work you can't afford to silently reprocess forever.

**`activeDeadlineSeconds`** — not required, but set it. Without it a wedged Job runs until something else kills it, holding a slot against `maxReplicaCount` forever.

### `scalingStrategy` — four options, in plain terms

| Strategy | What it does | Choose it when |
|---|---|---|
| `default` | wants *(desired count)* minus *(Jobs currently running)* | **start here.** Correct whenever your queue metric still counts work that in-flight Jobs are holding — which is true of Kafka lag, since lag only falls on commit |
| `custom` | `default`, but you subtract a fixed amount from the queue length and/or discount running Jobs by a percentage | you've measured a systematic over- or under-shoot and need to bias it. Rare, and needs real data |
| `accurate` | assumes the metric **excludes** work already claimed, and counts *pending* Jobs rather than running ones | your source removes/hides an item as soon as it's claimed (a queue with delete-on-consume). **Not the natural fit for Kafka lag** |
| `eager` | fills every free slot up to `maxReplicaCount` while work remains | messages sit in the queue despite free capacity — the classic case where `default` under-provisions because it keeps subtracting long-running Jobs |

The `eager` option is worth understanding because it fixes a real and confusing symptom. With `default`, long-running Jobs are subtracted from the desired count every poll, so a steady trickle of new messages can leave you stuck at the same Job count with obvious spare capacity and a growing backlog. `eager` uses the available slots instead.

> [!warning] Gotcha — a malformed `custom` strategy silently reverts to `default`
> `customScalingRunningJobPercentage` is a **string** holding a float (`"0.5"`). If it can't be parsed, KEDA logs the problem and falls back to `default` behaviour. Your carefully tuned strategy is quietly not in effect, and the ScaledJob reports perfectly healthy — the only evidence is one line in the operator log.
> **Mitigation:** quote it and keep it a plain decimal (`"0.5"`, not `"50%"` or `0.5`). After applying a `custom` strategy, check the operator log once (`kubectl logs -n keda deploy/keda-operator | grep -i scaledjob`) and confirm the Job counts actually move differently from `default`.

### `rollout` — what happens to in-flight Jobs when you update the ScaledJob

| `rollout.strategy` | Behaviour on update |
|---|---|
| unset / `default` | KEDA **deletes** Jobs belonging to the previous version, then recreates from the new spec |
| `gradual` | KEDA **leaves existing Jobs alone**; only new Jobs use the new spec |

`rollout.propagationPolicy` (`background`, the default, or `foreground`) controls the cascade order when that deletion happens.

> [!warning] Gotcha — the default rollout strategy kills work in flight
> Update the image tag on a ScaledJob and, by default, every running Job from the previous version is **deleted mid-execution**. For a 40-minute transcode that's 40 minutes of compute thrown away — and, since the offset was never committed, the message comes back and gets processed again from scratch on the new version. A routine image bump becomes an incident, and it doesn't look like a deploy problem because the Jobs simply vanish.
> **Mitigation:** set **`rollout.strategy: gradual`** on any ScaledJob whose Jobs run longer than a few seconds. It's the behaviour you almost certainly wanted; it just isn't the default. Accept the consequence — two versions run concurrently during a rollout, so keep your message handling forward-compatible. Note also that the top-level `rolloutStrategy` field is **deprecated** in favour of `rollout.strategy`; using the old one still works but won't forever.

---

## 6. When ScaledJob genuinely wins

Four situations. If none applies, use a ScaledObject.

**Per-message work of minutes to hours.** Start-up overhead of a few seconds against 30 minutes of work is a rounding error, and you get complete isolation for free.

**You want Kubernetes-level retry semantics.** `backoffLimit` gives you bounded, observable retries per unit of work, and a failed Job object you can inspect afterwards. Kafka's alternative is redelivery to whichever consumer next owns the partition, with no per-message attempt count and a poison message capable of wedging the partition entirely.

**Per-message resource isolation.** One message needs 8 GB, the next needs 200 MB. A Deployment must be sized for the worst case on every pod. A Job can be sized per invocation, and one OOM kill takes out one unit of work rather than a consumer holding several partitions.

**Work spiky enough that keeping consumers warm is wasteful.** Nothing for six hours, then two hundred items. A pool sized for the spike idles expensively; a pool sized for the average can't clear the spike. Jobs exist only while there's work.

---

## 7. When it's the wrong choice

Be firm about this, because it's where the mistakes happen.

**Pod-per-message overhead is not small.** Every Job pays scheduling, possibly image pull, container start, application start-up, and a Kafka client connecting and joining a group — commonly seconds, and on a cold node with a large image, tens of seconds. Then it exits and the next one pays it again. For sub-second work, a warm consumer holding an open connection and a stable partition assignment is not slightly better, it is **orders of magnitude** better. There is no tuning that closes this gap.

**It multiplies your Job and Pod object count.** A steady 20 messages/second means 20 Job objects and 20 Pods created per second, each with API-server writes, scheduling decisions, and etcd churn — plus 40 more objects per second of completed Jobs waiting to be pruned. Kubernetes is not a function runtime, and treating it as one at that rate becomes a control-plane problem, not an application one.

And then the one nobody expects:

> [!warning] Gotcha — a ScaledJob does NOT reduce Kafka rebalancing. It usually **increases** it
> This is the least obvious and most important trade-off in the module, and it's the reason people who adopt ScaledJob "to stop the rebalance churn" from [[02 - Kafka Consumer Groups, Partitions & Rebalancing]] end up worse off.
>
> Rebalances happen when consumer group membership changes. A ScaledObject's consumers are long-lived: membership changes only when you scale or a pod dies. A ScaledJob's consumers are **maximally short-lived** — every single Job joins the group when it starts and leaves when it exits. With a shared `group.id`, processing a thousand messages means roughly **two thousand membership changes**, each pausing consumption for the whole group. You didn't remove rebalancing; you made it the dominant activity in the group.
>
> The usual workaround is to stop using group management: assign partitions manually in the Job, or give every Job a distinct `group.id`. Both work, and both cost you things you may not have priced in:
> - **Manual assignment** means *you* now own partition distribution. Nothing stops two Jobs from reading the same partition, and nothing guarantees every partition is covered.
> - **A distinct group per Job** means offsets aren't shared, so each Job needs to be told exactly what to read — and you accumulate consumer groups that Kafka eventually expires.
> - **Either way, KEDA's partition ceiling stops protecting you.** `allowIdleConsumers: false` caps useful parallelism at the partition count *because Kafka gives each partition one owner in a group* ([[07 - The Kafka Scaler — Every Field & the Replica Math]], §4). Leave group management and that assumption is void: nothing prevents Job number thirteen on a twelve-partition topic, and nothing makes it useful either.
>
> **Mitigation:** decide the consumption model *before* choosing ScaledJob, not after. The shape that actually works is a **dispatcher pattern** — keep one small, warm ScaledObject consumer group whose only job is to read messages and create work items (a Kubernetes Job, or a row in a work table), then trigger the ScaledJob from *that* queue rather than from Kafka directly. The group stays stable, rebalances stay rare, and the Jobs never touch Kafka at all. If you do trigger from Kafka directly, use manual assignment with an explicit partition-to-Job mapping you can reason about, and drop `maxReplicaCount` to a number you've chosen deliberately, because KEDA is no longer choosing it for you.

---

## 8. `ScaledObject` vs `ScaledJob` — the decision table

| Dimension | `ScaledObject` — choose it when | `ScaledJob` — choose it when |
|---|---|---|
| Per-message duration | milliseconds to seconds | **minutes to hours** |
| Throughput | high and sustained | low count, high cost each |
| Start-up cost | matters — amortise it over many messages | irrelevant next to the work |
| Retry semantics | Kafka redelivery + your own DLQ is enough | you want `backoffLimit` per unit of work, and a failed Job to inspect |
| Resource profile | uniform across messages | **varies per message** |
| Kafka group membership | **stable** — the partition ceiling protects you | churns on every Job; you'll leave group management behind |
| Rebalancing | one per scale event, manageable | **two per Job** unless you leave group management |
| Scaling smoothness | full HPA `behavior`: stabilization windows, rate policies | `scalingStrategy` only — coarser |
| Failure fallback | `spec.fallback` gives a defined replica count | **none. No fallback exists** |
| Cost at idle | zero replicas, but the machinery stays warm | zero Jobs — genuinely nothing |
| Objects created | one Deployment, one HPA | one Job + one Pod **per unit of work** |
| Observability | replica count, HPA events, one metric | Job success/failure per item, exit codes, per-item logs |
| Traffic shape | steady or gently varying | **very spiky** |

> [!tip] Committed recommendation
> **Default to `ScaledObject`.** Move to `ScaledJob` when per-message work is measured in minutes and you can articulate which of §6's four reasons applies. Then design the consumption model deliberately — the dispatcher pattern in §7's mitigation, not "point a Job at the same consumer group and hope."

---

## 9. The rest of the gotchas

> [!warning] History limits default to 100 each and quietly fill your namespace
> `successfulJobsHistoryLimit` and `failedJobsHistoryLimit` are both **100**, so a busy ScaledJob keeps up to 200 completed Job objects — plus their Pods — sitting around. Multiply by a few ScaledJobs and you have thousands of dead objects: `kubectl get pods` becomes unusable, resource quotas on object counts start rejecting new Jobs, and etcd carries load for nothing. Pruning also only happens on the polling cycle, so the real count can briefly exceed the limit.
> **Mitigation:** set both explicitly. **`successfulJobsHistoryLimit: 3–5`** — you rarely inspect a success. Keep `failedJobsHistoryLimit` higher (10–20) because that's your debugging evidence, and alert on failed Jobs instead of relying on the history to notice.

> [!warning] `restartPolicy: Always` is the default your tooling gives you, and it's invalid for a Job
> Pod templates default to `restartPolicy: Always`, which Kubernetes forbids on a Job — a Job must be able to complete. KEDA is defensive here and forces `OnFailure` when the field is unset, but if you copy a pod spec out of a Deployment with `Always` written in it explicitly, the Job is rejected.
> **Mitigation:** always write it. **`Never`** when you want KEDA/Kubernetes to create a fresh pod per attempt (clearer failures, one pod per attempt to inspect); **`OnFailure`** when restarting the container in place is cheaper than rescheduling. `Never` is the better default for long work — a fresh pod gets a fresh scheduling decision and possibly a healthier node.

> [!warning] A Job that consumes more than one message is a mini-consumer with none of the benefits
> The single most common ScaledJob anti-pattern: the container runs a normal consumer loop, polling until the topic is empty or a timeout fires. Now you have a consumer pool that pays full pod start-up cost, churns group membership constantly, has no HPA smoothing, no `fallback`, and unbounded per-Job work — every disadvantage of both models. It usually happens because the same consumer image was reused unchanged from the Deployment.
> **Mitigation:** a ScaledJob container must **read one unit of work, process it, and exit non-zero on failure**. If it loops, you wanted a ScaledObject. Enforce it with `activeDeadlineSeconds` sized to one unit of work — a Job that legitimately handles one message can't hit a deadline set for one message.

> [!warning] `fallback` doesn't exist on a ScaledJob, and neither does `advanced`
> Applying a ScaledJob with a `fallback` block gets it rejected or ignored, and people conclude fallback is broken. It was never there: `fallback` sets a replica count on an HPA, and there's no HPA. Same for `cooldownPeriod`, `idleReplicaCount` and the whole `advanced` block. The consequence is real — when your Kafka brokers are unreachable, a ScaledObject with `fallback` holds a defined replica count, while a ScaledJob simply **creates no Jobs**. A broker outage means no work is processed at all, silently, because "no lag reported" and "can't reach Kafka" produce the same outcome.
> **Mitigation:** don't look for the field; build the safety net outside. Alert on scaler errors and on "zero Jobs created while lag exists" ([[14 - Observability & Debugging — Why Isn't It Scaling]]). If a defined-behaviour-under-failure guarantee is a requirement, that's an argument for the ScaledObject shape, or for the dispatcher pattern where the ScaledJob's trigger is a queue you control.

> [!warning] The partition ceiling stops protecting you the moment you use manual assignment
> `allowIdleConsumers: false` and a `maxReplicaCount` pinned to the partition count are both premised on group-managed assignment. Move to manual assignment or per-Job consumer groups (the §7 workaround) and those guards become decorative — KEDA will still cap the number it reports, but nothing connects that cap to useful parallelism any more. You can end up with Jobs that read nothing, or several Jobs reading the same partition and duplicating work.
> **Mitigation:** if you leave group management, own the arithmetic explicitly. Choose `maxReplicaCount` from your real concurrency limit (downstream capacity, licence seats, database connections), not from the partition count, and make the partition-to-Job mapping something a person can read. And make processing idempotent — with manual assignment, at-least-once is now your responsibility rather than Kafka's.

---

## 10. The alternative shapes

`ScaledJob` isn't only competing with `ScaledObject`. Broader autoscaling alternatives are in [[16 - Alternatives & Trade-offs]].

| Shape | What it is | Choose it when |
|---|---|---|
| **KEDA `ScaledObject`** | autoscaled pool of warm consumers | per-message work is short, throughput is sustained, and you want stable group membership. **The default** |
| **KEDA `ScaledJob`** | one Job per unit of work | work is minutes-to-hours, needs isolation or `backoffLimit` retry, and traffic is spiky |
| **Kubernetes `CronJob`** | Jobs on a schedule | work is **time**-driven, not event-driven — nightly aggregation, hourly reconciliation. No trigger needed, no KEDA needed |
| **Plain queue-worker Deployment** | fixed-size consumer pool, no autoscaling | load is predictable, or the cost of getting autoscaling wrong exceeds the cost of over-provisioning. An honest choice, not a lazy one |
| **Dispatcher + `ScaledJob`** | small warm ScaledObject consumer creates work items; ScaledJob scales on those | you want per-item Jobs **without** wrecking Kafka group membership. **The pattern that actually works at scale** |

A note on `CronJob`, because the confusion is common: a `CronJob` fires on the clock whether or not there's work; a `ScaledJob` fires on work whether or not it's a convenient time. If your batch has a schedule, use `CronJob` — it's simpler, it's built in, and it has its own history limits and concurrency policy. If your batch has a backlog, use `ScaledJob`.

---

## 11. Questions you should be able to answer

- What's the structural difference between a ScaledObject and a ScaledJob? *(A ScaledObject scales an existing workload through an HPA it owns; a ScaledJob creates Jobs directly and there is no HPA.)*
- Why is there no `cooldownPeriod` or `fallback` on a ScaledJob? *(Both are HPA concepts, and no HPA exists.)*
- KEDA's own criterion for choosing ScaledJob? *(Long-running executions — minutes to hours per unit of work.)*
- Does a ScaledJob reduce Kafka rebalancing? *(**No — it increases it.** Every Job joins and leaves the group, so you get roughly two membership changes per message.)*
- What do you give up by working around that with manual assignment? *(Group-managed distribution — and with it the partition ceiling that `allowIdleConsumers: false` provides.)*
- What's the shape that gets per-item Jobs without the rebalance cost? *(A dispatcher: one warm ScaledObject consumer creates work items; the ScaledJob triggers on those.)*
- Your namespace has 3,000 completed Pods. Why? *(Both history limits default to 100, per ScaledJob.)*
- You bumped the image and 40-minute Jobs disappeared mid-run. Why? *(The default rollout strategy deletes previous-version Jobs. Use `rollout.strategy: gradual`.)*
- Which `scalingStrategy` fixes "messages waiting while slots are free"? *(`eager`.)*
- Why is `accurate` usually wrong for Kafka? *(It assumes the metric excludes claimed work; Kafka lag doesn't fall until offsets are committed.)*
- Your brokers are unreachable. What does a ScaledJob do? *(Creates no Jobs — silently, with no fallback. Alert on scaler errors.)*
- Your Job container loops until the topic is empty. What's wrong? *(It's a consumer pool paying pod-per-message cost with none of a Deployment's benefits. One unit of work per Job, then exit.)*

---

## 12. Sources

- [KEDA — Scaling Jobs (concepts, incl. the long-running-executions criterion)](https://keda.sh/docs/2.20/concepts/scaling-jobs/)
- [KEDA — ScaledJob specification (every field, defaults, `scalingStrategy` formulas)](https://keda.sh/docs/2.20/reference/scaledjob-spec/)
- [KEDA — ScaledObject specification (for the comparison)](https://keda.sh/docs/2.20/reference/scaledobject-spec/)
- [KEDA — Apache Kafka scaler](https://keda.sh/docs/2.20/scalers/apache-kafka/)
- [KEDA issue #5114 — the `eager` scaling strategy and the problem it solves](https://github.com/kedacore/keda/issues/5114)
- [KEDA PR #1227 — the `accurate` scaling strategy](https://github.com/kedacore/keda/pull/1227)
- [Kubernetes — Jobs (`backoffLimit`, `restartPolicy`, `activeDeadlineSeconds`)](https://kubernetes.io/docs/concepts/workloads/controllers/job/)
- [Kubernetes — Job v1 API reference (`JobSpec`, which `jobTargetRef` is)](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/job-v1/)
- [Kubernetes — CronJob](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/)
- [Kubernetes — foreground vs background cascading deletion (`propagationPolicy`)](https://kubernetes.io/docs/tasks/administer-cluster/use-cascading-deletion/)
- [Apache Kafka 4.3 — consumer configuration (`group.id`, assignment)](https://kafka.apache.org/43/configuration/consumer-configs/)
