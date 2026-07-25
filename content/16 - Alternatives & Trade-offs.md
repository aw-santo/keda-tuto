---
title: Alternatives & Trade-offs
tags:
  - keda
  - kubernetes
  - autoscaling
  - kafka
  - senior
aliases:
  - KEDA Alternatives
  - KEDA vs Knative
  - KEDA vs Prometheus Adapter
  - When not to use KEDA
status: ready
created: 2026-07-25
---

# Alternatives & Trade-offs

> [!abstract] Scope
> The judgment module: **when KEDA is not the answer.** Everything else in this track assumes you've decided to use it. Here you decide. Two questions pick your tool — *what signal drives scaling* and *does it need to reach zero* — and the rest of this module is the comparison table those questions index into, plus a frank list of situations where reaching for KEDA is the wrong call. The one thing to take away: **KEDA competes with the Prometheus Adapter and with Knative, but it is complementary to VPA and to node autoscaling.** Confusing "competing" with "complementary" is where most of the bad architecture decisions in this space come from. Builds on [[01 - Why Event-Driven Autoscaling — The Limits of HPA]].

Related: [[00 - KEDA Index]], [[01 - Why Event-Driven Autoscaling — The Limits of HPA]], [[03 - KEDA Architecture — Operator, Metrics Adapter & the HPA It Owns]], [[07 - The Kafka Scaler — Every Field & the Replica Math]], [[13 - ScaledJob — When a Pod per Message Beats a Consumer Pool]], [[15 - Production Operations]], [[17 - LAB 4 — Production-Grade Kafka Autoscaling Capstone]]

> [!warning] Written against KEDA v2.20 · Kubernetes 1.33–1.35 · Apache Kafka 4.3
> The comparison below is about *architecture*, which moves slowly — but two facts in it are worth re-checking for your own cluster: whether anything else is already registered as the cluster's external-metrics provider (§3), and whether your Kubernetes version supports in-place pod resizing, which changes how disruptive VPA is (§4).

---

## 1. Two questions pick the tool

Before comparing anything, answer these in order. They eliminate most of the table.

```text
Q1. WHAT SIGNAL should drive the replica count?

    CPU or memory of the pod itself ─────────────► plain HPA. Stop here.
    A number already in Prometheus ──────────────► go to Q2
    Depth of a queue / topic / stream ───────────► go to Q2
    Inbound HTTP requests or concurrency ────────► go to Q2
    Nothing — load is predictable by the clock ──► scheduled scaling (KEDA's cron
                                                   trigger, or a CronJob that
                                                   patches replicas)

Q2. Does the workload need to sit at ZERO replicas when idle?

    No,  and the signal is in Prometheus ────────► HPA + Prometheus Adapter
    No,  and the signal is a queue ──────────────► KEDA (still the least work)
    Yes, and the signal is HTTP requests ────────► Knative Serving
    Yes, and the signal is a queue/event source ─► KEDA          ◄── this track
```

Two notes on that tree, because they're where people go wrong:

**"CPU-bound" is a claim you should be able to defend.** If the pod's CPU rises and falls with the thing you actually care about, plain HPA is genuinely the right answer and adding KEDA is pure operational cost. That's rarer than people think for anything queue-driven ([[01 - Why Event-Driven Autoscaling — The Limits of HPA]]), but it's completely normal for an encoding service, an image resizer or a CPU-heavy API.

**Scale-to-zero is the strongest discriminator in the whole table.** A stock HPA cannot go below one replica outside an alpha feature gate, and a *client-published* metric — like a Kafka consumer's own `records-lag-max` — doesn't exist while there are no clients to publish it. Any design that needs zero needs something that measures from outside the workload. KEDA and Knative both qualify; a Prometheus Adapter scraping your pods does not.

---

## 2. The comparison table

This is the centrepiece. Read the "choose it when" column first, then check the "reaches zero" column, then argue.

| Tool | Scales what | Signal it uses | Reaches zero | Choose it when |
|---|---|---|:---:|---|
| **Plain HPA** (CPU/memory) | pod count | resource metrics from metrics-server | ✗ | the workload really is CPU- or memory-bound and demand is smooth. Zero extra components, built into every cluster, universally understood |
| **HPA + Prometheus Adapter** | pod count | any PromQL expression, served as a custom or external metric | ✗ | everything you'd scale on is already in Prometheus, you don't need zero, and you'd rather operate one adapter you already understand than a new controller |
| **KEDA** | pod count (and Jobs, via ScaledJob) | 60+ event sources queried **from outside the pod** — brokers, queues, cloud APIs, Prometheus, cron | ✓ | the signal is a backlog or an event source; or you need zero; or you'd otherwise be building a bespoke metrics pipeline. **The default answer for a Kafka consumer** |
| **VPA** | the CPU/memory **requests of each pod** | historical + live usage of the workload | ✗ (it's not a pod-count tool) | one copy is the wrong *size*. Complementary to horizontal scaling, with one important restriction — §4 |
| **Knative Serving** | pod count, per revision | in-flight request concurrency or requests-per-second, measured at its own proxy | ✓ | the workload is HTTP request/response and you want a serverless platform: routing, revisions, traffic splitting and buffered cold starts, not just an autoscaler |
| **Cluster Autoscaler / Karpenter** | **node count** | Pending pods that don't fit | ✗ (nodes, not pods) | always, in any cluster that autoscales pods. **Complementary, never an alternative** — §6 |

> [!tip] The one-liner worth remembering
> **KEDA answers "how many pods should exist?" using a signal from outside the pod.** Anything that answers a different question — how big should each pod be (VPA), how many nodes should exist (Cluster Autoscaler/Karpenter), how should partitions be spread across brokers (Cruise Control) — is not an alternative to KEDA no matter how often it appears in the same sentence.

---

## 3. HPA + Prometheus Adapter — and the constraint that decides it

The [Prometheus Adapter](https://github.com/kubernetes-sigs/prometheus-adapter) turns PromQL queries into metrics the HPA can consume, on `custom.metrics.k8s.io` (metrics attached to a Kubernetes object) and on `external.metrics.k8s.io` (metrics attached to nothing in particular — a queue depth, for instance). If your Kafka lag is already in Prometheus via an exporter, you can absolutely autoscale on it this way, with no KEDA at all.

And then you hit the constraint that makes this an either/or decision rather than a preference. From KEDA's FAQ, verbatim:

> "Kubernetes currently only supports one metric server serving `external.metrics.k8s.io` metrics per cluster."

That API group is registered cluster-wide as a single `APIService` object. Whoever installs second either fails or silently takes over the registration — and every HPA depending on the loser stops getting metrics.

**The precise shape of the conflict**, because the sloppy version of this fact causes its own problems:

| Combination | Works? |
|---|---|
| KEDA + prometheus-adapter serving **only** `custom.metrics.k8s.io` | ✓ different API group, no clash |
| KEDA + prometheus-adapter serving `external.metrics.k8s.io` | ✗ one provider per cluster; they fight over the same `APIService` |
| KEDA + metrics-server (CPU/memory) | ✓ different API group entirely (`metrics.k8s.io`), and KEDA's own CPU/memory triggers deliberately read from metrics-server rather than proxying |

So the honest comparison is: the adapter reuses monitoring you already run and adds no new controller, but it caps you at metrics that reach Prometheus, it can't reach zero, and *choosing it is choosing it for the whole cluster*. KEDA brings sixty-plus maintained triggers, broker-side measurement and scale-to-zero, and writes the HPA for you — at the cost of a controller to operate and version-pin ([[15 - Production Operations]]).

> [!warning] Gotcha — installing KEDA into a cluster that already runs prometheus-adapter breaks external metrics
> Both want to own `v1beta1.external.metrics.k8s.io`. Depending on install order you get an `APIService` that never becomes Available, or one that becomes Available pointing at the *other* component — and HPAs on both sides start reporting `<unknown>` for their targets. Nothing in either product's happy-path install warns you.
> **Mitigation:** before installing KEDA, run `kubectl get apiservice v1beta1.external.metrics.k8s.io`. If it exists and isn't KEDA's, you have a decision to make, not an install to run. If you need both tools, keep prometheus-adapter on `custom.metrics.k8s.io` only, and move anything external onto KEDA's Prometheus **trigger** — KEDA can query Prometheus itself, which usually makes the adapter redundant rather than a peer.

---

## 4. VPA — the other axis, not a competitor

The Vertical Pod Autoscaler changes the CPU and memory *requests* of your pods. It answers "is each copy the right size?", where the HPA answers "do I need more copies?". For a Kafka consumer that's a real question: right-sized requests are what let the scheduler pack twelve consumer pods onto the nodes you have.

Two practical points:

**Applying a new size has historically meant replacing the pod.** VPA's updater evicts a pod so it comes back with the recommended requests. On a consumer group, that eviction is a rebalance — exactly the cost [[02 - Kafka Consumer Groups, Partitions & Rebalancing]] warns about. Kubernetes has been graduating **in-place pod resizing**, which changes the disruption profile considerably; check whether your cluster version and your VPA version support it before assuming either behaviour.

**A recommendation-only mode exists and is underused.** Running VPA in `Off` mode gives you sizing recommendations without it touching anything — which is often exactly what you want: read the numbers, set the requests yourself, keep the churn at zero.

> [!warning] Gotcha — VPA and HPA on the same metric fight each other
> The VPA project's own guidance is not to use VPA together with an HPA **on CPU or memory**. The loop is easy to see: VPA raises the CPU request, so measured utilisation as a *percentage of request* falls, so the HPA scales in, so per-pod load rises, so VPA raises the request again. The two controllers are reading and writing opposite ends of the same ratio.
> **Mitigation:** don't let them share a signal. A KEDA-driven Kafka consumer is the good case — KEDA scales the pod count on an **external** metric (consumer lag), which VPA doesn't touch, so VPA on CPU/memory and KEDA on lag coexist cleanly. Just make sure you haven't also added a CPU trigger to that same `ScaledObject`, because that puts you straight back into the conflict.

---

## 5. Knative Serving — the better answer for request-driven HTTP

Knative Serving is a serverless platform, not just an autoscaler. Its own autoscaler (the KPA) scales on **in-flight request concurrency** or requests-per-second measured at Knative's proxy, and it scales to zero. Crucially, when a revision is at zero, Knative's **activator** holds the incoming request open while a pod starts, then forwards it — so a cold start costs latency rather than a dropped request.

KEDA has an **HTTP Add-on** (a separate component from core KEDA, with an interceptor that plays a similar buffering role and an external scaler that reports pending requests). It exists and it works, but it is a smaller, less mature piece of software than Knative Serving, and it does not bring revisions or traffic splitting.

| | Knative Serving | KEDA (+ HTTP Add-on) |
|---|---|---|
| Best at | HTTP request/response workloads | queue, topic, stream and cloud-event workloads |
| Signal | concurrency / RPS at its own proxy | 60+ external sources; pending requests via the add-on |
| Zero → one | activator buffers the request that caused it | interceptor buffers, for HTTP; for a queue there's nothing to buffer — the work is already durable |
| Also gives you | routing, revisions, traffic splitting, a request path you must adopt | nothing but autoscaling; your Service and Ingress stay as they are |
| Cost of adoption | your workload becomes a Knative `Service` | a `ScaledObject` next to your existing Deployment |

**The honest verdict:** for request-driven HTTP that must idle at zero, Knative is the more complete answer, and it's worth adopting the platform to get it. For queue- and event-driven work, KEDA is the right tool and Knative has nothing to say about your Kafka topic. The two coexist in one cluster without fighting — Knative's autoscaler doesn't go through `external.metrics.k8s.io`.

---

## 6. Cluster Autoscaler / Karpenter — complementary, and required

This is the one row of the table where treating it as an alternative produces a genuine outage.

KEDA (through its HPA) changes a Deployment's `spec.replicas`. That is all it does. If the cluster has no room for the new pods, they are created and they sit in `Pending` — forever, as far as KEDA is concerned. Nothing in KEDA provisions capacity.

Node autoscaling is the other half:

- **Cluster Autoscaler** grows and shrinks pre-defined node groups when pods can't be scheduled.
- **Karpenter** provisions nodes directly from pending-pod requirements, without node groups, and consolidates them again when they empty out.

Either one turns "12 Pending consumer pods" into "12 running consumer pods, on nodes that appeared". Both are covered from the KEDA side in [[15 - Production Operations]].

> [!warning] Gotcha — expecting KEDA to give you capacity
> A burst arrives, KEDA does exactly the right thing, the HPA asks for 12 replicas, and eight of them stay `Pending` because the cluster is full. Lag keeps climbing. Every dashboard says the autoscaler is working — `kubectl get hpa` shows a healthy 12 desired — and the actual constraint is invisible unless you look at pod status.
> **Mitigation:** run a node autoscaler in any cluster where you autoscale pods, and make Pending pods a first-class alert (`kube_pod_status_phase{phase="Pending"}` sustained for a few minutes). Set resource **requests** on the consumer so the scheduler and the node autoscaler can reason about it at all — a pod with no requests tells them nothing. And remember that the time-to-capacity of your node autoscaler is part of your drain-time budget: if a node takes 90 seconds to appear, your 10-minute SLO has 90 fewer seconds in it ([[17 - LAB 4 — Production-Grade Kafka Autoscaling Capstone]]).

---

## 7. Kafka-adjacent tools that are not consumer autoscalers

This section exists because these get proposed in design reviews as alternatives to KEDA, and they aren't alternatives to anything KEDA does.

**Cruise Control** (originally from LinkedIn, and integrated into Strimzi) is broker-side capacity management. It watches broker load and **rebalances partition replicas across brokers** — moving partitions off a hot broker, filling a newly added one, draining one you're about to remove, and self-healing after a broker failure. It is genuinely excellent at that. It has nothing to do with how many consumers are reading your topic.

**Strimzi** is a Kubernetes operator for running Kafka itself: brokers, KRaft controllers, topics, users, listeners, certificates, upgrades. It manages the cluster your consumers connect to. It does not autoscale your consumer Deployment. (Strimzi and KEDA are a common and sensible pairing — Strimzi runs the broker, KEDA scales the consumer.)

> [!warning] Gotcha — reaching for Cruise Control when consumer lag is the problem
> The symptoms overlap enough to be genuinely confusing: lag is high, one broker looks busy, and "rebalance" is the word in both stories. But Cruise Control's rebalance moves **partitions between brokers**; the rebalance that matters to your lag moves **partitions between consumers**. If your consumers can't keep up, spreading partitions differently across brokers changes nothing about how fast they are drained.
> **Mitigation:** separate the two diagnoses before choosing a tool. Is broker CPU, network or disk saturated, or is one broker leading far more partitions than its peers? That's a Cruise Control problem. Is the broker comfortable while the group's committed offsets fall behind the log end? That's a consumer-capacity problem — KEDA, more partitions, or a faster consumer. The `kubectl exec … kafka-consumer-groups.sh --describe` output in [[14 - Observability & Debugging — Why Isn't It Scaling]] tells you which one you have in about ten seconds.

---

## 8. Consumer-side alternatives to adding pods

Adding pods is not the only way to add throughput, and it's not always the cheapest. Each of these is sometimes the correct engineering answer.

| Alternative | How it works | Choose it when |
|---|---|---|
| **Internal concurrency** | one pod, several consumer instances or a worker pool behind one consumer; throughput rises with no new group members | per-message work is I/O-bound (so threads are nearly free), or the partition count is small and you've hit the ceiling that horizontal scaling can't cross |
| **Scheduled / manual scaling** | fix the replica count by clock, not by signal — KEDA's own **`cron`** trigger, a CronJob that patches `spec.replicas`, or the `autoscaling.keda.sh/paused-replicas` annotation to pin a count | load is genuinely predictable (a nightly batch, a 9-to-5 business workload) and reacting to a signal buys nothing |
| **Over-provisioning** | run a fixed, comfortable replica count and stop thinking about it | the workload is small and cheap, or latency-critical enough that you never want a cold start. Four pods at 50 mCPU each, always on, may cost less than the engineering time to autoscale them |

> [!tip] Best practice — scaling internal concurrency is the fix for the partition ceiling
> A 4-partition topic can never usefully run more than 4 group members. If four pods aren't enough, more pods is not the lever — either raise the partition count (a producer-side decision with its own ordering consequences) or make each pod process more at once. This is also the one legitimate reason to set `allowIdleConsumers: true`: if each pod runs several consumer instances internally, the partition count no longer describes your real parallelism ([[07 - The Kafka Scaler — Every Field & the Replica Math]]).

> [!important] "Just over-provision" is a senior answer, not a lazy one
> Autoscaling has a running cost: a controller to upgrade and version-pin, a rebalance on every scale event, cold-start latency at the edges, and one more component in every incident's suspect list. For a consumer that peaks at three pods, that cost can easily exceed the compute you'd save. Say so in the design review — being able to argue *against* the sophisticated option is most of what "judgment" means here.

---

## 9. When KEDA is the wrong choice

Five situations where the answer is no. Recognising them is worth more in an interview than reciting the field list.

**1. The workload is genuinely CPU- or memory-bound.** A plain HPA on CPU is built in, needs no new controller, and every engineer on the team already understands it. Adding KEDA to scale on CPU means running an operator, a metrics adapter and an admission webhook so that KEDA can create an HPA that reads CPU from metrics-server — which is what you'd have had for free.

**2. Horizontal scaling can't help, because of the partition count.** A 2-partition topic supports 2 useful consumers. If two aren't enough, KEDA's whole contribution is to move you between one and two pods. The real fix is more partitions or more concurrency inside the pod — see §8.

**3. Latency-critical paths where a cold start is unacceptable.** Scale-to-zero means the first message after an idle period waits for a pod to schedule, pull an image, start a JVM or interpreter, join the group and complete a rebalance. That's seconds at best. If your SLO can't absorb it, set `minReplicaCount: 1` and keep a warm consumer — or don't autoscale this one at all.

**4. Very small deployments.** One team, three services, a topic that does 50 messages a minute. The operational cost of another cluster-scoped controller — with breaking changes in minor releases and a hard dependency on the aggregation layer — is real and the savings are not.

**5. The cluster is already committed to a different external-metrics provider.** §3. If prometheus-adapter is serving `external.metrics.k8s.io` for twenty other HPAs, installing KEDA is a migration project, not an addition.

And one honourable mention that isn't "don't use KEDA" but "don't use a `ScaledObject`": if each message takes minutes to hours, a pod-per-message **ScaledJob** fits the shape of the work far better than a long-lived consumer pool that gets killed mid-batch on every scale-in ([[13 - ScaledJob — When a Pod per Message Beats a Consumer Pool]]).

---

## 10. Where KEDA fits with the others

The tools stack rather than compete. Read this bottom-up: each layer is meaningless without the one below it.

```text
  ┌──────────────────────────────────────────────────────────────┐
  │ APPLICATION CONCURRENCY   threads / consumer instances /     │
  │                           batch size inside ONE pod          │  ← you own this
  │                           (the only lever above the          │
  │                            partition ceiling)                │
  ├──────────────────────────────────────────────────────────────┤
  │ POD AUTOSCALING           how many pods?                     │
  │   KEDA ──► owns 0↔1, feeds and owns an HPA                   │  ← this track
  │   HPA  ──► owns 1↔N arithmetic                               │
  │   VPA  ──► how BIG each pod is (different axis, same layer)  │
  ├──────────────────────────────────────────────────────────────┤
  │ NODE AUTOSCALING          is there anywhere to put the pods?  │
  │   Cluster Autoscaler / Karpenter                             │  ← without this,
  │                                                              │    pods go Pending
  └──────────────────────────────────────────────────────────────┘

  Alongside, not in the stack:
    Strimzi / Cruise Control ──► the Kafka cluster itself (brokers, partitions)
    Knative Serving          ──► replaces the pod-autoscaling layer for HTTP
    Prometheus Adapter       ──► replaces KEDA at the pod-autoscaling layer,
                                 and CANNOT share the cluster with it
```

A complete production picture for a Kafka consumer is usually: **Karpenter or Cluster Autoscaler** underneath, **KEDA** driving the replica count from lag, **resource requests** set from real measurements (with VPA in recommendation mode if you want help finding them), and a **tuned consumer** whose concurrency and batch size you chose deliberately. Not one tool — four decisions.

---

## 11. The rules worth internalising

- Answer "what signal?" and "does it need zero?" **before** naming a tool. Those two answers eliminate most of the table.
- Plain HPA on CPU is the right answer more often than KEDA enthusiasts admit — and it's free.
- **One external-metrics provider per cluster.** KEDA and prometheus-adapter-on-external-metrics are mutually exclusive; check `kubectl get apiservice v1beta1.external.metrics.k8s.io` before you install anything.
- KEDA does not provision nodes. Without a node autoscaler you've built a machine for producing `Pending` pods.
- VPA and horizontal autoscaling are complementary **as long as they don't share a signal**. Lag-driven KEDA plus CPU-driven VPA is fine; CPU-driven HPA plus CPU-driven VPA is a feedback loop.
- For request-driven HTTP with scale-to-zero, Knative Serving is the more complete answer. For queues and event streams, KEDA is.
- Cruise Control rebalances **partitions across brokers**. It will never help consumer lag. Strimzi runs Kafka; it doesn't scale your consumer.
- Above the partition ceiling, the only lever left is concurrency inside the pod.
- Predictable load wants a schedule, not a signal — and KEDA's `cron` trigger means you don't have to leave KEDA to get one.
- Sometimes the right answer is four always-on pods and a note in the README explaining why.

---

## 12. Questions you should be able to answer

- Which two questions pick your autoscaling tool? *(What signal drives scaling, and does it need to reach zero?)*
- Why can't you run KEDA and the Prometheus Adapter together for external metrics? *(Kubernetes supports one metric server for `external.metrics.k8s.io` per cluster; they'd fight over the same `APIService`. A prometheus-adapter serving only `custom.metrics.k8s.io` is fine.)*
- Your HPA says 12 desired replicas and 8 pods are Pending. Which autoscaler do you fix? *(None — it's node capacity. Cluster Autoscaler or Karpenter. KEDA never provisions nodes.)*
- Is VPA an alternative to KEDA? *(No. VPA changes pod size, KEDA changes pod count. Complementary — but don't let VPA and an autoscaler share CPU as a signal.)*
- KEDA's HTTP add-on or Knative Serving for a request-driven service that must idle at zero? *(Knative — it's the more complete answer for HTTP, with routing, revisions and an activator that buffers the cold-start request. Use KEDA for queue-driven work.)*
- Someone proposes Cruise Control to fix consumer lag. Your response? *(Cruise Control rebalances partitions across brokers — broker capacity management. It doesn't change consumer parallelism. Check whether the brokers or the consumers are the bottleneck first.)*
- Name three situations where you would *not* use KEDA. *(Genuinely CPU-bound workloads; a partition count so low that horizontal scaling is pointless; latency-critical paths where cold starts are unacceptable; tiny deployments; a cluster already committed to another external-metrics provider.)*
- Your topic has 2 partitions and 2 pods can't keep up. What do you do? *(Not more pods — more partitions, or more concurrency inside each pod. The partition count is a hard ceiling on useful group members.)*
- When is "just over-provision" the correct engineering answer? *(Small, cheap workloads, or latency-critical ones — when the operational cost of a controller and the rebalance cost of scaling exceed the compute you'd save.)*

---

## 13. Sources

- [KEDA — FAQ (one external metrics server per cluster; don't combine a ScaledObject with an HPA)](https://keda.sh/docs/2.20/reference/faq/)
- [KEDA — the scaler catalogue](https://keda.sh/docs/2.20/scalers/)
- [KEDA — Cron scaler](https://keda.sh/docs/2.20/scalers/cron/)
- [KEDA — CPU scaler (reads from metrics-server, not through KEDA's adapter)](https://keda.sh/docs/2.20/scalers/cpu/)
- [KEDA — Scaling Deployments: activation vs scaling](https://keda.sh/docs/2.20/concepts/scaling-deployments/)
- [KEDA — HTTP Add-on](https://github.com/kedacore/http-add-on)
- [Kubernetes — Autoscaling Workloads (HPA, VPA, node autoscaling)](https://kubernetes.io/docs/concepts/workloads/autoscaling/)
- [Kubernetes — HorizontalPodAutoscaler walkthrough and support for metrics APIs](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/)
- [Kubernetes — HorizontalPodAutoscaler v2 API (`minReplicas`, `HPAScaleToZero`)](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/horizontal-pod-autoscaler-v2/)
- [Kubernetes — Resize CPU and Memory Resources assigned to Containers (in-place pod resize)](https://kubernetes.io/docs/tasks/configure-pod-container/resize-container-resources/)
- [Vertical Pod Autoscaler (including the guidance not to combine it with an HPA on CPU/memory)](https://github.com/kubernetes/autoscaler/tree/master/vertical-pod-autoscaler)
- [Prometheus Adapter — custom and external metrics from PromQL](https://github.com/kubernetes-sigs/prometheus-adapter)
- [Kubernetes — Cluster Autoscaler](https://github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler)
- [Karpenter documentation](https://karpenter.sh/docs/)
- [Knative Serving — autoscaling](https://knative.dev/docs/serving/autoscaling/)
- [Knative Serving — scale to zero](https://knative.dev/docs/serving/autoscaling/scale-to-zero/)
- [LinkedIn Cruise Control for Apache Kafka](https://github.com/linkedin/cruise-control)
- [Strimzi — overview (what the operator manages)](https://strimzi.io/docs/operators/latest/overview.html)
- [Apache Kafka 4.3 — `KafkaConsumer` (each partition is assigned to exactly one consumer in the group)](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
- [Apache Kafka 4.3 — monitoring (`records-lag-max` is published by the consumer, not the broker)](https://kafka.apache.org/43/operations/monitoring/)
