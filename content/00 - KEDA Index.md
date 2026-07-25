---
title: KEDA & Event-Driven Autoscaling — Master Index
tags:
  - keda
  - kubernetes
  - autoscaling
  - kafka
  - learning/index
  - senior
aliases:
  - KEDA Index
  - KEDA Track
  - Event-Driven Autoscaling
status: ready
created: 2026-07-25
---

# KEDA & Event-Driven Autoscaling — Master Index

> [!abstract] Goal
> Everything a platform or backend engineer needs to **autoscale event-driven workloads on Kubernetes** — with **Kafka consumers as the running example**, because Kafka is where the naive answer (a CPU-based HorizontalPodAutoscaler) fails hardest and most instructively. The track goes from *why CPU is the wrong signal* through KEDA's architecture, the full `ScaledObject` API, the Kafka scaler's exact replica arithmetic, scale-to-zero, tuning, authentication, observability and production operations — and it includes **four hands-on labs you run locally in Docker**, where you watch real consumer lag drive real pod counts and real partition reassignments. Concept-first and interview-grade: every module teaches you to justify a choice, name the failure mode, and mitigate it. Pairs with the [Spring Boot Tutorials](https://aw-santo.github.io/tutos/00---spring-boot-index) track when the consumer you're scaling is a Spring service.

---

## How to use this track

You don't learn KEDA by memorizing `ScaledObject` fields — you learn it by understanding **which component makes which decision**. KEDA is not an autoscaler that replaces the HPA; it is a controller that *feeds and owns* an HPA, and takes over exactly one thing the HPA cannot do: crossing zero. Once that split is clear, most KEDA confusion dissolves.

Go top-to-bottom for a full pass, or jump to the module you need. Every reference module follows the same through-line: **concept → how it works internally → trade-offs & alternatives → gotchas (shown failing) → mitigations → best practice → interview talking points → sources.** The four labs follow a different shape on purpose: **build it → 🔴 break it → 🟢 fix it → checkpoint.**

> [!important] The through-line every module answers
> What it is → the trade-offs → **when NOT to use it** → the failure mode → the committed recommendation. That is what a senior platform interview probes: not "what is a ScaledObject" but "your Kafka consumer is at 40 replicas on a 12-partition topic and lag isn't falling — what happened?"

> [!warning] Versions here are HIGH-CHURN — re-verify before relying on them
> This track is written against **KEDA v2.20.1** (released 2026-06-08) on **Kubernetes v1.33–v1.35**, with **Apache Kafka 4.3.x**. Two things make KEDA unusually version-sensitive:
> 1. **The CRDs are still `v1alpha1` and will stay there** — the maintainers have declined to graduate the API on capacity grounds. **Breaking changes therefore ship in *minor* releases** under a two-release deprecation window; v2.18, v2.19 and v2.20 each removed previously-valid fields.
> 2. **Releases land every ~4 months** (January / May / September, per the [roadmap](https://github.com/kedacore/keda/blob/main/ROADMAP.md)), and the supported Kubernetes window is **N-2** — only the three most recent minors.
>
> **Pin your KEDA version and read the release notes before every upgrade.** The *concepts* here (lag as a signal, the partition ceiling, activation vs scaling, rebalance cost) are durable; the *field names and defaults* move. Each module flags its volatile facts.

> [!important] Four ideas that run through EVERY module (the spine)
> 1. **KEDA owns 0↔1; the HPA owns 1↔N.** The operator writes the `scale` subresource directly to cross zero, because an HPA's `minReplicas` cannot be 0 outside an alpha feature gate. Almost every "why won't it scale" question resolves to *which of the two is responsible for this transition*. See [[03 - KEDA Architecture — Operator, Metrics Adapter & the HPA It Owns]].
> 2. **The scaler never computes a replica count.** It reports one number; the HPA divides. Understanding that division — and the fact that KEDA enforces the partition ceiling by **deliberately under-reporting the metric** — is the single highest-value insight in the track. See [[07 - The Kafka Scaler — Every Field & the Replica Math]].
> 3. **Every scale event is a Kafka rebalance.** Autoscaling a consumer group has a cost that KEDA's own documentation never mentions, and managing it is the difference between autoscaling that helps and autoscaling that hurts. See [[02 - Kafka Consumer Groups, Partitions & Rebalancing]].
> 4. **Lag is a proxy, not the truth.** Offset lag ignores message size, processing cost, compaction and transaction markers. Knowing where the proxy breaks is what separates a working config from a cargo-culted one. See [[08 - Scale-to-Zero & Activation]].

---

## Curriculum

| # | Module | Core question | Animated? | Status |
|---|--------|--------------|:---:|--------|
| 01 | [[01 - Why Event-Driven Autoscaling — The Limits of HPA]] | the HPA formula, resource vs custom vs external metrics, why CPU is orthogonal to backlog, why HPA can't reach zero | 🎞️ | `ready` |
| 02 | [[02 - Kafka Consumer Groups, Partitions & Rebalancing]] | partition assignment caps parallelism; eager vs cooperative; KIP-848; committed offset vs high watermark vs lag | 🎞️ | `ready` |
| 03 | [[03 - KEDA Architecture — Operator, Metrics Adapter & the HPA It Owns]] | the components, the mTLS gRPC hop, `external.metrics.k8s.io`, who owns 0↔1 vs 1↔N | 🎞️ | `ready` |
| 04 | [[04 - The ScaledObject CRD, End to End]] | every field with its real default, `metricType`, status conditions, the admission-webhook rejections | | `ready` |
| 05 | [[05 - LAB 1 — Local Cluster, Kafka & KEDA from Scratch]] | a working local stack on your laptop that you can break safely | | `ready` |
| 06 | [[06 - LAB 2 — Your First Kafka ScaledObject]] | burst a topic and watch lag → metric → replicas, and partitions get reassigned | | `ready` |
| 07 | [[07 - The Kafka Scaler — Every Field & the Replica Math]] | `kafka` vs `apache-kafka`, `ceil(lag/lagThreshold)`, and the partition cap enforced by truncating the metric | 🎞️ | `ready` |
| 08 | [[08 - Scale-to-Zero & Activation]] | `activationLagThreshold` vs `lagThreshold`, the invalid-offset matrix, the 7-day offset-retention trap | 🎞️ | `ready` |
| 09 | [[09 - LAB 3 — Break It on Purpose]] | trigger each production failure mode live — rebalance churn, flapping, lost offsets — then fix it | | `ready` |
| 10 | [[10 - Tuning Scaling Behaviour]] | `pollingInterval`, `cooldownPeriod` vs HPA `stabilizationWindowSeconds` — which knob governs what | 🎞️ | `ready` |
| 11 | [[11 - Multiple Triggers, Scaling Modifiers & Fallback]] | max-of-triggers arithmetic, the formula language, `fallback.behavior` | | `ready` |
| 12 | [[12 - Authentication — TriggerAuthentication, SASL, TLS & MSK IAM]] | working SCRAM/TLS config, external secret stores, and the secret-read blast radius | | `ready` |
| 13 | [[13 - ScaledJob — When a Pod per Message Beats a Consumer Pool]] | long-running work, Kubernetes-level retry, and why ScaledJob *increases* rebalances | | `ready` |
| 14 | [[14 - Observability & Debugging — Why Isn't It Scaling]] | the real metric names, status conditions, CloudEvents, and the `kubectl get --raw` drill | | `ready` |
| 15 | [[15 - Production Operations]] | HA limits, upgrades & version pinning, `watchNamespace`, KEDA + Cluster Autoscaler/Karpenter | | `ready` |
| 16 | [[16 - Alternatives & Trade-offs]] | HPA + Prometheus Adapter, Knative, VPA, node autoscalers — when to pick which | | `ready` |
| 17 | [[17 - LAB 4 — Production-Grade Kafka Autoscaling Capstone]] | assemble everything against an SLO, with fallback and alerting | | `ready` |
| 18 | [[18 - Interview Drill — KEDA & Kafka Autoscaling Q&A]] | rapid recall of the answers a senior is expected to have | | `ready` |

---

## Suggested order

**Foundations** (01–04) → **get it running** (05–06) → **the scaler in depth** (07–09) → **tuning & judgment** (10–13) → **operate it** (14–16) → **capstone & recall** (17–18).

Do **01 → 02 → 03 → 05 → 06** first regardless of your goal. Modules 01–03 are the conceptual spine, and doing Lab 1 and Lab 2 early means every later module has something concrete to point at — you can test any claim in the track against a cluster on your laptop within minutes.

If you only have time for six before an interview: **01, 02, 03, 07, 08, 10**. The HPA's limits, the rebalance cost, the ownership split, the replica arithmetic, activation, and the cooldown-vs-stabilization-window confusion are where senior questions concentrate and where real incidents come from.

---

## How this fits the rest of the vault

```text
KUBERNETES WORKLOADS                    Deployments, the scale subresource, probes
        +
HORIZONTAL POD AUTOSCALER               the built-in control loop: resource,
                                        custom & external metrics — but never zero
        ▼
KEDA (this track)  ──────────────────►  event-driven triggers, scale-to-zero,
                                        60+ scalers; feeds and OWNS an HPA
        +
APACHE KAFKA                            consumer groups, partitions, offsets,
                                        rebalancing — the parallelism ceiling
        ▼
YOUR CONSUMER SERVICE                   graceful shutdown, idempotency, DLQs
                                        (bridges to [[00 - Spring Boot Index]] —
                                         see [[13 - Resilience & Production Readiness]]
                                         and [[10 - Messaging, Events & Async]])
```

KEDA sits **between** the Kubernetes autoscaling machinery and your event source. It does not replace the HPA and it does not replace node autoscaling — it is the piece that translates "there is work waiting in Kafka" into "there should be more pods," and it is the only piece that can say "there is no work, run none at all."

The consumer-side concerns that make autoscaling *safe* — graceful shutdown, idempotency, at-least-once delivery, dead-letter queues — are application concerns, and they are covered from the Spring side in [Messaging, Events & Async](https://aw-santo.github.io/tutos/10---messaging,-events--and--async) and [Resilience & Production Readiness](https://aw-santo.github.io/tutos/13---resilience--and--production-readiness) over on the Spring Boot Tutorials site. This track assumes them and shows where autoscaling breaks when they're missing.

---

## Anchor sources (re-verify — KEDA ships breaking changes in minor releases)

- KEDA docs: [keda.sh/docs/2.20](https://keda.sh/docs/2.20/) · [ScaledObject spec](https://keda.sh/docs/2.20/reference/scaledobject-spec/) · [ScaledJob spec](https://keda.sh/docs/2.20/reference/scaledjob-spec/) · [Kafka scaler](https://keda.sh/docs/2.20/scalers/apache-kafka/) · [FAQ](https://keda.sh/docs/2.20/reference/faq/)
- KEDA project: [kedacore/keda](https://github.com/kedacore/keda) · [CHANGELOG](https://github.com/kedacore/keda/blob/main/CHANGELOG.md) · [ROADMAP](https://github.com/kedacore/keda/blob/main/ROADMAP.md) · [governance/SUPPORT.md](https://github.com/kedacore/governance/blob/main/SUPPORT.md) · [charts](https://github.com/kedacore/charts)
- Kubernetes: [HorizontalPodAutoscaler walkthrough & formula](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/) · [HPA v2 API](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/horizontal-pod-autoscaler-v2/) · [Pod lifecycle & termination](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/)
- Apache Kafka 4.3: [consumer configs](https://kafka.apache.org/43/configuration/consumer-configs/) · [the new consumer rebalance protocol (KIP-848)](https://kafka.apache.org/43/operations/consumer-rebalance-protocol/) · [monitoring](https://kafka.apache.org/43/operations/monitoring/) · [KIP-429 incremental cooperative rebalancing](https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol)

> [!note] On the diagrams
> Animated GIFs are generated by `assets/gen_keda_gifs.py` (Pillow, via the `animated-diagrams` skill engine). Re-run it to tweak a figure: `python3 assets/gen_keda_gifs.py lagmath`. Obsidian animates GIFs in reading view (animated SVG renders frozen and Mermaid is static, so GIF is used for anything that *moves*).

> [!tip] Publishing this track
> The notes use wikilinks and relative `assets/` embeds, both of which [Quartz](https://quartz.jzhao.xyz/) resolves natively — so this folder can be published as a site without rewriting links.
