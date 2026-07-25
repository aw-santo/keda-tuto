---
title: 02 - Kafka Consumer Groups, Partitions & Rebalancing
tags:
  - keda
  - kafka
  - autoscaling
  - kubernetes
  - senior
aliases:
  - Consumer Groups
  - Partition Ceiling
  - Rebalancing
  - KIP-848
  - CooperativeStickyAssignor
status: ready
created: 2026-07-25
---

# Kafka Consumer Groups, Partitions & Rebalancing

> [!abstract] Scope
> The Kafka half of the picture, written for someone who has used consumer groups but hasn't had to think about what happens when the group's size changes every few minutes. Three ideas carry the rest of the track: **the partition count is a hard ceiling on useful consumers**; **lag is measured from *committed* offsets**, which is why a consumer that processes without committing looks permanently behind; and **every scale event is a rebalance** — a cost KEDA's documentation never mentions and you have to manage yourself. Feeds directly into [[07 - The Kafka Scaler — Every Field & the Replica Math]].

Related: [[00 - KEDA Index]], [[01 - Why Event-Driven Autoscaling — The Limits of HPA]], [[07 - The Kafka Scaler — Every Field & the Replica Math]], [[08 - Scale-to-Zero & Activation]], [[10 - Tuning Scaling Behaviour]], [[13 - ScaledJob — When a Pod per Message Beats a Consumer Pool]]

![[assets/kafka-rebalance.gif|760]]

> [!warning] Written against Apache Kafka 4.3 · KEDA v2.20 · Kubernetes 1.33–1.35
> Kafka 4.x is a moving target in exactly the area this module covers. The classic rebalance protocol is on a deprecation path (per KIP-1274, `KafkaConsumer` is slated to *default* to the new protocol in 5.0 and to support only the new protocol in 6.0), so the defaults below — especially `partition.assignment.strategy` and `group.protocol` — will change under you. Check `kafka.apache.org/<version>/configuration/consumer-configs/` for whatever brokers and clients you actually run.

---

## 1. The partition ceiling — the single most important fact

A consumer group divides a topic's partitions among its members. The rule, verbatim from the `KafkaConsumer` javadoc:

> "This is achieved by balancing the partitions between all members in the consumer group so that **each partition is assigned to exactly one consumer in the group**."

Exactly one. Not at least one, not usually one. So:

- **A 12-partition topic can usefully support at most 12 consumers in a group.** Member 13 joins, gets assigned nothing, and polls an empty assignment forever. It's a running pod, holding memory and a broker connection, doing zero work.
- **Adding consumers cannot fix a hot partition.** If a bad partition key funnels 80% of traffic into partition 3, partition 3 still has exactly one owner. Scaling to 40 pods gives you one pod working and 39 idle. The fix is in the producer's key choice, or in repartitioning the topic — never in the autoscaler.

This is why `maxReplicaCount` on a Kafka ScaledObject should be the partition count, and why KEDA caps its reported metric so the HPA's arithmetic lands there by itself ([[07 - The Kafka Scaler — Every Field & the Replica Math]]).

**Choose partition counts with autoscaling in mind.** You can add partitions to a topic later but never remove them, and adding them reshuffles key→partition mapping for hash-partitioned producers — so ordering guarantees per key break across the change. Pick a number with plenty of divisors up front:

| Partitions | Useful consumer ceiling | Comment |
|---|---|---|
| 1 | 1 | not autoscalable at all — a common surprise on a default-created topic |
| 6 | 6 | fine for modest throughput |
| **12 / 24 / 48** | 12 / 24 / 48 | **good choices** — many divisors, room to grow |
| 13 / 17 / 31 | 13 / 17 / 31 | prime: works, but rules out `ensureEvenDistributionOfPartitions` |

Check what you actually have before you write a `maxReplicaCount`, because the answer is frequently 1 on a topic that was auto-created:

```bash
kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic orders
# Topic: orders  PartitionCount: 12  ReplicationFactor: 3  Configs: ...
```

> [!warning] Gotcha — two Deployments sharing one `group.id` break the ceiling arithmetic
> KEDA reasons about a *ScaledObject* and a *consumer group*, and quietly assumes they correspond. Put two Deployments (a v1 and a v2 during a migration, a main consumer and a "reprocessor") on the same `group.id` and they are members of one group competing for the same 12 partitions — but each ScaledObject computes its replica count from the group's *total* lag as though it owned all of it. Both scale to 12 and you get 24 members on 12 partitions, half of them idle. It also blocks the assignor migration in §4, since the group is only cooperative when *every* member is.
> **Mitigation:** one consumer group per ScaledObject, always. If you need a second workload on the same topic, give it its own `group.id` — it then gets its own offsets, its own lag, and its own honest ceiling. During a rolling v1→v2 migration on a shared group, accept that the ceiling is shared and cap both at a total of the partition count.

> [!tip] Best practice — pick 12, not 10 or 13
> Create autoscaled topics with a **composite** partition count. 12 gives you the divisor ladder 1, 2, 3, 4, 6, 12; 13 gives you 1 and 13 and nothing between. That matters directly if you ever enable `ensureEvenDistributionOfPartitions`, and it's free to get right at topic-creation time and painful to change afterwards.

---

## 2. Offsets and lag — what KEDA is actually reading

Each partition is an append-only log with monotonically increasing offsets. A consumer group stores, per partition, a **committed offset** — the position it has durably recorded in Kafka's internal `__consumer_offsets` topic. Lag is the gap to the end of the log:

```
lag = end offset − committed offset
```

The canonical way to look at it:

```bash
kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --describe --group orders-workers
```

```text
GROUP           TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG   CONSUMER-ID              HOST         CLIENT-ID
orders-workers  orders  0          104233          104233          0     consumer-orders-1-a3f…   /10.42.1.17  consumer-orders-1
orders-workers  orders  1          98014           99120           1106  consumer-orders-1-a3f…   /10.42.1.17  consumer-orders-1
orders-workers  orders  2          101887          101890          3     consumer-orders-2-7c1…   /10.42.2.9   consumer-orders-2
orders-workers  orders  3          -               412             -     consumer-orders-2-7c1…   /10.42.2.9   consumer-orders-2
orders-workers  orders  4          77510           81440           3930  consumer-orders-3-e02…   /10.42.3.4   consumer-orders-3
orders-workers  orders  5          88301           88301           0     consumer-orders-3-e02…   /10.42.3.4   consumer-orders-3
```

Read it column by column:

- **CURRENT-OFFSET** is the group's *committed* offset. **This is the number KEDA reads.**
- **LOG-END-OFFSET** is the end of the log as a consumer sees it — the high watermark for a normal `read_uncommitted` consumer, or the *last stable offset* for a `read_committed` one, which is a genuinely different number when transactions are in flight.
- **LAG** is the subtraction. Summing this column across partitions is, near enough, the number KEDA publishes.
- **Partition 3 shows `-` for both.** That means *no committed offset exists* for this group on that partition — the group has never committed there. Its lag is not zero and not 412; it is **undefined**, which is why the tool prints a dash rather than a number.

That undefined case is not an edge case you can ignore, because it's the state of every brand-new consumer group and every partition a group hasn't reached yet. KEDA has to invent a number, and `offsetResetPolicy` is the field that decides what it invents — treat "no committed offset" as *the whole partition is backlog* (`earliest`) or as *near the end* (`latest`). Getting it wrong gives you either a phantom replay-sized backlog or a workload that never wakes up. The full matrix is in [[08 - Scale-to-Zero & Activation]].

> [!important] KEDA reads committed offsets — not your consumer's position
> This has a consequence people find genuinely surprising. Your consumer's in-memory position races ahead of its committed offset by one commit interval's worth of work. KEDA doesn't see that. It sees only what has been committed to the broker.
> Mostly this is a **feature**: it counts work that is not yet durably done, which is the honest definition of backlog. But it means **a consumer that processes messages and doesn't commit them looks permanently behind**, and KEDA will keep scaling up in response. New pods join, also don't commit, and lag keeps climbing. You end up at `maxReplicaCount` with a busy consumer and an idle Kafka cluster.
> Also note that the client-side JMX metric `records-lag-max` is based on the *current* position, not the committed offset — so your application dashboard and KEDA will legitimately disagree, and neither is wrong.

> [!warning] Gotcha — a broken commit path is indistinguishable from a real backlog
> Every symptom is identical: growing lag, rising replicas, pods at the ceiling. Manual commits behind a swallowed exception, a `commitAsync` whose callback nobody reads, an offset-commit path that runs only on clean shutdown — all present as "KEDA is over-scaling".
> **Mitigation:** the first diagnostic is always `kafka-consumer-groups.sh --describe`, watching whether **CURRENT-OFFSET advances**. If it doesn't move, this is an application bug and no autoscaling setting will help. Keep your commit interval comfortably below KEDA's `pollingInterval` (default 30 s) so KEDA sees progress between polls, and alert on committed-offset progress, not just on lag.

> [!tip] Useful when there are no consumers at all
> At zero replicas `--describe` still prints committed offsets and lag, with `-` in the CONSUMER-ID, HOST and CLIENT-ID columns and a `Consumer group 'orders-workers' has no active members.` line. That's the state KEDA wakes a workload from, and being able to read it is how you confirm the group's offsets survived the idle period ([[08 - Scale-to-Zero & Activation]] covers the 7-day offset-retention trap).

---

## 3. Rebalancing — the hidden cost of autoscaling

A rebalance is the group re-deciding who owns which partition. The javadoc lists what triggers one:

- the number of partitions changes for a subscribed topic
- a subscribed topic is created or deleted
- an existing member is shut down or fails
- **a new member is added to the group**

Look at the last two together. **Every autoscaling event is a rebalance.** Scale up: a new member joins → rebalance. Scale down: a member leaves → rebalance. There is no scaling action on a consumer group that isn't also a group-membership change. Autoscaling a Kafka consumer is not free the way autoscaling a stateless HTTP service is free, and this is the reason.

Two further mechanics make it sharper:

**Rebalances only progress inside `poll()`.** The consumer's rebalance work happens during an active call to `poll(Duration)`. So one member sitting in a slow message handler doesn't just delay itself — the group's rebalance waits on it. A single 4-minute handler holds up everyone.

**Then there's the failure-detection cost.** A pod killed without a clean shutdown isn't noticed until its session times out — `session.timeout.ms` defaults to **45000** (45 s), with heartbeats every **3000** ms. A consumer that closes cleanly sends an explicit LeaveGroup and the group reacts immediately. That difference — 45 seconds of a partition owned by a dead pod versus none — is entirely down to whether your container handles SIGTERM (§7).

> [!warning] Gotcha — flapping replica counts multiply into rebalance churn
> A twitchy scaling config (a `lagThreshold` of 1, a bursty producer, no scale-up stabilisation) doesn't merely churn pods. Each churn is a membership change, and each membership change pauses or disrupts consumption for the whole group. The failure mode is self-reinforcing: rebalancing suppresses throughput, suppressed throughput grows lag, growing lag asks for more pods, more pods means another rebalance.
> **Mitigation:** three things, in order of leverage. (1) Adopt the new rebalance protocol (§5) so a rebalance stops being a group-wide stall. (2) Set `lagThreshold` to roughly one pod's throughput per `pollingInterval` so the count moves in meaningful steps. (3) Add `behavior.scaleUp.stabilizationWindowSeconds` — Kubernetes applies **no** stabilisation to scale-up by default, only to scale-down ([[10 - Tuning Scaling Behaviour]]).

The two commands to reach for when you suspect rebalancing rather than throughput:

```bash
# Is the group stable right now, and which assignment strategy is it really using?
kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group orders-workers --state
# COORDINATOR  ASSIGNMENT-STRATEGY  STATE   #MEMBERS
# kafka-1:9092  range               Stable  6

# Who are the members and what did each one get assigned?
kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group orders-workers --members --verbose
```

A `STATE` that is anything other than `Stable` — `PreparingRebalance`, `CompletingRebalance` — while you're watching means you're paying for a membership change right now. If it's rarely `Stable`, your scaling config is churning faster than the group can settle, and no amount of extra replicas will help.

> [!warning] Gotcha — adding partitions to a live autoscaled topic does two things at once
> Increasing the partition count is the natural response to hitting the ceiling, and it's correct — but it *also* triggers a rebalance across the group (a subscribed topic's partition count changed), and it changes the key→partition mapping for hash-partitioned producers. Messages for a given key now land on a different partition from the ones already in the log, so **per-key ordering is not preserved across the change** and two pods can be processing the same key concurrently for a while.
> **Mitigation:** treat repartitioning as a planned change, not a capacity tweak. Do it during a quiet period, ideally with the topic drained, and only if per-key ordering isn't load-bearing for you. If it is, create a new topic with the right partition count and migrate — and pick a composite number this time. Remember partitions can never be removed.

> [!note] On vocabulary and on numbers — two things to be careful about
> **"Rebalance storm" is not an Apache Kafka term.** It comes from Confluent's writing on incremental cooperative rebalancing, and there it describes **Kafka Connect** ("it could bring the cluster into a state of consecutive rebalances"). Use it if you like, but know it's vendor vocabulary about a different subsystem — the consumer-side mechanism is described by KIP-345, which says only that "one process restart will trigger one rebalance".
> **Apache publishes no quantified per-rebalance cost.** Not a millisecond figure, not a multiplier. The official statements are proportional: the new protocol "decreases rebalance times"; cooperative rebalancing avoids stopping the world. The "20× faster" style figures circulating in vendor blogs are unverified — **don't cite them in a design doc or an interview.** Say "it removes the global synchronisation barrier" and you're on solid, citable ground.

---

## 4. Eager vs cooperative — and the mixed-list trap

Under the classic protocol there are exactly two rebalance protocols, and the javadoc defines them crisply. **EAGER** requires a consumer to revoke *all* its owned partitions before participating in a rebalance. **COOPERATIVE** lets a consumer retain its partitions through the rebalance and only give up the ones actually being moved.

Eager is the "stop-the-world" behaviour that KIP-429 was written to fix. The reason it has to work that way is a correctness requirement: each partition must be owned by exactly one consumer at any instant, so no partition can be reassigned before it has been revoked. Everybody drops everything, the coordinator computes a new assignment, everybody picks up. Consumption stops for the whole group in between — including for the 11 partitions that weren't going to move.

**And eager is what you get out of the box.** From the Kafka 4.3 consumer configuration docs, on `partition.assignment.strategy`:

> "The default assignor is [RangeAssignor, CooperativeStickyAssignor], **which will use the RangeAssignor by default**, but allows upgrading to the CooperativeStickyAssignor with just a single rolling bounce that removes the RangeAssignor from the list."

So the default list is a migration aid, not a cooperative configuration. Of the built-in assignors, **only `CooperativeStickyAssignor` is cooperative** — `RangeAssignor`, `RoundRobinAssignor` and `StickyAssignor` are all eager.

> [!warning] Gotcha — a mixed assignor list silently behaves as eager
> This is the one that catches careful people. The group's effective protocol is the one **common to all members**, so a list containing both `RangeAssignor` and `CooperativeStickyAssignor` resolves to eager — that is the documented purpose of shipping them in that order. And it applies per *member*: if you roll out cooperative-only to nine pods and one straggler still has the default list, **the whole group stays eager.** No warning, no error, no metric — you did the work and got none of the benefit.
> **Mitigation:** it's a two-step rolling bounce, done in order, and both steps must reach every member. First deploy `[CooperativeStickyAssignor, RangeAssignor]` everywhere (so mixed old/new members can still agree). Then deploy `[CooperativeStickyAssignor]` alone. Verify against a live group rather than against your config: `kafka-consumer-groups.sh --describe --group <g> --state` shows the assignment strategy the group is actually using. A KEDA-scaled Deployment reaches a uniform config quickly because pods are replaced constantly — but a leftover manually-scaled pod or a second Deployment sharing the group will hold you back indefinitely.

```properties
# Step 1 — everywhere. Cooperative preferred, range as the fallback for mixed groups.
partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor,\
org.apache.kafka.clients.consumer.RangeAssignor

# Step 2 — everywhere, after step 1 has fully rolled out. Now the group is cooperative.
partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

---

## 5. KIP-848 — the new rebalance protocol, and why it's the real fix

Apache Kafka 4.0 shipped the next generation of the consumer rebalance protocol as **Generally Available and ready for production**. It moves assignment computation from a client-elected group leader to the broker's group coordinator and makes reconciliation fully incremental — which means it **no longer relies on a global synchronisation barrier.** That phrasing is the point: the thing that made a rebalance expensive was that every member had to synchronise on every rebalance. The new protocol removes it, rather than making it cheaper.

There is one detail that matters more than anything else in this section:

**It is on by default on the broker, and off by default in the client.** From Kafka 4.0 the new protocol is automatically enabled server-side (governed by the `group.version` feature flag), but the protocol itself is not enabled by default — **the client must set `group.protocol=consumer`.** In Kafka 4.3 the client-side default is still `classic`. So an upgraded cluster full of upgraded clients can be running the old protocol with nobody having made a decision.

```properties
# The single highest-leverage change for scale-churn pain.
# Requires Kafka >= 4.0 brokers and a client that supports the new protocol.
group.protocol=consumer
group.id=orders-workers
```

That's the whole client change. Heartbeat and session timing move to the broker side (`group.consumer.heartbeat.interval.ms`, default 5000; `group.consumer.session.timeout.ms`, default 45000) and assignors become server-side (`group.consumer.assignors`, default `uniform,range`).

> [!warning] Gotcha — turning it on makes three familiar configs unusable
> Because the broker now drives timing and assignment, `heartbeat.interval.ms`, `session.timeout.ms` and `partition.assignment.strategy` become **unusable** with `group.protocol=consumer` — as does programmatic `enforceRebalance()`. If your consumer sets any of them (and most production consumers set at least the timeouts), the switch is not config-neutral. Client-side assignors are explicitly out of scope for now, and rack-aware assignment is not fully supported yet.
> **Mitigation:** audit your consumer config for those keys and remove them before flipping `group.protocol`, then move the timing intent to the broker-side `group.consumer.*` settings if you genuinely needed non-default values. Roll it out to one consumer group in a non-production environment first — this is a group-level behaviour change, not a per-pod tweak. If you rely on a custom client-side assignor, you can't adopt it yet; use `CooperativeStickyAssignor` instead.

> [!tip] Best practice — if your brokers are 4.0+, set `group.protocol=consumer`
> For an autoscaled consumer group this is the committed recommendation. Scale churn is *precisely* the workload the new protocol was designed for: frequent, small membership changes on a group that would otherwise synchronise globally on every one of them. Adopting it makes the assignor question in §4 disappear entirely, because assignment moves to the broker.
> If your brokers are older than 4.0, fall back to `CooperativeStickyAssignor` everywhere — and do the two-step rollout properly.

---

## 6. Static membership — a trade-off, not a win

`group.instance.id` makes a consumer a **static** member: its identity survives a restart, so a bounce that would normally trigger a rebalance doesn't. Combined with a larger session timeout, that's the documented purpose — avoiding rebalances caused by transient unavailability such as process restarts. KIP-345 describes the design as prioritising *state persistence* over *liveness*, which is an unusually honest summary of the trade.

The cost is exactly that reprioritisation, and it lands badly on an autoscaled workload:

- **A scaled-in static member's partitions stay stranded.** The docs for `max.poll.interval.ms` are explicit that for a consumer with a non-null `group.instance.id`, hitting that timeout does **not** immediately reassign its partitions — they're reassigned only after the session timeout expires. That's the feature working as designed: the group is waiting for the member to come back. But when KEDA scaled that pod away deliberately, nobody is coming back, and its partitions have no owner for the full session timeout. KIP-345 raised the session-timeout ceiling to 30 minutes, so this can be a very long stall.
- **It needs stable per-replica identity.** Each member needs its own durable `group.instance.id`. A Deployment's pods have random names, so in practice you need a StatefulSet and its ordinals to derive one.
- **Neither of those composes with scaling to zero.** Static membership optimises for *a fixed set of members that occasionally restart.* Scale-to-zero is the opposite: the member set is the thing that changes, and sometimes it's empty.

> [!warning] Gotcha — static membership plus autoscaling is a poor fit, and this is reasoning, not documentation
> Be honest about the epistemic status here: Apache documents static membership, and KEDA documents autoscaling, but **the interaction between them is not documented anywhere by either project.** The conclusion above is inference from two verified facts — that a static member's partitions wait for the session timeout, and that KEDA changes membership constantly. Present it that way rather than as a citation, and you'll be on firmer ground than the blog posts.
> **Mitigation:** don't use `group.instance.id` on a workload whose replica count KEDA controls. If you have a genuine reason to (a stateful consumer with expensive local warm-up), then: use a StatefulSet, keep the session timeout modest, make absolutely certain the consumer calls `close()` on SIGTERM so it sends an explicit LeaveGroup instead of waiting for the timeout, and don't set `minReplicaCount: 0`. If you find yourself wanting static membership because rebalances hurt, the correct fix is §5, not this.

---

## 7. Consumer-side hygiene that makes autoscaling safe

Autoscaling exposes application bugs that a fixed replica count hides. Four things to get right; none are KEDA settings.

**Bound your batch with `max.poll.records`.** Default **500**. Every poll must complete within `max.poll.interval.ms` (default **300000**, five minutes) or the broker considers the member gone and rebalances it out — and the batch's work is redone by someone else. Smaller batches mean shorter uncommitted windows, faster shutdowns, and less redelivery on scale-in. Tune it to what one poll can genuinely finish in a few seconds.

**Handle SIGTERM.** This is the highest-value item on the list. Kubernetes sends SIGTERM and then waits `terminationGracePeriodSeconds` — **30 by default** — before SIGKILL. A consumer that ignores it gets killed mid-batch: the batch is uncommitted, the messages come back to another pod after a rebalance, and the group waits out `session.timeout.ms` before even noticing the pod is gone. The shutdown you want is: stop polling → finish the current batch → commit → `close()` the consumer (which sends LeaveGroup and prompts an immediate rebalance) → exit.

```yaml
spec:
  template:
    spec:
      terminationGracePeriodSeconds: 90   # > worst-case batch time; default is 30
      containers:
      - name: consumer
        # the container must trap SIGTERM and reach the consumer's close() path.
        # If your entrypoint is a shell wrapper, use `exec` so PID 1 is the app
        # and actually receives the signal.
```

**Be idempotent.** Kafka is at-least-once by default and autoscaling gives you more of the events that cause redelivery — every scale-in is a chance for an uncommitted batch to be reprocessed. Idempotency isn't a mitigation for an autoscaling bug; it's a precondition for autoscaling at all. Deduplicate on a message key or an idempotency key at the point of side effect.

**Have a dead-letter queue.** A message your consumer can't process wedges the partition: the offset stops advancing, lag grows without bound, KEDA scales up, and each new pod eventually inherits the same poison message. A DLQ means processing always moves the offset forward. KEDA's `excludePersistentLag` dampens the *symptom* — it discounts partitions whose committed offset isn't moving — but the fix is in the consumer. See [[07 - The Kafka Scaler — Every Field & the Replica Math]].

> [!tip] If a single message takes minutes, you're in the wrong shape
> The hygiene above assumes short per-message work. If processing one message legitimately takes minutes, a warm consumer pool is the wrong pattern — you'll fight `max.poll.interval.ms`, grace periods and rebalances forever. That's what [[13 - ScaledJob — When a Pod per Message Beats a Consumer Pool]] is for. Note it has its own rebalance trade-off, though: a Job's consumer joins and leaves the group, so sharing one `group.id` across Jobs produces *more* rebalances, not fewer.

---

## 8. Alternatives — how the group handles membership change

This is the real decision this module leads to. Assume Kafka 4.x brokers.

| Option | Behaviour during a membership change | Choose it when |
|---|---|---|
| **`RangeAssignor`** (default) | eager: every member revokes every partition; consumption stops group-wide until reassignment completes | never, deliberately, for an autoscaled group. You have it because it's the default — that's the whole reason it's here |
| **`CooperativeStickyAssignor`** (only) | cooperative: members keep partitions they're not losing; only moved partitions pause | brokers are older than 4.0, or your client can't use the new protocol yet. Requires removing `RangeAssignor` from **every** member |
| **KIP-848, `group.protocol=consumer`** | broker-driven, fully incremental, **no global synchronisation barrier**; assignors move server-side | **brokers are 4.0+ and the client supports it — the default recommendation for anything KEDA scales.** Cost: `heartbeat.interval.ms`, `session.timeout.ms` and `partition.assignment.strategy` become unusable |
| **Static membership (`group.instance.id`)** | restarts don't rebalance at all; but a departed member's partitions wait out the session timeout | a fixed-size, stateful consumer set that restarts often — **not** a workload whose size KEDA changes, and not one that scales to zero |

**The committed recommendation:** if your brokers are Kafka 4.0 or newer, set `group.protocol=consumer` and stop thinking about assignors. If they aren't, set `partition.assignment.strategy` to `CooperativeStickyAssignor` alone, via a two-step rolling bounce, and verify with `--describe --state` that the group actually switched. Leave static membership out of an autoscaled group.

---

## 9. Questions you should be able to answer

- How many consumers in a group can usefully read a 12-partition topic? *(12. Each partition has exactly one owner in the group.)*
- One partition is hot. Does scaling out help? *(No — one owner per partition. Fix the producer's partition key or repartition.)*
- What exactly does KEDA subtract to get lag? *(End offset minus the group's **committed** offset, per partition, summed.)*
- `--describe` shows `-` for CURRENT-OFFSET on one partition. What does that mean and what resolves it? *(No committed offset exists for the group there, so lag is undefined. `offsetResetPolicy` decides what KEDA counts instead.)*
- Your consumer is processing happily but lag never falls. First check? *(Whether CURRENT-OFFSET is advancing. If not, it's a commit bug, not an autoscaling problem.)*
- Name four things that trigger a rebalance, and say what that implies for autoscaling. *(Member joins, member leaves/fails, partition count changes, subscription changes — so **every scale event is a rebalance**.)*
- What's the default assignor in Kafka 4.x, and is it eager or cooperative? *(`RangeAssignor` first in the default list — eager. `CooperativeStickyAssignor` is second and only takes effect once `RangeAssignor` is removed from every member.)*
- You set `CooperativeStickyAssignor` and rebalances are still stop-the-world. Why? *(The group's protocol is the common one across all members — a single member with a mixed or default list keeps the whole group eager.)*
- Is KIP-848 on by default? *(GA and enabled on the broker since Kafka 4.0, but **not** in the client — you must set `group.protocol=consumer`. The 4.3 client still defaults to `classic`.)*
- What breaks when you enable it? *(`heartbeat.interval.ms`, `session.timeout.ms`, `partition.assignment.strategy` and `enforceRebalance()` become unusable — the broker drives them.)*
- Why is static membership a bad fit for a KEDA-scaled Deployment? *(A scaled-in static member's partitions stay stranded until the session timeout, and it needs stable identity, so a StatefulSet. And it's inference — nobody documents the interaction.)*
- How long does a pod that ignores SIGTERM keep its partitions after being killed? *(Until `session.timeout.ms` expires — 45 s by default. A clean `close()` sends LeaveGroup and rebalances immediately.)*
- How much faster is a cooperative rebalance? *(Trick question — Apache publishes no quantified figure. Say "it avoids stopping the world"; don't quote vendor multipliers.)*
- Two Deployments share a `group.id`, each with its own ScaledObject. What goes wrong? *(Both compute replicas from the same total lag and both scale to the partition count — twice as many members as partitions, half idle.)*
- You add partitions to relieve the ceiling. What did you also just do? *(Triggered a rebalance, and changed the key→partition mapping — per-key ordering isn't preserved across the change.)*

---

## 10. Sources

- [Apache Kafka 4.3 — consumer configuration (`partition.assignment.strategy`, `session.timeout.ms`, `max.poll.records`, `group.instance.id`, `group.protocol`)](https://kafka.apache.org/43/configuration/consumer-configs/)
- [Apache Kafka 4.3 — `KafkaConsumer` javadoc (one partition, one consumer; rebalance triggers)](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
- [Apache Kafka 4.3 — `ConsumerPartitionAssignor.RebalanceProtocol` (EAGER vs COOPERATIVE)](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/ConsumerPartitionAssignor.RebalanceProtocol.html)
- [Apache Kafka 4.3 — the new consumer rebalance protocol (KIP-848)](https://kafka.apache.org/43/operations/consumer-rebalance-protocol/)
- [Apache Kafka 4.3 — basic operations, including `kafka-consumer-groups.sh --describe`](https://kafka.apache.org/43/operations/basic-kafka-operations/)
- [Apache Kafka 4.3 — monitoring (`records-lag-max` is position-based and published by the consumer)](https://kafka.apache.org/43/operations/monitoring/)
- [KIP-429 — Kafka Consumer Incremental Rebalance Protocol (the "stop-the-world effect")](https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol)
- [KIP-345 — static membership](https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A+Introduce+static+membership+protocol+to+reduce+consumer+rebalances)
- [Confluent — incremental cooperative rebalancing (source of the term "rebalance storm", about Kafka Connect)](https://www.confluent.io/blog/incremental-cooperative-rebalancing-in-kafka/)
- [Kubernetes — Pod lifecycle and termination (`terminationGracePeriodSeconds`, SIGTERM then SIGKILL)](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/)
- [KEDA — Apache Kafka scaler](https://keda.sh/docs/2.20/scalers/apache-kafka/)
- [KEDA — Scaling Deployments](https://keda.sh/docs/2.20/concepts/scaling-deployments/)
