---
title: 05 - LAB 1 — Local Cluster, Kafka & KEDA from Scratch
tags:
  - keda
  - kafka
  - kubernetes
  - lab
  - kind
aliases:
  - LAB 1
  - KEDA Lab Setup
  - kind Kafka KEDA
status: ready
created: 2026-07-25
---

# LAB 1 — Local Cluster, Kafka & KEDA from Scratch

> [!abstract] Scope
> Build the stack every other lab in this track runs on: a **3-node `kind` cluster pinned to Kubernetes v1.35.5**, a **single-broker Apache Kafka 4.3.1 in KRaft mode**, and **KEDA 2.20.1** installed by Helm — plus the one health check that tells you whether any of it will work. You'll also deliberately break the **advertised-listener** config, because it is the single most common way a local Kafka setup fails, and it fails in a way that looks like a network problem when it isn't. Nothing autoscales in this lab; that's [[06 - LAB 2 — Your First Kafka ScaledObject]]. Total footprint ≈ **2.5–3 GB** of RAM, about 20 minutes.

Related: [[00 - KEDA Index]], [[06 - LAB 2 — Your First Kafka ScaledObject]], [[03 - KEDA Architecture — Operator, Metrics Adapter & the HPA It Owns]], [[07 - The Kafka Scaler — Every Field & the Replica Math]], [[09 - LAB 3 — Break It on Purpose]], [[02 - Kafka Consumer Groups, Partitions & Rebalancing]]

> [!important] Read this before you start — the honest status of these manifests
> Kafka, the Python client, cooperative rebalancing and SIGTERM handling in this lab were **exercised for real** on an arm64 Mac, and every manifest is **schema-validated against the real KEDA 2.20.1 CRDs**. But the **full KEDA → Kafka scaling loop has not yet been run end-to-end on a single cluster.** It is expected to work as written — nothing here is guesswork — but this may be its first complete run on your machine, so budget a little debugging time and read [§12 Troubleshooting](#12-troubleshooting) before you conclude something is broken. If you find a correction, write it into the note.

---

## 1. Goal

By the end of this lab you will have, on your laptop:

- a **`kind` cluster** named `keda-lab` — 1 control-plane + 2 workers, on **Kubernetes v1.35.5**
- **metrics-server**, patched so it actually becomes Ready on kind
- a **Kafka 4.3.1 broker** in namespace `lab`, reachable in-cluster at `kafka-0.kafka.lab.svc.cluster.local:9092`
- a topic **`orders` with 12 partitions**
- **KEDA 2.20.1** in namespace `keda`, with `v1beta1.external.metrics.k8s.io` reporting `AVAILABLE=True`
- optionally, **kafbat-ui** on `http://127.0.0.1:30080` for a visual view of lag and group membership

All the files you need are already in the track at **`lab/`** — you don't have to type any YAML. Run the commands from inside that folder.

---

## 2. Prerequisites

**Docker Desktop, resized.** Settings → Resources:

| | Minimum | Comfortable |
|---|---|---|
| Memory | **6 GB** | **8 GB** |
| CPUs | **4** | **6** |

Where that goes: kind's 3 nodes ≈ 800 MiB · KEDA's 3 deployments ≈ 300 MiB · Kafka ≈ 400 MiB (measured 366 MiB RSS) · kafbat-ui ≈ 400 MiB · up to 12 consumer pods × ~50 MiB. **Total ≈ 2.5–3 GB**, so 6 GB leaves headroom for Docker itself and your editor. Below 6 GB the broker and the KEDA metrics apiserver compete, and pods get OOM-killed at exactly the moment you're trying to read a graph.

**Tools:**

```bash
brew install kind helm kubernetes-cli
kind version && helm version --short && kubectl version --client
```

Expected — at or above `kind v0.32.0`, `helm v4.2.3`, `kubectl v1.36.3`. `kubectl` being newer than the cluster is fine; it supports one minor of skew forward and several back.

> [!note] arm64 is fully covered
> Every image in this lab (kind node, metrics-server v0.9.0, all three KEDA images, `apache/kafka:4.3.1`, `python:3.13-slim`, kafbat-ui) publishes a real `linux/arm64` manifest — nothing runs under emulation. The one ecosystem casualty is the **Bitnami Kafka chart**, and that's a licensing change rather than an architecture gap (§6).

---

## 3. Why `kind`, and why the version is pinned

| | Multi-node | Can pin the k8s version | Load a local image | Choose it when |
|---|:--:|:--:|---|---|
| **kind** | ✅ real worker containers | ✅ `--image` | `kind load docker-image` | **This lab.** ~250 MiB/node, vanilla upstream Kubernetes, nothing preinstalled to confuse the picture |
| k3d | ✅ | ✅ | `k3d image import` | You want the lightest multi-node option and don't mind disabling k3s's bundled Traefik, ServiceLB and metrics-server first |
| minikube | ✅ | ✅ | `minikube image load` | You need a driver kind doesn't have (VM isolation, a specific CNI). Heaviest on arm64 — an extra VM layer under Docker's |
| Docker Desktop's built-in Kubernetes | ✅ (kind-backed since 4.65) | ❌ **fixed** | shares the Docker image store | Never, for this lab — see below |

The decisive fact is a version window:

> [!warning] Gotcha — KEDA 2.20 is only tested against Kubernetes 1.33–1.35, and everything ships 1.36
> `kind` v0.32.0 defaults to node image **1.36.1**, and Docker Desktop 4.83.0's built-in Kubernetes is **1.36.1**. Both sit *outside* KEDA 2.20's tested range. Running outside it doesn't guarantee failure, but it does guarantee that any weirdness you hit is unsupported and undiagnosable — a bad place to start learning a tool.
> **Mitigation:** pin the node image to **`v1.35.5`** with `--image`, as below. This is also why **Docker Desktop's built-in cluster is disqualified**: its Kubernetes version tracks the Docker Desktop release and cannot be pinned. Standalone kind costs you nothing extra — Docker Desktop's own Kubernetes is kind under the hood now anyway — and buys you version control.

---

## 4. Create the cluster

`lab/kind-cluster.yaml` is 1 control-plane + 2 workers, with host port 30080 reserved:

```yaml
nodes:
  - role: control-plane
    extraPortMappings:
      - containerPort: 30080
        hostPort: 30080
        listenAddress: "127.0.0.1"
        protocol: TCP
  - role: worker
  - role: worker
```

Two workers add no meaningful compute — they exist so you can watch KEDA-created pods spread across nodes with `kubectl get pods -o wide`, which makes scaling feel real rather than abstract.

```bash
kind create cluster --config lab/kind-cluster.yaml \
  --image kindest/node:v1.35.5@sha256:ce977ae6d65918d0b58a5f8b5e940429c2ce42fa3a5619ec2bbc60b949c0ac95
```

Expected — roughly 60–90 seconds of `✓` lines (ensuring node image, preparing nodes, starting control-plane, installing CNI, joining workers), ending with `Set kubectl context to "kind-keda-lab"`.

Then confirm the version actually landed:

```bash
kubectl config use-context kind-keda-lab
kubectl get nodes
```

Expected — three nodes, all `Ready`, all **v1.35.5**:

```
NAME                     STATUS   ROLES           AGE   VERSION
keda-lab-control-plane   Ready    control-plane   72s   v1.35.5
keda-lab-worker          Ready    <none>          58s   v1.35.5
keda-lab-worker2         Ready    <none>          58s   v1.35.5
```

If `VERSION` says `v1.36.1`, the `--image` flag didn't take. Delete the cluster and try again — don't continue.

> [!warning] Gotcha — `extraPortMappings` is create-time only
> Port mappings are properties of the node *container*, fixed when Docker creates it. There is no `kind update cluster`. Deciding later that you'd like a second nodePort exposed means **deleting and recreating the whole cluster**, losing Kafka, your topic and every offset with it.
> **Mitigation:** reserve the ports you might want up front. `lab/kind-cluster.yaml` reserves 30080 for the UI. If you plan to expose anything else — a Grafana on 30081, a second broker listener on 30092 — add those mappings *now*, before you have state worth keeping. For anything ad-hoc, `kubectl port-forward` needs no cluster change and is the right tool.

---

## 5. metrics-server (and the patch it needs on kind)

```bash
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.9.0/components.yaml
kubectl -n kube-system patch deploy metrics-server --type=json \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'
kubectl -n kube-system rollout status deploy/metrics-server
```

Expected: `deployment "metrics-server" successfully rolled out`. Then, after ~30 seconds of metric collection, `kubectl top nodes` should print CPU and memory for all three nodes rather than `error: Metrics API not available`.

> [!warning] Gotcha — without `--kubelet-insecure-tls`, metrics-server never becomes Ready on kind
> The v0.9.0 release manifest deliberately ships **without** that flag, because in a real cluster you should fix the kubelet's serving certificate instead. kind's kubelets use self-signed certs that no CA in the cluster vouches for, so metrics-server's scrape fails TLS verification, its own readiness probe fails, and it sits in `0/1 Running` or CrashLoopBackOff forever. The logs say `x509: cannot validate certificate ... because it doesn't contain any IP SANs` — which reads like a cluster misconfiguration rather than an expected local-dev quirk.
> **Mitigation:** the patch above. Never carry that flag into a real cluster — there, provision proper kubelet serving certs.

**Does KEDA need this?** No. KEDA's Kafka scaler talks to Kafka, not to metrics-server, and registers its own `external.metrics.k8s.io` API. You need metrics-server for `kubectl top` and for CPU/memory triggers — worth having so you can watch what your consumers actually cost, and required if you later add a CPU trigger alongside the Kafka one.

---

## 6. Kafka — a single-broker KRaft StatefulSet

```bash
kubectl apply -f lab/kafka.yaml
kubectl -n lab rollout status sts/kafka
```

Expected — around 15–20 seconds, then `statefulset rolling update complete 1 pods at revision kafka-...`, and `kubectl -n lab get pods` showing `kafka-0   1/1   Running`.

`lab/kafka.yaml` is a Namespace, a headless Service and a 1-replica StatefulSet on the ASF-official `apache/kafka:4.3.1` image. Three parts of it are load-bearing, and each corresponds to a way local Kafka usually fails.

### 6a. Why the Service is headless

```yaml
spec:
  clusterIP: None            # headless: stable per-pod DNS for advertised listeners
```

A normal `ClusterIP` Service gives you one virtual IP that load-balances across pods — exactly wrong for Kafka, where clients must reach a **specific** broker because each partition has one leader. A headless Service creates per-pod DNS records instead, so pod `kafka-0` becomes addressable at the stable **`kafka-0.kafka.lab.svc.cluster.local`** (`<pod>.<service>.<namespace>.svc.cluster.local`). That name is what makes the next part possible.

### 6b. Why the advertised listener must resolve *from inside the broker's own pod*

```yaml
- name: KAFKA_CONTROLLER_QUORUM_VOTERS
  value: "1@kafka-0.kafka.lab.svc.cluster.local:9093"
- name: KAFKA_LISTENERS
  value: "PLAINTEXT://:9092,CONTROLLER://:9093"
- name: KAFKA_ADVERTISED_LISTENERS
  value: "PLAINTEXT://kafka-0.kafka.lab.svc.cluster.local:9092"
```

`KAFKA_LISTENERS` is where the broker *binds* — `:9092` means all interfaces. `KAFKA_ADVERTISED_LISTENERS` is the address the broker **hands out to clients** in its metadata response. A client's bootstrap connection is only ever a first hop: it asks "who are the brokers?", gets the advertised address back, and reconnects to *that*. So the advertised name must be resolvable by clients.

And in KRaft it must also resolve **from inside the broker's own pod**, because this node is both broker and controller and talks to itself through `KAFKA_CONTROLLER_QUORUM_VOTERS`. During verification, a first attempt using a name that only the *host* could resolve produced a `UnknownHostException` loop in the broker's controller-request thread — the broker came up and then failed to reach its own quorum. The headless-Service FQDN satisfies both requirements at once, which is precisely why it's used.

You'll do this wrong on purpose in §10.

> [!note] The image derives `KAFKA_LISTENERS` if you omit it
> The ASF image will build `KAFKA_LISTENERS` from `KAFKA_ADVERTISED_LISTENERS` by substituting `0.0.0.0` for the host. Convenient, but it hides the distinction between "where I bind" and "what I advertise" — the very distinction that causes the failure. The manifest sets both explicitly for legibility. Env-var naming rule for this image: uppercase the property, `.`→`_`, `_`→`__`, `-`→`___`, prefix `KAFKA_`. `CLUSTER_ID` is the exception: no prefix, and it is **mandatory** in KRaft mode.

### 6c. Why the single-replica overrides are mandatory

```yaml
- name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR                 # value: "1"
- name: KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR         # value: "1"
- name: KAFKA_TRANSACTION_STATE_LOG_MIN_ISR                    # value: "1"
- name: KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR # value: "1"
- name: KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR            # value: "1"
```

Kafka's internal topics default to **replication factor 3**, a sane production default. On a one-broker cluster that is unsatisfiable, so the broker cannot create `__consumer_offsets` — and `__consumer_offsets` is where committed offsets live. Without it, consumer groups don't work at all, which means **KEDA has nothing to read**. The failure surfaces late and looks like a client problem: the broker is happily `Running`, and your consumer hangs on join.

Two more entries are demo ergonomics rather than necessity: `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: "0"` (default 3000) removes a flat three-second pause from every first rebalance, so scaling looks responsive; `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` forces you to create `orders` explicitly with the partition count you want, instead of a typo silently producing a 1-partition topic that caps the whole lab at one consumer.

> [!warning] `emptyDir` means any pod restart wipes the cluster
> The volume is `emptyDir: {}` — deliberately, so there's no PVC to clean up and teardown is one command. The cost is that **restarting `kafka-0` destroys every message, every topic and every committed offset.** Editing the StatefulSet rolls the pod, so a config tweak silently resets your lab state.
> **Mitigation:** expect it, and re-create the topic after any broker restart (§7). If you want state to survive, swap `emptyDir` for a `volumeClaimTemplate` — kind ships a default StorageClass, so it works out of the box.

> [!note] Strimzi is the "production realism" alternative — and most tutorials for it are stale
> [Strimzi](https://strimzi.io/) 1.1.0 is the right choice for a *second*, production-shaped lab: real operator, real `Kafka` CRs, rolling upgrades. It costs about **1.1 GB** and ~105 s to Ready versus this StatefulSet's 366 MiB and ~15 s, and for learning KEDA every one of those megabytes is a distraction. If you go there, know that its API moved a lot recently and **most blog posts you'll find are wrong**: `kafka.strimzi.io/v1beta2` is **removed** (all CRDs serve only `v1` since 1.0.0), **ZooKeeper is gone** (KRaft only since 0.46.0 — `spec.zookeeper` doesn't exist), and a **`KafkaNodePool` is now mandatory** (a `Kafka` CR alone gives you zero pods and `No KafkaNodePools found`). `replicas`, `storage` and `resources` moved from `Kafka` to `KafkaNodePool`. The `strimzi.io/kraft: enabled` annotation you'll see everywhere is obsolete and silently ignored — its presence is a reliable signal the rest of the page is out of date.

> [!warning] Rejected approaches, so you don't waste an evening on them
> **The Bitnami Kafka Helm chart is a dead end** — since 2025-08-28 Bitnami moved versioned images to `docker.io/bitnamilegacy` (unmaintained) and the public catalog keeps only `latest`, so charts resolve but reference tags that no longer exist. **Kafka in Docker Compose beside the cluster is actively broken on kind** — pods can't resolve Docker container names, because CoreDNS forwards to the node's `resolv.conf`, which points at Docker's embedded DNS at `127.0.0.11`, unreachable from a containerd pod's netns. **Redpanda** ships arm64 and is a fine broker, but **KEDA's Sarama-based scaler hasn't been verified against it** — don't put an unverified scaler path on a teaching lab's critical path.

---

## 7. Create the `orders` topic with 12 partitions

The Kafka CLI scripts live at `/opt/kafka/bin/` inside the image, so you can run them in the broker pod and skip installing anything on your Mac.

```bash
kubectl -n lab exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic orders --partitions 12 --replication-factor 1
```

Expected: `Created topic orders.` Verify:

```bash
kubectl -n lab exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic orders
```

Expected — `PartitionCount: 12`, and twelve `Partition:` lines each with `Leader: 1`:

```
Topic: orders	TopicId: ...	PartitionCount: 12	ReplicationFactor: 1	Configs:
	Topic: orders	Partition: 0	Leader: 1	Replicas: 1	Isr: 1
	Topic: orders	Partition: 1	Leader: 1	Replicas: 1	Isr: 1
	... (through Partition: 11)
```

Note `--bootstrap-server localhost:9092` here: inside the broker's own pod, localhost is the broker. Everything *else* in the lab uses the FQDN.

**Why 12?** Because the partition count is the hard ceiling on useful consumers ([[02 - Kafka Consumer Groups, Partitions & Rebalancing]]), and 12 is composite with many divisors — **1, 2, 3, 4, 6, 12**. That matters in three places later:

- Scaling 1 → 2 → 3 consumers gives you the clean **12 → 6/6 → 4/4/4** split you'll watch in [[06 - LAB 2 — Your First Kafka ScaledObject]].
- `maxReplicaCount: 12` equals the partition count, so you can see the partition ceiling take effect ([[07 - The Kafka Scaler — Every Field & the Replica Math]]).
- `ensureEvenDistributionOfPartitions` snaps the replica count to *divisors* of the partition count. On 12 that's a comfortable ladder; on a prime count like 13 it's a cliff from 1 pod straight to 13. Picking 12, 24 or 48 for topics you intend to autoscale is a real production habit, not a lab convenience.

---

## 8. Install KEDA 2.20.1

```bash
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm install keda kedacore/keda --namespace keda --create-namespace --version 2.20.1
```

Expected: `NAME: keda … STATUS: deployed … REVISION: 1`.

Pin `--version` every time. KEDA's CRDs are still `v1alpha1` and the maintainers have said they'll stay there, so **breaking changes ship in minor releases** — an unpinned chart can quietly remove a field your ScaledObject uses.

### The health check that matters most

Run all four. Three of them are inventory; the fourth is the one that decides whether anything works.

```bash
kubectl -n keda get deploy
```

Expected — **3 deployments**, all `1/1`:

```
NAME                     READY   UP-TO-DATE   AVAILABLE   AGE
keda-admission-webhooks  1/1     1            1           60s
keda-metrics-apiserver   1/1     1            1           60s
keda-operator            1/1     1            1           60s
```

```bash
kubectl get crd | grep keda.sh
```

Expected — **6 CRDs**: `scaledobjects`, `scaledjobs`, `triggerauthentications`, `clustertriggerauthentications`, `cloudeventsources`, `clustercloudeventsources`.

```bash
kubectl get apiservice v1beta1.external.metrics.k8s.io
```

Expected — and this is the line to care about:

```
NAME                              SERVICE                        AVAILABLE   AGE
v1beta1.external.metrics.k8s.io   keda/keda-metrics-apiserver    True        62s
```

```bash
kubectl get --raw /apis/external.metrics.k8s.io/v1beta1 | head -c 200
```

Expected — an `APIResourceList` naming `externalmetrics`.

> [!important] If that APIService isn't `AVAILABLE=True`, nothing else will work — check it first, always
> This is the aggregation-layer registration through which the HPA reads KEDA's metrics. When it's `False`, KEDA's operator still looks healthy and your ScaledObject still reports `Ready`, but every HPA that depends on it shows `TARGETS: <unknown>` and never scales. Every "why isn't it scaling" debugging session should start here, before you look at Kafka, the trigger config or the consumer.
> **Mitigation:** if it's `False`, read the message column — usually `keda-metrics-apiserver` hasn't finished starting (wait 30 s), or something else already owns the API. **Only one `external.metrics.k8s.io` adapter can exist per cluster**, so if you have prometheus-adapter installed with external metrics enabled, one of them wins and the other is dead. Don't install both.

Nothing to enable for the aggregation layer itself — kind's kubeadm setup has it on, and the chart registers the APIService for you. Resource cost: default requests are `100m` CPU / `100Mi` memory × 3 pods ≈ **300 mCPU / 300 MiB**.

> [!warning] `helm uninstall keda` deletes the CRDs
> Removing the chart removes the CustomResourceDefinitions, and deleting a CRD cascades to **every custom resource of that kind in the cluster** — all your ScaledObjects, in every namespace, gone, along with the HPAs KEDA owned. Harmless here; genuinely destructive in a shared cluster.
> **Mitigation:** in a real cluster, treat `helm uninstall keda` as a change that removes autoscaling from every team on it, and check who else has ScaledObjects first.

---

## 9. Optional — kafbat-ui for a visual view

Not required, and it costs ~400 MiB. But seeing per-partition lag and live group membership in a browser, next to your consumers' rebalance logs, makes [[06 - LAB 2 — Your First Kafka ScaledObject]] click much faster.

```bash
cat <<'EOF' | kubectl apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-ui
  namespace: lab
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kafka-ui
  template:
    metadata:
      labels:
        app: kafka-ui
    spec:
      containers:
        - name: kafka-ui
          image: ghcr.io/kafbat/kafka-ui:v1.5.0
          ports:
            - containerPort: 8080
          env:
            - name: KAFKA_CLUSTERS_0_NAME
              value: "lab"
            - name: KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS
              value: "kafka-0.kafka.lab.svc.cluster.local:9092"
            - name: DYNAMIC_CONFIG_ENABLED
              value: "true"
---
apiVersion: v1
kind: Service
metadata:
  name: kafka-ui
  namespace: lab
spec:
  type: NodePort
  selector:
    app: kafka-ui
  ports:
    - port: 8080
      targetPort: 8080
      nodePort: 30080      # matches the kind extraPortMapping
EOF
kubectl -n lab rollout status deploy/kafka-ui
```

Then open **http://127.0.0.1:30080** — which works only because `lab/kind-cluster.yaml` mapped host 30080 to node 30080 at create time. Topics → `orders` shows 12 partitions; the **Consumers** view is the useful one, showing per-partition lag and which member owns which partition.

> [!warning] Gotcha — the tag needs its `v`, and the project you'll find first isn't the one you want
> `ghcr.io/kafbat/kafka-ui:1.5.0` fails with `MANIFEST_UNKNOWN`; the tag is **`v1.5.0`**. Separately, the widely-linked **`provectus/kafka-ui`** is stale — last commit **2024-07-26**. It isn't formally archived, so it still looks alive on GitHub, but it has had no maintenance in two years; **kafbat** is the community fork carrying the work forward.
> **Mitigation:** where an older tutorial says `provectus/kafka-ui`, substitute `ghcr.io/kafbat/kafka-ui:v1.5.0` — the env-var config format is compatible.

---

## 10. 🔴 Break it — advertise a name the broker can't resolve

This is failure mode #1 for local Kafka, and it's worth ten minutes because the symptom points away from the cause: **the connection succeeds, and then the client hangs.**

> [!note] This restarts the broker, which wipes `emptyDir`
> Your `orders` topic will be gone afterwards. That's fine — you re-create it in the fix step, and it reinforces §6's warning.

Point the advertised listener at a name that doesn't exist in DNS:

```bash
kubectl -n lab set env sts/kafka \
  KAFKA_ADVERTISED_LISTENERS='PLAINTEXT://kafka-broker.lab.svc.cluster.local:9092'
kubectl -n lab rollout status sts/kafka
```

The broker comes up **Ready** — `kubectl -n lab get pods -l app=kafka` shows `1/1 Running`, `0` restarts. Nothing suggests a problem.

Now connect a client from another pod — the same thing your consumers will do:

```bash
kubectl -n lab run kcheck --rm -it --restart=Never --image=apache/kafka:4.3.1 -- \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-0.kafka.lab.svc.cluster.local:9092 --list
```

**What you should see:** a long pause — roughly 60 seconds of nothing — then a timeout:

```
Error while executing topic command : Timed out waiting for a node assignment.
org.apache.kafka.common.errors.TimeoutException: Timed out waiting for a node assignment. Call: listTopics
```

Read that carefully. The client **reached** `kafka-0.kafka.lab.svc.cluster.local:9092` — the bootstrap hop worked, or you'd have got a connection refusal immediately. What failed is the second hop: the broker's metadata response said "I am `kafka-broker.lab.svc.cluster.local:9092`", the client tried to resolve that, and there is no such name. So it has a healthy TCP path to a broker it cannot address.

**The diagnostic:**

```bash
kubectl -n lab exec kafka-0 -- env | grep -E 'ADVERTISED|QUORUM'
```

```
KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka-broker.lab.svc.cluster.local:9092
KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka-0.kafka.lab.svc.cluster.local:9093
```

There it is: the advertised host is a name nothing resolves. (The same value also appears in `/opt/kafka/config/server.properties` — `kubectl -n lab exec kafka-0 -- grep advertised /opt/kafka/config/server.properties`.) Confirm the DNS side directly with `kubectl -n lab run dnscheck --rm -it --restart=Never --image=busybox:1.37 -- nslookup kafka-broker.lab.svc.cluster.local`, which should return `NXDOMAIN` — whereas `kafka-0.kafka.lab.svc.cluster.local` resolves, because the headless Service publishes it.

**A harsher variant, worth knowing about:** break `KAFKA_CONTROLLER_QUORUM_VOTERS` the same way instead, and the broker can't reach *itself* as controller. During verification that produced a repeating `UnknownHostException` in the broker's controller-request thread — visible in `kubectl -n lab logs kafka-0`, and much easier to diagnose precisely because the broker complains out loud.

### 🟢 Fix it

Put the resolvable FQDN back and re-create the topic:

```bash
kubectl -n lab set env sts/kafka \
  KAFKA_ADVERTISED_LISTENERS='PLAINTEXT://kafka-0.kafka.lab.svc.cluster.local:9092'
kubectl -n lab rollout status sts/kafka

kubectl -n lab exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic orders --partitions 12 --replication-factor 1
```

Re-run the `kcheck` pod above. It should now return **immediately**, printing `orders`.

**The rule to carry away:** advertise a name that resolves *both* from client pods and from inside the broker's own pod. In Kubernetes, the headless-Service FQDN is the one name that satisfies both. And when a Kafka client "hangs" rather than erroring, suspect advertised listeners before you suspect the network.

> [!warning] Gotcha — the internal-listener port-forward trap
> The obvious next move is `kubectl -n lab port-forward kafka-0 9092:9092` and then pointing a Mac-side client at `localhost:9092`. It will hang, for exactly the reason you just watched: bootstrap over the tunnel succeeds, the broker replies "I am `kafka-0.kafka.lab.svc.cluster.local:9092`", and your Mac cannot resolve a cluster-internal name.
> **Mitigation:** run CLI tools and UIs **inside** the cluster (`kubectl exec`, or the kafbat-ui pod) — that's what this lab does throughout. If you truly need a host-side client, add a **second listener** advertising `localhost:<hostPort>` and map that port through kind `extraPortMappings` at create time. Don't try to port-forward the internal listener.

---

## 11. Checkpoint

Five commands. If every one gives the expected answer, the stack is ready for [[06 - LAB 2 — Your First Kafka ScaledObject]].

```bash
# 1. three nodes, all Ready, all v1.35.5 — inside KEDA 2.20's tested window
kubectl get nodes

# 2. the APIService that everything depends on — must say True
kubectl get apiservice v1beta1.external.metrics.k8s.io

# 3. KEDA's three deployments, all 1/1
kubectl -n keda get deploy

# 4. the broker Running, and (optionally) the UI
kubectl -n lab get pods

# 5. the topic, with PartitionCount: 12
kubectl -n lab exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic orders
```

And a real end-to-end proof that a client can actually use the broker — produce three messages and read them back:

```bash
kubectl -n lab exec kafka-0 -- bash -c \
  'echo -e "a\nb\nc" | /opt/kafka/bin/kafka-console-producer.sh \
     --bootstrap-server kafka-0.kafka.lab.svc.cluster.local:9092 --topic orders'

kubectl -n lab exec kafka-0 -- /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-0.kafka.lab.svc.cluster.local:9092 \
  --topic orders --from-beginning --max-messages 3
```

Expected: `a`, `b`, `c`, then `Processed a total of 3 messages`. Note this uses the **FQDN**, not `localhost` — so it exercises the advertised-listener path you just fixed. Finally, `kubectl top pods -A --sort-by=memory | head` should show Kafka around **350–400 MiB** and KEDA's three pods near their 100 MiB requests.

---

## 12. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `apiservice v1beta1.external.metrics.k8s.io` shows `AVAILABLE=False` | `keda-metrics-apiserver` still starting, or another external-metrics adapter (prometheus-adapter) owns the API | Wait 30 s, then `kubectl -n keda logs deploy/keda-metrics-apiserver`. **Only one `external.metrics.k8s.io` adapter per cluster** — remove the other one. Until this is `True`, no HPA can read KEDA's metrics and nothing scales |
| Client connects, then hangs; `TimeoutException: Timed out waiting for a node assignment` | Advertised listener names a host the client can't resolve | `kubectl -n lab exec kafka-0 -- env \| grep ADVERTISED`. Advertise the headless-Service FQDN `kafka-0.kafka.lab.svc.cluster.local:9092` (§10) |
| Broker logs loop on `UnknownHostException` in a controller thread | `KAFKA_CONTROLLER_QUORUM_VOTERS` names a host that doesn't resolve **inside the broker's own pod** | Same FQDN, port 9093 |
| Broker `Running` but consumer groups never form | Missing single-replica RF overrides → `__consumer_offsets` can't be created (defaults to RF=3) | Use `lab/kafka.yaml` unmodified; check `kubectl -n lab logs kafka-0` for replication-factor errors |
| `metrics-server` in CrashLoopBackOff or stuck `0/1 Running`; logs mention `x509` / `IP SANs` | kind's kubelet serving certs are self-signed and v0.9.0 ships without `--kubelet-insecure-tls` | Apply the patch in §5 |
| `kubectl top` says `error: Metrics API not available` | metrics-server not Ready, or fewer than ~30 s since it started | Fix the above, then wait one scrape interval (`--metric-resolution=15s`) |
| Can't reach a Service from macOS at the node's IP | **kind node IPs are not routable from macOS** — Docker Desktop runs the engine in a VM | Only two ways in: `extraPortMappings` declared at **cluster create time**, or `kubectl port-forward`. Nothing else works, and no amount of `docker network inspect` will help |
| Added a nodePort and it isn't reachable on the host | `extraPortMappings` is create-time only | Recreate the cluster with the mapping, or use `kubectl port-forward` |
| `kubectl get nodes` shows `v1.36.1` | The `--image` pin didn't apply — kind v0.32.0's default is 1.36.1 | `kind delete cluster --name keda-lab` and recreate with the full `--image kindest/node:v1.35.5@sha256:...` |
| Odd, undiagnosable KEDA behaviour | Kubernetes outside KEDA 2.20's tested 1.33–1.35 window | Check the node version first. Docker Desktop's built-in cluster is always outside it and can't be pinned |
| Mac-side client on a `port-forward` to 9092 hangs | Bootstrap returns the cluster-internal FQDN, unresolvable on your Mac | Run clients in-cluster, or add a second listener advertising `localhost:<hostPort>` |
| Pods `Pending` / OOMKilled, everything slow | Docker Desktop under 6 GB | Raise it to 6–8 GB and 4–6 CPUs, then recreate the cluster |
| `ErrImagePull` on `kafbat/kafka-ui:1.5.0` | Tag needs the `v` prefix | `ghcr.io/kafbat/kafka-ui:v1.5.0` |

---

## 13. Teardown

```bash
kind delete cluster --name keda-lab
docker system prune -f          # reclaim node and image layers
kind get clusters               # should print nothing
```

`kind delete` removes the node containers and the kubeconfig context in one step; `emptyDir` means there are no volumes to chase. If a ScaledObject ever hangs on delete (a stuck finalizer, usually after the operator was removed first):

```bash
kubectl patch scaledobject <name> -n lab -p '{"metadata":{"finalizers":null}}' --type=merge
```

Recreating the whole stack from scratch takes about 3 minutes once the images are in your local Docker cache — so deleting the cluster between sessions is cheap, and a good habit.

---

## Questions you should be able to answer

- Why pin the kind node image instead of taking the default? *(KEDA 2.20 is tested only against Kubernetes 1.33–1.35; kind v0.32.0 defaults to 1.36.1.)*
- Why is Docker Desktop's built-in Kubernetes unsuitable here? *(Its version can't be pinned — it's fixed at whatever the Docker Desktop release ships, currently 1.36.1.)*
- Why does Kafka need a **headless** Service rather than a normal ClusterIP one? *(Clients must reach a specific broker; headless gives stable per-pod DNS, which is what the advertised listener can name.)*
- A client connects and then hangs. First thing you check? *(`KAFKA_ADVERTISED_LISTENERS` — bootstrap works, then the client can't resolve the advertised name.)*
- Why must the advertised host resolve from inside the broker's *own* pod? *(In KRaft the node reaches itself as controller through `KAFKA_CONTROLLER_QUORUM_VOTERS`.)*
- What breaks if you drop the replication-factor overrides on a single-broker cluster? *(`__consumer_offsets` can't be created at RF=3, so no consumer group works — and KEDA has no committed offsets to read.)*
- Which single command tells you whether KEDA can serve metrics at all? *(`kubectl get apiservice v1beta1.external.metrics.k8s.io` — must be `AVAILABLE=True`.)*
- Why 12 partitions and not 10 or 13? *(Many divisors: clean 12 → 6/6 → 4/4/4 splits, and `ensureEvenDistributionOfPartitions` on a prime count jumps 1 → 13.)*
- You want to expose one more nodePort on the existing cluster. What does that cost? *(A full recreate — `extraPortMappings` is create-time only. Or use `port-forward`.)*
- Does KEDA need metrics-server? *(No. Only `kubectl top` and CPU/memory triggers do.)*

---

## Sources

- [kind — quick start (cluster config, `extraPortMappings`, loading images)](https://kind.sigs.k8s.io/docs/user/quick-start/) · [releases and node images](https://github.com/kubernetes-sigs/kind/releases)
- [KEDA — deploying with Helm](https://keda.sh/docs/2.20/deploy/) · [Kubernetes compatibility matrix](https://keda.sh/docs/2.20/operate/cluster/) · [Helm charts](https://github.com/kedacore/charts)
- [metrics-server — releases and manifests](https://github.com/kubernetes-sigs/metrics-server/releases) · [requirements, incl. `--kubelet-insecure-tls`](https://github.com/kubernetes-sigs/metrics-server#requirements)
- [Apache Kafka — official Docker image](https://hub.docker.com/r/apache/kafka)
- [Apache Kafka 4.3.1 — single-node KRaft compose example the manifest derives from](https://github.com/apache/kafka/blob/4.3.1/docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml)
- [Apache Kafka 4.3 — broker configuration (`advertised.listeners`, `controller.quorum.voters`)](https://kafka.apache.org/43/configuration/broker-configs/)
- [Kubernetes — StatefulSets and stable network identity](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/) · [headless Services](https://kubernetes.io/docs/concepts/services-networking/service/#headless-services) · [the aggregation layer](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/apiserver-aggregation/)
- [kafbat-ui — the maintained Kafka UI](https://github.com/kafbat/kafka-ui)
- [Strimzi — documentation (v1 API, KRaft, KafkaNodePool)](https://strimzi.io/documentation/)
