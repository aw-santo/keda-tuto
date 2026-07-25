---
title: 12 - Authentication — TriggerAuthentication, SASL, TLS & MSK IAM
tags:
  - keda
  - kafka
  - authentication
  - security
  - kubernetes
  - senior
aliases:
  - TriggerAuthentication
  - ClusterTriggerAuthentication
  - KEDA SASL
  - KEDA MSK IAM
status: ready
created: 2026-07-25
---

# Authentication — TriggerAuthentication, SASL, TLS & MSK IAM

> [!abstract] Scope
> How KEDA gets credentials for your event source, using Kafka as the worked example. You'll build a complete SASL_SSL / SCRAM-SHA-512 setup — Secret, `TriggerAuthentication`, ScaledObject — and understand the five places credentials can come from and when each is the right one. The idea to take away: **credentials never belong in a trigger's `metadata`**, and the CRD that holds them instead is deliberately a separate object with its own scope, its own blast radius, and its own RBAC consequences for the KEDA operator. Assumes the trigger fields from [[07 - The Kafka Scaler — Every Field & the Replica Math]].

Related: [[00 - KEDA Index]], [[04 - The ScaledObject CRD, End to End]], [[07 - The Kafka Scaler — Every Field & the Replica Math]], [[11 - Multiple Triggers, Scaling Modifiers & Fallback]], [[13 - ScaledJob — When a Pod per Message Beats a Consumer Pool]], [[14 - Observability & Debugging — Why Isn't It Scaling]], [[15 - Production Operations]]

> [!warning] Written against KEDA v2.20 · Kubernetes 1.33–1.35 · Apache Kafka 4.3
> `TriggerAuthentication` is `v1alpha1` like every other KEDA CRD, and new authentication providers arrive most releases. Field names below are current for v2.20 — check the [authentication concepts page](https://keda.sh/docs/2.20/concepts/authentication/) for whatever version you run.

---

## 1. Why authentication needs its own CRD

A trigger's `metadata` block is plain, unencrypted strings sitting inside a custom resource:

```yaml
triggers:
- type: kafka
  metadata:
    bootstrapServers: broker-1:9093
    consumerGroup: orders-workers
    topic: orders
    lagThreshold: "500"
```

That is fine for a topic name. It is completely wrong for a password. Anyone who can run `kubectl get scaledobject -o yaml` — and that's a much wider group than the people who can read Secrets, because `ScaledObject` is usually treated as ordinary application config — would read the credential in clear text. It would land in your Git repo, in your Helm values, in `kubectl describe` output, and in every log line that dumps the resource.

So KEDA splits the two concerns:

- **`metadata`** — non-secret configuration. Which broker, which topic, which threshold.
- **`authParams`** — the resolved secret values. You never write these directly. KEDA assembles them at poll time from whatever the referenced `TriggerAuthentication` points at.

The trigger connects the two with `authenticationRef`:

```yaml
  triggers:
  - type: kafka
    metadata:
      bootstrapServers: broker-1:9093
      consumerGroup: orders-workers
      topic: orders
      lagThreshold: "500"
    authenticationRef:
      name: kafka-scram-auth        # a TriggerAuthentication in THIS namespace
```

Everything the scaler needs but shouldn't see in the CR — `username`, `password`, `ca`, `cert`, `key` — arrives through that reference.

> [!tip] The mental model
> A `TriggerAuthentication` is a **mapping**, not a store. It says "the parameter the scaler calls `password` lives in key `password` of Secret `keda-kafka-secrets`." The value itself stays wherever it already was: a Kubernetes Secret, a ConfigMap, Vault, a cloud secret manager, or nowhere at all if you use pod identity.

---

## 2. `TriggerAuthentication` vs `ClusterTriggerAuthentication`

The two objects have an **identical spec**. The only difference is scope.

| | `TriggerAuthentication` | `ClusterTriggerAuthentication` |
|---|---|---|
| Scope | namespaced | cluster-wide |
| Short names | `ta`, `triggerauth` | `cta`, `clustertriggerauth` |
| Referenced by | ScaledObjects **in the same namespace** | ScaledObjects in **any** namespace |
| How to reference | `authenticationRef: {name: x}` | `authenticationRef: {name: x, kind: ClusterTriggerAuthentication}` |

The cluster-scoped variant exists for a real reason: if forty namespaces all consume from the same Kafka cluster with the same platform credential, duplicating forty `TriggerAuthentication` objects and forty Secrets is genuinely worse — forty places to rotate, forty places to get wrong.

But be honest about what you're trading away.

> [!warning] Gotcha — a `ClusterTriggerAuthentication` is usable by anyone who can create a ScaledObject
> There is no allow-list of namespaces on a `ClusterTriggerAuthentication`. Any tenant who can create a `ScaledObject` in their own namespace can point `authenticationRef` at it, and KEDA will happily use those credentials on their behalf. They can't *read* the credential — but they can aim it at a `consumerGroup` and `topic` of their choosing and learn things about a Kafka cluster they were never granted access to. In a multi-tenant cluster that is a real privilege escalation surface, and it's invisible in the tenant's own RBAC.
> **Mitigation:** default to namespaced `TriggerAuthentication`. Reserve `ClusterTriggerAuthentication` for genuinely shared platform infrastructure where every namespace is already entitled to the same access — and gate `ScaledObject` creation itself with a policy controller if your tenants are untrusted.

> [!tip] Committed recommendation
> **Namespaced by default.** One `TriggerAuthentication` per namespace, next to the Secret it references, owned by the same team that owns the workload. Reach for cluster-scoped only when the credential is a platform credential and duplicating it across namespaces would be the bigger risk. (There is one forced exception — see §8, where restricting the operator's Secret access makes namespaced Secrets unusable.)

---

## 3. Where credentials can come from

A `TriggerAuthentication` can resolve parameters from several sources, and they can be mixed in one object.

| Source | Field | Choose it when |
|---|---|---|
| Kubernetes Secret | `secretTargetRef` | **the common case.** You already manage the credential as a Secret, or your GitOps tooling syncs one in |
| ConfigMap | `configMapTargetRef` | the value is config, not a credential — a CA bundle, a token endpoint URL, a realm name |
| Scale-target env | `env` | the consumer container already receives the credential as an env var and you want exactly one source of truth |
| Pod identity | `podIdentity` | **preferred wherever the cloud supports it** — AWS IAM roles, Azure Workload Identity, GCP Workload Identity. No stored credential at all |
| HashiCorp Vault | `hashiCorpVault` | Vault is already your system of record and you want short-lived, dynamically issued credentials |
| AWS Secrets Manager | `awsSecretManager` | on AWS, credential lifecycle is already managed there |
| Azure Key Vault | `azureKeyVault` | on Azure, ditto |
| GCP Secret Manager | `gcpSecretManager` | on GCP, ditto |
| ServiceAccount token | `boundServiceAccountToken` | the event source accepts a projected Kubernetes ServiceAccount token directly |
| OAuth2 client credentials | `oauth2` | the source authenticates with a client-credentials flow and KEDA should fetch the token itself |

Three notes that matter in practice:

**`env` looks convenient and usually isn't.** It reads the environment of a container in the scale target. That couples your autoscaler's ability to authenticate to the shape of your Deployment's pod spec, and it silently breaks when the workload is scaled to zero in some configurations. Use it when you genuinely want one source of truth for app and scaler; otherwise use `secretTargetRef`.

**Pod identity is the strategic answer.** With `podIdentity`, KEDA authenticates as its own workload identity and there is no password anywhere — nothing to rotate, nothing to leak, nothing in Git. It also sidesteps most of §8's blast-radius problem, because there's no Secret for the operator to read.

```yaml
apiVersion: keda.sh/v1alpha1
kind: TriggerAuthentication
metadata:
  name: kafka-msk-auth
  namespace: orders
spec:
  podIdentity:
    provider: aws                                          # aws | aws-eks | azure-workload | gcp | none
    roleArn: arn:aws:iam::123456789012:role/keda-msk-reader
```

**External secret stores are not a free upgrade.** Pointing a `TriggerAuthentication` at Vault or a cloud secret manager means KEDA now needs credentials *for that store*, and that bootstrap credential is itself usually… a Kubernetes Secret. The win is real (short-lived secrets, central audit, one rotation path) but it moves the trust anchor rather than removing it. Pod identity plus a cloud secret manager is the combination that actually removes long-lived material.

---

## 4. A complete Kafka SASL_SSL / SCRAM-SHA-512 setup

Three objects. Apply them in this order.

**Step 1 — the Secret.** Note `stringData`, not `data`.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: keda-kafka-secrets
  namespace: orders
type: Opaque
stringData:                       # stringData accepts plain text; data requires base64
  sasl: "scram_sha512"
  tls: "enable"
  username: "kafka-consumer"
  password: "s3cr3t"
  ca: |
    -----BEGIN CERTIFICATE-----
    ...your private CA...
    -----END CERTIFICATE-----
```

> [!warning] Gotcha — KEDA's own docs show these values under `data:` in plain text
> The Kafka scaler documentation's SASL example writes `data:` with `sasl: "plaintext"` and `password: "admin"` unencoded. A Kubernetes Secret's `data` field **must** be base64. Copy that example verbatim and the API server either rejects it or — worse, when the value happens to be valid base64 — accepts it and decodes it into garbage. The scaler then fails to authenticate and the error message points at Kafka, not at your YAML.
> **Mitigation:** always use `stringData` and let Kubernetes do the encoding. If you must use `data`, encode every value: `printf '%s' 'scram_sha512' | base64`. Verify with `kubectl get secret keda-kafka-secrets -n orders -o jsonpath='{.data.sasl}' | base64 -d` and check you get the string you expect.

**Step 2 — the `TriggerAuthentication`**, mapping each scaler parameter to a Secret key. The key and the parameter don't have to share a name; here they do, which is the sane convention.

```yaml
apiVersion: keda.sh/v1alpha1
kind: TriggerAuthentication
metadata:
  name: kafka-scram-auth
  namespace: orders                # MUST match the ScaledObject's namespace
spec:
  secretTargetRef:
  - parameter: sasl                # the name the scaler asks for
    name: keda-kafka-secrets       # the Secret
    key: sasl                      # the key inside it
  - parameter: tls
    name: keda-kafka-secrets
    key: tls
  - parameter: username
    name: keda-kafka-secrets
    key: username
  - parameter: password
    name: keda-kafka-secrets
    key: password
  - parameter: ca
    name: keda-kafka-secrets
    key: ca
```

**Step 3 — the ScaledObject** referencing it. Nothing secret appears here.

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: orders-consumer
  namespace: orders
spec:
  scaleTargetRef:
    name: orders-consumer
  pollingInterval: 30
  minReplicaCount: 0
  maxReplicaCount: 12
  triggers:
  - type: kafka                              # the stable scaler
    metadata:
      bootstrapServers: broker-1:9093,broker-2:9093
      consumerGroup: orders-processor
      topic: orders
      lagThreshold: "500"
      activationLagThreshold: "20"
    authenticationRef:
      name: kafka-scram-auth
```

**Verify it worked** — the `TriggerAuthentication` records who is using it, and the ScaledObject reports which auth types resolved:

```bash
kubectl get triggerauthentication kafka-scram-auth -n orders -o yaml | grep -A3 status
kubectl get scaledobject orders-consumer -n orders \
  -o jsonpath='{.status.authenticationsTypes}{"\n"}'
kubectl get scaledobject orders-consumer -n orders   # READY / ACTIVE columns
```

If `READY` is `False`, the reason is on the object's conditions and the detail is in the operator log — see [[14 - Observability & Debugging — Why Isn't It Scaling]].

---

## 5. The Kafka SASL mechanism support matrix

KEDA has two Kafka triggers ([[07 - The Kafka Scaler — Every Field & the Replica Math]], §2) and **they do not support the same authentication mechanisms.** This is the single biggest reason to pick one over the other.

| Mechanism | `type: kafka` (stable) | `type: apache-kafka` (experimental) |
|---|---|---|
| `none` | ✓ | ✓ |
| `plaintext` (SASL/PLAIN) | ✓ | ✓ |
| `scram_sha256` | ✓ | ✓ |
| `scram_sha512` | ✓ | ✓ |
| `oauthbearer` | ✓ | ✗ rejected — "not implemented yet" |
| `gssapi` (Kerberos) | ✓ | ⚠️ **accepted then fails at connect time** |
| AWS MSK IAM | ✓ via `sasl: oauthbearer` + `saslTokenProvider: aws_msk_iam` | ✓ via `sasl: aws_msk_iam` |

For Kerberos on the stable scaler you also supply `realm`, `kerberosConfig` (the `krb5.conf` contents) and either `password` or `keytab`; `kerberosServiceName` defaults to `kafka` and `kerberosDisableFAST` exists for brokers that need FAST negotiation off. For OAuth you supply `oauthTokenEndpointUri` and `scopes`, plus `oauthExtensions` if your provider needs custom SASL extensions.

> [!warning] Gotcha — Kerberos on `apache-kafka` passes validation and then fails at runtime
> The experimental scaler's schema *lists* `gssapi` as a valid value, so `kubectl apply` succeeds and the admission webhook is happy. But it has no Kerberos implementation and no `keytab`, `realm` or `kerberosConfig` parameters at all. What you get is a ScaledObject that looks correctly configured, reports `Ready`, and then errors on every single poll with an unhelpful "sasl type" message. The trigger never produces a metric, so the HPA has no data and the workload sits wherever it happens to be.
> **Mitigation:** **anything needing Kerberos or OAuth must use `type: kafka`.** This is not a version-dependent quirk to wait out — the experimental scaler's underlying client library doesn't implement these mechanisms ([design issue #4692](https://github.com/kedacore/keda/issues/4692) says so plainly). Validation passing is not evidence that a mechanism works; check the matrix.

---

## 6. TLS

TLS is configured separately from SASL, and for a typical managed Kafka you need both — SASL for *who you are*, TLS for *encryption in transit*. That combination is what brokers call `SASL_SSL`.

| Parameter | Where | What it does |
|---|---|---|
| `tls` | metadata **or** authParams | `enable` turns TLS on. Default `disable` |
| `ca` | authParams | CA certificate that signed the broker certs. Needed for a private CA |
| `cert` | authParams | client certificate, for mutual TLS |
| `key` | authParams | the matching private key |
| `keyPassword` | authParams | passphrase, if `key` is encrypted |
| `unsafeSsl` | metadata (`kafka` only) | skips certificate verification entirely |

Two rules:

**`cert` and `key` must be set together.** KEDA rejects one without the other, which is the right behaviour — half an mTLS config would otherwise fail at connect time with a much vaguer error.

**`ca` is what you need for a private CA**, and it's the common case for self-managed Kafka. Supply the CA certificate as a Secret key and map it as a parameter, exactly as in §4. There's also a cluster-wide alternative: the KEDA operator can be given a directory of trusted CAs at install time, which applies to every scaler at once — see [[15 - Production Operations]]. Use that when *every* scaler in the cluster talks to endpoints signed by your internal CA; use `ca` in the `TriggerAuthentication` when it's specific to one event source.

> [!warning] Gotcha — `unsafeSsl: "true"` is a debugging tool that ends up in production
> It disables certificate verification, which means KEDA will accept **any** certificate from anything answering on that address. It makes a TLS error disappear instantly, which is exactly why it survives from the debugging session into the merged manifest. The KEDA docs themselves note it "isn't ideal because it allows any certificate, which is not secure."
> **Mitigation:** never set it outside a lab. When you hit a certificate error, the fix is supplying `ca` — or registering your CA with the operator cluster-wide. Add a policy check that rejects `unsafeSsl: "true"` in any non-dev namespace; it's a one-line policy and it catches this permanently.

---

## 7. AWS MSK IAM

On Amazon MSK you can skip passwords entirely and authenticate with an IAM role. This is the best-case configuration: no credential exists to leak.

```yaml
apiVersion: keda.sh/v1alpha1
kind: TriggerAuthentication
metadata:
  name: kafka-msk-auth
  namespace: orders
spec:
  podIdentity:
    provider: aws
    roleArn: arn:aws:iam::123456789012:role/keda-msk-reader
---
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: orders-consumer
  namespace: orders
spec:
  scaleTargetRef:
    name: orders-consumer
  maxReplicaCount: 12
  triggers:
  - type: kafka
    metadata:
      bootstrapServers: b-1.mycluster.abc123.c2.kafka.eu-west-1.amazonaws.com:9098
      consumerGroup: orders-processor
      topic: orders
      lagThreshold: "500"
      awsRegion: eu-west-1                   # REQUIRED for MSK IAM
      tls: enable                            # REQUIRED for MSK IAM
      sasl: oauthbearer                      # on type: kafka
      saslTokenProvider: aws_msk_iam
    authenticationRef:
      name: kafka-msk-auth
```

The role needs read permission on the cluster, the topic and the consumer group — MSK IAM authorises at that granularity, so a role scoped to exactly the topic and group KEDA measures is both possible and correct. Credential-based authentication (`awsAccessKeyID` / `awsSecretAccessKey` in a Secret) also works, and `awsRoleArn` can be supplied as an auth parameter instead of via `podIdentity` — but both reintroduce a stored credential for no benefit on EKS.

> [!warning] Gotcha — MSK IAM silently requires both a region and TLS
> KEDA validates that MSK IAM has `awsRegion` **and** `tls: enable`. Miss either and the ScaledObject is rejected or fails to authenticate, and neither error obviously says "you forgot the region." The bootstrap address embeds the region, which makes `awsRegion` feel redundant — it isn't.
> **Mitigation:** treat `awsRegion` + `tls: enable` + `saslTokenProvider: aws_msk_iam` as one indivisible block. Also note MSK IAM listens on a **different port** (9098) than SASL_SSL (9096) or TLS (9094) — pointing IAM auth at the SASL_SSL port produces a connection error that looks like a network problem.

---

## 8. The security consideration nobody mentions until the audit

Here's the part that matters in a shared cluster. To resolve a `TriggerAuthentication` that uses `secretTargetRef`, **the KEDA operator must read that Secret** — in the workload's namespace, not KEDA's. By default the KEDA install grants exactly that: a ClusterRole with `get`/`list`/`watch` on `secrets`.

Read that again. A default KEDA install can read **every Secret in every namespace**. Not because KEDA is careless — it's the only way a central controller can resolve a namespaced credential reference — but it is a fact your security review will find, and it is worth knowing before they do rather than after.

KEDA offers a switch. Set the environment variable `KEDA_RESTRICT_SECRET_ACCESS` to `true` on **both** the operator and the metrics server, and KEDA stops expecting cluster-wide Secret access, letting you omit `secrets` from the ClusterRole. Via Helm:

```yaml
# values.yaml
permissions:
  operator:
    restrict:
      secret: true
  metricServer:
    restrict:
      secret: true
```

> [!important] The trade-off is severe, and it's the reason most clusters leave this off
> With Secret access restricted, KEDA can only read Secrets in **its own namespace**. That means **a namespaced `TriggerAuthentication` using `secretTargetRef` stops working entirely** — the docs put it bluntly: it "will disallow `TriggerAuthentication` to be used for your triggers if the `TriggerAuthentication` is using secrets. You can, however, still use `ClusterTriggerAuthentication`." So the setting forces you into the pattern §2 told you to avoid: every credential becomes a Secret in the KEDA namespace behind a `ClusterTriggerAuthentication`, usable by any tenant who can write a ScaledObject.
> You've traded "one controller can read all Secrets" for "all credentials live in one namespace and any tenant can reference them." Which is better depends entirely on your threat model — but it is a trade, not a fix.

**What actually resolves this** is removing the Secrets from the equation:

| Approach | Effect on the blast radius | Choose it when |
|---|---|---|
| Default (broad Secret read) | operator can read all Secrets cluster-wide | single-tenant cluster, or you trust the operator as platform infrastructure |
| `KEDA_RESTRICT_SECRET_ACCESS: true` | no cross-namespace Secret read, but credentials centralise in the KEDA namespace and become tenant-referenceable | a hard audit requirement forbids cluster-wide Secret read, and tenants are trusted |
| **Pod identity** | no Secret to read at all | the cloud supports it — **the recommended target state** |
| External secret store + pod identity | KEDA authenticates to the store with a workload identity; no long-lived material anywhere | you already run Vault or a cloud secret manager |
| `watchNamespace` scoping | operator only sees the namespaces you list, Secrets included | strong per-tenant isolation; costs you one KEDA install per scope |

Operational hygiene either way: KEDA's ServiceAccounts are separate per component (`keda-operator`, `keda-metrics-server`, `keda-webhook`) — review the *generated* Role and ClusterRole after any Helm customisation, not the values you passed. A tool like `kubectl-who-can` answers "who can read secrets in this namespace" in one command. Namespace scoping, HA limits and the rest of the operator's install surface are in [[15 - Production Operations]].

---

## 9. The gotchas that cause real incidents

Alongside §4's base64 trap, §5's Kerberos trap and §6's `unsafeSsl` trap:

> [!warning] Credentials in `metadata` instead of `authParams`
> Most Kafka auth parameters *look* like they could go in `metadata`, and `sasl` and `tls` genuinely can. `username` and `password` cannot — there is no `metadata` field for them, and there was never meant to be. The failure isn't a rejection; it's that the unrecognised keys are ignored, the scaler connects without credentials, and you get an authentication error from Kafka that looks like a broker-side problem.
> **Mitigation:** the rule is mechanical. Anything you wouldn't paste into a public Git repo goes in a Secret behind a `TriggerAuthentication`. `bootstrapServers`, `topic`, `consumerGroup`, `lagThreshold`, `awsRegion` in `metadata`; `username`, `password`, `ca`, `cert`, `key`, `keytab` never.

> [!warning] A `TriggerAuthentication` in the wrong namespace fails with a "not found"
> `authenticationRef` on a namespaced `TriggerAuthentication` resolves **only within the ScaledObject's own namespace**. Put the auth object in `keda` (a natural instinct — that's where KEDA lives) and reference it from `orders`, and KEDA reports it as missing. The error says the object doesn't exist, which sends people looking for a typo in a name that is spelled perfectly.
> **Mitigation:** Secret, `TriggerAuthentication` and `ScaledObject` in the same namespace, always. If you genuinely need one auth object across namespaces, that's what `ClusterTriggerAuthentication` is for — and remember to add `kind: ClusterTriggerAuthentication` to the `authenticationRef`, because without it KEDA looks for a namespaced object of that name and reports it missing.

> [!warning] Setting `sasl` or `tls` in both places behaves differently on each scaler
> `sasl` and `tls` may be specified in the ScaledObject's `metadata` *or* in the `TriggerAuthentication`. Setting both is where the two Kafka scalers diverge: the **stable** `kafka` scaler documents that **the ScaledObject wins**, silently overriding the auth object. The **experimental** `apache-kafka` scaler **hard-errors**: "unable to set 'sasl' in both ScaledObject and TriggerAuthentication together." So the same manifest works on one scaler and refuses to start on the other — and the silent-override case is worse, because a stale `sasl: plaintext` in the ScaledObject can quietly defeat the `scram_sha512` you carefully put in the Secret.
> **Mitigation:** pick one home and be consistent. **Put `sasl` and `tls` in the `TriggerAuthentication`** with the credentials they belong to, and leave them out of `metadata` entirely — that config is then portable across both scalers and there's one place to look.

> [!warning] Rotating a Secret doesn't take effect when you expect
> KEDA caches the built scaler — including its resolved credentials and open broker connection — and rebuilds it when the ScaledObject changes or when the scaler errors. So updating a Secret in place does **not** immediately re-resolve it. During a rotation with a grace period this is invisible; when the old credential is revoked immediately, the trigger starts failing and the workload freezes at its current replica count until the scaler is rebuilt.
> **Mitigation:** don't rely on implicit pickup. After rotating, force a rebuild by changing the ScaledObject — bumping a harmless annotation is enough (`kubectl annotate scaledobject orders-consumer -n orders rotated-at="$(date -u +%FT%TZ)" --overwrite`). Better, rotate with an overlap window so both credentials are valid, and best of all use pod identity so there is nothing to rotate. Pair either with `spec.fallback` ([[11 - Multiple Triggers, Scaling Modifiers & Fallback]]) so an auth failure resolves to a defined replica count instead of a frozen one.

---

## 10. The rules worth internalising

- Credentials go in a Secret behind a `TriggerAuthentication`. Never in `metadata`.
- Namespaced `TriggerAuthentication` by default. `ClusterTriggerAuthentication` only for shared platform credentials — and remember it's referenceable by any tenant.
- Secret, auth object and ScaledObject live in the same namespace.
- Use `stringData`, not `data`. The official Kafka example is wrong about this.
- Put `sasl` and `tls` in the `TriggerAuthentication` only — never in both places.
- Kerberos or OAuth means `type: kafka`. The experimental scaler validates them and then fails.
- `cert` and `key` together, or neither. `ca` for a private CA. `unsafeSsl` never.
- MSK IAM needs `awsRegion` **and** `tls: enable`, on port 9098.
- Prefer pod identity wherever the cloud offers it — it deletes the whole rotation and blast-radius problem.
- Know that a default KEDA install can read every Secret in the cluster, and know what restricting it costs before you propose it.

---

## 11. Questions you should be able to answer

- Why can't a Kafka password go in the trigger's `metadata`? *(It's plain text in the CR — readable by anyone who can read the ScaledObject, and it ends up in Git.)*
- What's the real risk of a `ClusterTriggerAuthentication`? *(Any namespace that can create a ScaledObject can reference it and use those credentials, without being able to read them.)*
- Your Secret has `data: {password: "s3cr3t"}` and auth fails. Why? *(`data` requires base64; use `stringData`. KEDA's own docs get this wrong.)*
- You configured `sasl: gssapi` on `type: apache-kafka`, it applied cleanly, and every poll errors. What happened? *(The value is in the schema but Kerberos isn't implemented. Switch to `type: kafka`.)*
- Which mechanisms force you onto the stable scaler? *(OAuth/`oauthbearer` and Kerberos/`gssapi`.)*
- What does MSK IAM require beyond the role? *(`awsRegion` and `tls: enable` — and port 9098.)*
- What Secret permissions does the KEDA operator have by default, and what breaks if you restrict them? *(Cluster-wide Secret read; restricting it disables namespaced `TriggerAuthentication` with `secretTargetRef`, leaving only `ClusterTriggerAuthentication`.)*
- You rotated the Secret and revoked the old credential; scaling froze. Why? *(KEDA had cached the scaler with the old credentials; force a rebuild by touching the ScaledObject, and use `fallback`.)*
- What single change removes most of this module's problems? *(Pod identity — no stored credential to rotate, leak, or grant read access to.)*

---

## 12. Sources

- [KEDA — Authentication concepts (`TriggerAuthentication`, `ClusterTriggerAuthentication`)](https://keda.sh/docs/2.20/concepts/authentication/)
- [KEDA — Apache Kafka scaler, Authentication Parameters (documents `type: kafka`)](https://keda.sh/docs/2.20/scalers/apache-kafka/)
- [KEDA — Apache Kafka (Experimental) scaler (documents `type: apache-kafka`)](https://keda.sh/docs/2.20/scalers/apache-kafka-go/)
- [KEDA — Cluster operations: Restrict Secret Access](https://keda.sh/docs/2.20/operate/cluster/)
- [KEDA — Security: custom CAs and `unsafeSsl`](https://keda.sh/docs/2.20/operate/security/)
- [KEDA — Secret authentication provider](https://keda.sh/docs/2.20/authentication-providers/secret/) · [ConfigMap](https://keda.sh/docs/2.20/authentication-providers/configmap/) · [Environment variable](https://keda.sh/docs/2.20/authentication-providers/environment-variable/)
- [KEDA — AWS pod identity](https://keda.sh/docs/2.20/authentication-providers/aws/) · [Azure AD Workload Identity](https://keda.sh/docs/2.20/authentication-providers/azure-ad-workload-identity/) · [GCP Workload Identity](https://keda.sh/docs/2.20/authentication-providers/gcp-workload-identity/)
- [KEDA — HashiCorp Vault](https://keda.sh/docs/2.20/authentication-providers/hashicorp-vault/) · [AWS Secrets Manager](https://keda.sh/docs/2.20/authentication-providers/aws-secret-manager/) · [Azure Key Vault](https://keda.sh/docs/2.20/authentication-providers/azure-key-vault/) · [GCP Secret Manager](https://keda.sh/docs/2.20/authentication-providers/gcp-secret-manager/)
- [KEDA issue #4692 — the `apache-kafka` scaler design discussion, incl. OAuth/GSSAPI gaps](https://github.com/kedacore/keda/issues/4692)
- [Kubernetes — Secrets (`data` vs `stringData`)](https://kubernetes.io/docs/concepts/configuration/secret/)
- [Apache Kafka 4.3 — Security (SASL mechanisms, SASL_SSL)](https://kafka.apache.org/43/documentation/#security)
- [AWS — IAM access control for Amazon MSK](https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html)
