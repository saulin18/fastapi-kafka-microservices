# Technical test — Backend (Open Finance)

## Context / scenario

You work on a **transactions** pipeline (Open Finance style) with roughly this flow:

```text
Producer (transaction events)
    → Kafka (`transactions` topic)
    → Consumer
    → Downstream (antifraud, statement, reconciliation)
```

During the early morning (around **03:10**), a **partner campaign** pushes traffic to **~3×** the usual load. Serious symptoms appear:

- Kafka **consumer lag** grows continuously.
- The **antifraud** service reports **duplicate events**.
- Settlement / processing **p99 latency** degrades (from ~200 ms to several seconds).

The expected stack for this kind of exercise is typically:

- **Programming language** (producer and consumer)
- **Kafka** (partitions, consumer groups, offset commits)
- **Kubernetes** (Deployments, HPA, ConfigMaps, logs / `kubectl`)
- Optionally **Redis** (deduplication / circuit breaker)

---

## What is expected from you

Answer the **6 questions** below (investigation, mitigation, idempotency design, observability, communication/postmortem, and use of AI).

Optionally (as in the reference solution), implement the **core of an idempotent consumer in a language of your choice**, and if you want, Kubernetes manifests + Dockerfiles to reproduce the scenario locally (Minikube).

---

## Questions

### 1 — Incident investigation (before the fix)

Given the scenario above, describe **step by step HOW you would investigate** — without jumping to the fix.

Include:

1. Your **initial hypotheses** (ordered by likelihood).
2. What you would look at first (logs, metrics, traces, commands, or dashboards).
3. How each observation would **confirm or discard** a hypothesis.
4. What your **first signal** would be that you are on the right track.

---

### 2 — Mitigation vs permanent fix

It is **03:10** and lag is still growing.

- What do you do **FIRST** to stabilize (**mitigation**)?
- What do you leave for later (**permanent fix**)?
- Justify the **trade-off** between stopping the bleeding now and attacking the root cause.
- Explain the **risk** of each choice.

---

### 3 — Duplicate events and idempotency

Antifraud is receiving duplicate events.

1. Explain the **likely causes** in this kind of pipeline (idempotent production, reprocessing, partitioning, offset commits).
2. Explain how you would guarantee **idempotency** and **per-transaction ordering** in a robust way.
3. *(Optional)* Implement the core of idempotent consumption.

Common design hints from the reference solution:

- Producer with idempotence (`enable.idempotence=true` or equivalent).
- Consumer with `enable.auto.commit=false` and commit only after process/persist.
- Dedup table/store keyed by `transaction_id` (e.g. Redis).
- Partition key = `transaction_id` to preserve order within the same transaction.

---

### 4 — Prevention and early detection

After the incident, what would you change so it **does not repeat**, or so you detect it in **minutes, not hours**?

- Call out **2 or 3 SLIs/alerts** that were missing.
- What you would **instrument** (logs, metrics, tracing).
- Also consider **backpressure** and behavior under **3× load**.

---

### 5 — Communication and mini-postmortem

Write:

1. A **short summary** of how you would communicate to the team and manager **during** and **after** the incident.
2. A **mini-postmortem**: what happened, impact, likely cause, and actions.

Clarity, technical honesty, and ownership of the outcome are valued.

---

### 6 — Use of AI in this scenario

Explain how you would use AI tools to speed up and improve your work **in this scenario**:

- Which tasks you would lean on AI for.
- What you would ask the AI.
- Where you would trust vs **validate** the output before deploy/apply.
- What risks you would watch for (hallucination, unsafe code, sensitive data leakage, privacy / LGPD-style concerns).

---

## Suggested deliverables (aligned with the reference repo)

| Deliverable | Description |
|---|---|
| Answers document | Equivalent to `Respostas.md` / `Respuestas.md` |
| `src/producer/` | Async event producer |
| `src/consumer/` | Idempotent consumer |
| `src/something/` | Internal packages (idempotency, model, Kafka) |
| `src/k8s/` | Kubernetes manifests (namespace, Redis, Kafka, producer, consumer) |
| Dockerfiles | Producer and consumer images |

### How to reproduce the scenario locally (reference)

```bash
# 1. Start minikube
minikube start --cpus=4 --memory=8192

# 2. Namespace and infrastructure
kubectl apply -f src/k8s/00-namespace.yaml
kubectl apply -f src/k8s/01-redis.yaml
kubectl apply -f src/k8s/02-kafka.yaml

# 3. Create topic
kubectl exec kafka-0 -n openfinance -- /opt/kafka/bin/kafka-topics.sh \
  --create --topic transactions \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

# 4. Deploy producer and consumer
kubectl apply -f src/k8s/03-producer.yaml
kubectl apply -f src/k8s/04-consumer.yaml

# 5. Verify
kubectl logs -n openfinance -l app=producer --tail=5
kubectl logs -n openfinance -l app=consumer --tail=5
```

---

## Original test

[OpenFinance_Test](https://github.com/FreyreCorona/OpenFinance_Test)

Spanish version of this brief: [`enunciado-prueba-tecnica.md`](enunciado-prueba-tecnica.md)
