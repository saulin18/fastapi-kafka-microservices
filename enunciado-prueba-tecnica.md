# Prueba técnica — Backend (Open Finance)

## Contexto / escenario

Trabajas en un pipeline de **transacciones** (estilo Open Finance) con este flujo aproximado:

```text
Productor (eventos de transacción)
    → Kafka (tópico `transactions`)
    → Consumidor
    → Downstream (antifraude, extracto, conciliación)
```

Durante la madrugada (alrededor de las **03:10**), una **campaña de un partner** eleva el tráfico a **~3×** lo habitual. Empiezan a aparecer síntomas graves:

- El **lag de consumo** de Kafka crece de forma continua.
- El servicio de **antifraude** reporta **eventos duplicados**.
- La latencia **p99** de liquidación/procesamiento se degrada (de ~200 ms a varios segundos).

El stack esperado es típico de este tipo de prueba:

- **Programming language** (productor y consumidor)
- **Kafka** (particiones, consumer groups, commit de offsets)
- **Kubernetes** (Deployments, HPA, ConfigMaps, logs/`kubectl`)
- Opcionalmente **Redis** (deduplicación / circuit breaker)

---

## Qué se espera de ti

Responde las **6 preguntas** de abajo (investigación, mitigación, diseño de idempotencia, observabilidad, comunicación/postmortem y uso de IA).

Opcionalmente (como en la solución de referencia), implementa el **núcleo de un consumidor idempotente en el lenguaje escogido por ti** y, si quieres, manifests de Kubernetes + Dockerfiles para reproducir el escenario en local (Minikube).

---

## Preguntas

### 1 — Investigación del incidente (antes de la solución)

Ante el escenario anterior, describe **paso a paso CÓMO investigarías** — sin saltar a la corrección.

Incluye:

1. Tus **hipótesis iniciales** (ordenadas por probabilidad).
2. Qué mirarías primero (logs, métricas, traces, comandos o dashboards).
3. Cómo cada observación **confirmaría o descartaría** una hipótesis.
4. Cuál sería tu **primer señal** de que vas por el camino correcto.

---

### 2 — Mitigación vs corrección definitiva

Son las **03:10** y el lag sigue creciendo.

- ¿Qué haces **PRIMERO** para estabilizar (**mitigación**)?
- ¿Qué dejas para después (**corrección definitiva**)?
- Justifica el **trade-off** entre cortar el sangrado ahora y atacar la causa raíz.
- Explica el **riesgo** de cada elección.

---

### 3 — Eventos duplicados e idempotencia

El antifraude está recibiendo eventos duplicados.

1. Explica las **causas probables** en este tipo de pipeline (producción idempotente, reprocesamiento, particionamiento, commit de offset).
2. Explica cómo garantizarías **idempotencia** y **orden por transacción** de forma robusta.
3. *(Opcional)* Implementa el núcleo del consumo idempotente.

Pistas de diseño habituales en la solución de referencia:

- Productor con idempotencia (`enable.idempotence=true` o equivalente).
- Consumidor con `enable.auto.commit=false` y commit solo después de procesar/persistir.
- Tabla/store de deduplicación indexada por `transaction_id` (p. ej. Redis).
- Clave de partición = `transaction_id` para preservar orden dentro de la misma transacción.

---

### 4 — Prevención y detección temprana

Después del incidente, ¿qué cambiarías para que **no se repita**, o para detectarlo en **minutos, no en horas**?

- Señala **2 o 3 SLIs/alertas** que faltaban.
- Qué **instrumentarías** (logs, métricas, tracing).
- Considera también **backpressure** y el comportamiento bajo **3× de carga**.

---

### 5 — Comunicación y mini-postmortem

Escribe:

1. Un **resumen corto** de cómo comunicarías al equipo y al gestor **durante** y **después** del incidente.
2. Un **mini-postmortem**: qué pasó, impacto, causa probable y acciones.

Se valora claridad, honestidad técnica y responsabilidad sobre el resultado.

---

### 6 — Uso de IA en este escenario

Explica cómo usarías herramientas de IA para acelerar y mejorar tu ejecución **en este escenario**:

- En qué tareas te apoyarías.
- Qué pedirías a la IA.
- Dónde confiarías y dónde **validarías** la salida antes de desplegar/aplicar cambios.
- Qué riesgos observarías (alucinación, código inseguro, fuga de datos sensibles, LGPD / privacidad).

---

## Entregables sugeridos (alineados con el repo de referencia)

| Entregable | Descripción |
|---|---|
| Documento de respuestas | Equivalente a `Respuestas.md` |
| `src/producer/` | Productor asíncrono de eventos |
| `src/consumer/` | Consumidor idempotente en Go |
| `src/something/` | Paquetes internos (idempotencia, modelo, Kafka) |
| `src/k8s/` | Manifests de Kubernetes (namespace, Redis, Kafka, producer, consumer) |
| Dockerfiles | Imágenes del productor y del consumidor |

### Cómo reproducir el escenario en local (referencia)

```bash
# 1. Iniciar minikube
minikube start --cpus=4 --memory=8192

# 2. Namespace e infraestructura
kubectl apply -f src/k8s/00-namespace.yaml
kubectl apply -f src/k8s/01-redis.yaml
kubectl apply -f src/k8s/02-kafka.yaml

# 3. Crear tópico
kubectl exec kafka-0 -n openfinance -- /opt/kafka/bin/kafka-topics.sh \
  --create --topic transactions \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

# 4. Deploy del producer y consumer
kubectl apply -f src/k8s/03-producer.yaml
kubectl apply -f src/k8s/04-consumer.yaml

# 5. Verificar
kubectl logs -n openfinance -l app=producer --tail=5
kubectl logs -n openfinance -l app=consumer --tail=5
```

---

## Prueba original

[OpenFinance_Test](https://github.com/FreyreCorona/OpenFinance_Test)

## English version of this brief

[`technical-test-brief.md`](technical-test-brief.md)
