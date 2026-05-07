# Kafka Java demo

Minimal **plain Java** (`kafka-clients`) and **Spring Boot** (`spring-kafka`) apps that share a single topic: **`demo-topic`**. Kafka and ZooKeeper run via Docker; apps use Java 17 (Eclipse Temurin).

## About Apache Kafka

### What it is

**Apache Kafka** is a distributed **event streaming platform**: a cluster of brokers that stores streams of records (events/messages) in categories called **topics**, and lets many **producers** publish and many **consumers** subscribe at high throughput with strong ordering guarantees **per partition**. It is often used as a **central nervous system** for data in motion—connecting services, databases, and analytics without tight point-to-point coupling.

### Core ideas

- **Topic & partitions:** A topic is a log split into **partitions** (append-only, ordered segments). Partitioning allows **horizontal scale**: different partitions can live on different brokers and be consumed in parallel.
- **Consumer groups:** Consumers that share a **group id** cooperate: each partition of a subscribed topic is read by **at most one consumer in the group**, which enables **load balancing** and **scalable consumption**. Different groups each get their own view of the stream (useful for multiple downstream systems).
- **Retention:** Messages are kept for a configurable time or size, not only until “delivered.” New consumers can **replay** history (depending on offset policy), which supports analytics, auditing, and recovery.
- **Durability & replication:** Partitions are **replicated** across brokers; Kafka is designed for **fault tolerance** and surviving broker failures (configuration-dependent).

### Capabilities and features (typical)

- **High throughput, low latency** for many workloads compared to traditional message queues.
- **At-least-once** consumption is common; **exactly-once** semantics are possible in specific setups (e.g. transactions, idempotent producers, stream processing—more advanced than this demo).
- **Schema evolution** is often layered via **Schema Registry** (Avro, Protobuf, JSON Schema) in production, keeping producers and consumers compatible as data shapes change.
- **Ecosystem:** **Kafka Connect** for moving data to/from databases and systems; **Kafka Streams** / **ksqlDB** for stream processing on top of the same topics; metrics, security (TLS, SASL, ACLs), and multi-datacenter patterns at larger scale.

### Real-world use cases

| Area | Examples |
|------|----------|
| **Microservices** | Async commands, domain events, **outbox** patterns, decoupling deploy cycles. |
| **Analytics & data pipelines** | Ingesting clickstreams, logs, and sensor data into **data lakes** or warehouses (often via Connect + Flink/Spark). |
| **Log aggregation** | Centralizing application logs for search and monitoring (similar ideas to specialized log stacks). |
| **CQRS / event sourcing** | Storing an append-only **event log** as the source of truth for projections and read models. |
| **Integrations** | Bridging legacy systems, SaaS webhooks, and internal APIs without brittle synchronous chains. |

### How this repository fits

This project demonstrates the **smallest slice**: a **single topic**, **string** keys/values, and a **plain client** plus **Spring** abstraction. In production you would typically add **authentication**, **monitoring**, **topic design** (partitions, retention, compaction), **consumer offset strategy**, **backpressure**, and often **schemas** and **Connect** or **Streams**—all on top of the same publish model you see here.

## Layout

| Module       | Role |
|-------------|------|
| `plain-java` | `SimpleProducer` / `SimpleConsumer` using `KafkaProducer` / `KafkaConsumer` |
| `spring-kafka` | REST producer `POST /api/messages` and `@KafkaListener` consumer |

Bootstrap servers default to `localhost:9092` on the host. Inside Compose, use `kafka:29092` (set via `KAFKA_BOOTSTRAP_SERVERS`).

## Run everything in Docker

```bash
docker compose up -d --build
```

This starts ZooKeeper, Kafka, the Spring app (port **8080**), and the plain Java consumer (logs to stdout).

Publish via Spring:

```bash
curl -s -X POST http://localhost:8080/api/messages \
  -H 'Content-Type: application/json' \
  -d '{"key":"api-1","message":"hello from curl"}'
```

Run the plain Java producer once (the `plain-consumer` service uses the same image; override the command to run the producer). Example: send **5** messages:

```bash
docker compose run --rm plain-consumer \
  java -cp /app/app.jar com.example.kafka.SimpleProducer 5
```

Omit the trailing number to send the default batch (10 records).

Watch logs:

```bash
docker compose logs -f plain-consumer spring-kafka
```

## Run locally (Kafka in Docker only)

```bash
docker compose up -d zookeeper kafka
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
# Terminal A — plain consumer
cd plain-java && mvn -q exec:java -Dexec.mainClass=com.example.kafka.SimpleConsumer
# Terminal B — plain producer
cd plain-java && mvn -q exec:java -Dexec.mainClass=com.example.kafka.SimpleProducer -Dexec.args=5
# Terminal C — Spring
cd spring-kafka && mvn spring-boot:run
```

## Build

From repo root:

```bash
mvn -B package
```

## Requirements

- Docker / Docker Compose  
- Java 17 and Maven (for local runs and builds)
