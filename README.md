# Kafka Java demo

Minimal **plain Java** (`kafka-clients`) and **Spring Boot** (`spring-kafka`) apps that share a single topic: **`demo-topic`**. Kafka and ZooKeeper run via Docker; apps use Java 17 (Eclipse Temurin).

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
