# EventFlow Commerce — Event-Driven Order Tracking System

An event-driven e-commerce order tracking system built with Java, Spring Boot, Apache Kafka, PostgreSQL, and Docker. The project demonstrates asynchronous communication between services, order lifecycle coordination, event serialization, and stream-based analytics.

## Overview

When a customer places an order, the order service publishes an event. Payment and inventory services process that event independently and publish their outcomes. The order service uses those outcomes to move the order from `PENDING` to `CONFIRMED` or `CANCELLED`. If inventory fails after payment succeeds, the payment service handles the refund flow.

The project also includes notification logging, dead-letter handling, a transactional outbox, idempotent consumers, Avro schemas, and Kafka Streams analytics.

## Key Features

- Event-driven communication using Apache Kafka
- Independent order, payment, inventory, notification, and analytics services
- Order lifecycle coordination with confirmation, cancellation, and refund flows
- Avro event contracts with Confluent Schema Registry
- PostgreSQL persistence and Flyway database migrations
- Transactional outbox for order-service events
- Idempotent event processing for consumers with side effects
- Retry and dead-letter-topic handling for failed messages
- Kafka Streams analytics for order counts and customer revenue
- Integration testing with Testcontainers
- Docker Compose development environment
- Kubernetes manifests managed with Kustomize
- GitHub Actions workflows for build, test, and manifest validation

## Architecture

```text
Client
  |
  | POST /api/orders
  v
order-service ---------------> order-events
  |                                  |
  |                         +--------+---------+
  |                         |                  |
  |                         v                  v
  |                  payment-service   inventory-service
  |                         |                  |
  |                         v                  v
  |                  payment-events    inventory-events
  |                         \                  /
  |                          \                /
  +<--------------------------+--------------+
  |
  +---- OrderConfirmed / OrderCancelled
  |
  +---- notification-service (event timeline and dead letters)
  |
  +---- order-analytics (Kafka Streams read models)
```

### Modules and services

| Module | Responsibility | Default port |
|---|---|---:|
| `common-events` | Avro schemas, generated event classes, topic names, and shared Kafka utilities | — |
| `order-service` | REST API, order persistence, and order lifecycle coordination | `8080` |
| `payment-service` | Payment outcomes and compensating refunds | `8081` |
| `inventory-service` | Stock validation and reservation | `8082` |
| `notification-service` | Event timeline and dead-letter logging | `8083` |
| `order-analytics` | Kafka Streams aggregations and query endpoints | `8084` |

## Technology Stack

- **Language:** Java 25
- **Application framework:** Spring Boot 4.1, Spring for Apache Kafka
- **Messaging:** Apache Kafka 4 (KRaft), Kafka Streams
- **Event serialization:** Apache Avro 1.12, Confluent Schema Registry
- **Database:** PostgreSQL 16, Flyway
- **Observability:** Micrometer, Prometheus, Spring Boot Actuator
- **Testing:** JUnit, Testcontainers, Kafka Streams `TopologyTestDriver`
- **Build:** Maven
- **Containers:** Docker and Docker Compose
- **Deployment manifests:** Kubernetes, Kustomize, kind
- **CI:** GitHub Actions

## Prerequisites

Install the following before starting:

- JDK 25
- Maven 3.9 or later
- Docker with Docker Compose v2
- Git

Check the installations:

```bash
java -version
mvn -version
docker --version
docker compose version
```

## Getting Started

### 1. Start the supporting infrastructure

From the project root, run:

```bash
docker compose up -d --wait
```

The Compose environment includes Kafka, Kafka UI, Schema Registry, and PostgreSQL. The configured host ports include:

| Component | Host port |
|---|---:|
| Kafka | `9092` |
| Schema Registry | `8085` |
| Kafka UI | `8090` |
| PostgreSQL | `5433` |

Open [Kafka UI](http://localhost:8090) to inspect topics, partitions, consumer groups, and registered schemas.

### 2. Build and test

```bash
mvn -B verify
```

The Maven verification lifecycle compiles the modules and runs the project's tests. Testcontainers-based tests may start their own Kafka, PostgreSQL, or Schema Registry containers, depending on the test suite.

### 3. Start the services

After building, run each application JAR from the project root in separate terminals, or use the launch approach appropriate for your operating system.

Example for a Bash-compatible terminal:

```bash
for s in order-service payment-service inventory-service notification-service order-analytics; do
  java -jar "$s/target/$s-0.1.0-SNAPSHOT.jar" > "/tmp/$s.log" 2>&1 &
done
```

On Windows, run each `java -jar` command in a separate Command Prompt or PowerShell terminal and adjust log redirection as needed.

### 4. Create an order

With the services running, send a request to the order service:

```bash
curl -i -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d "{\"customerId\":\"customer-1\",\"items\":[{\"sku\":\"SKU-1\",\"quantity\":2,\"unitPrice\":19.99}]}"
```

The response includes the order ID and initial status. The order is initially `PENDING`; payment and inventory outcomes are processed asynchronously.

Retrieve the current order state using:

```text
GET http://localhost:8080/api/orders/{id}
```

Replace `{id}` with the ID returned by the create-order request.

### 5. Explore analytics

The analytics service exposes query endpoints on port `8084`:

```bash
curl "http://localhost:8084/analytics/orders-per-minute?last=5"
curl "http://localhost:8084/analytics/customers/customer-1/revenue"
```

The available data depends on events processed by the running application.

## Order Lifecycle and Event Flow

The principal topics are:

| Topic | Example events | Purpose |
|---|---|---|
| `order-events` | `OrderPlaced`, `OrderConfirmed`, `OrderCancelled` | Order lifecycle |
| `payment-events` | `PaymentSucceeded`, `PaymentFailed`, `PaymentRefunded` | Payment results and compensation |
| `inventory-events` | `InventoryReserved`, `InventoryFailed` | Stock reservation results |
| `*.DLT` | Failed records with diagnostic headers | Dead-letter inspection |

Events are keyed by `orderId` to preserve ordering for a given order within a Kafka partition.

The order lifecycle is:

1. The order service persists a new order as `PENDING` and publishes `OrderPlaced`.
2. Payment and inventory services consume the order event in their own consumer groups.
3. Each service publishes its outcome.
4. The order service records the outcomes and confirms the order when both steps succeed.
5. If either step fails, the order is cancelled. If payment succeeded before an inventory failure, the payment service processes a refund.
6. The notification service logs the event timeline, including dead-letter records.

The payment and inventory rules are deterministic in this project so that success and failure paths can be reproduced during testing. They are demonstration rules, not integrations with real payment providers or warehouse systems.

## Reliability and Messaging Design

### Transactional outbox

The order service writes events to an outbox table as part of the database transaction. A relay publishes those events to Kafka and removes successfully published rows. This reduces the risk of the database update and event publication getting out of sync.

### Idempotent consumers

Consumers with side effects track processed event IDs so a redelivered event can be ignored rather than applying the same business change twice.

### Retries and dead-letter topics

Failed records can be retried according to the configured error handling policy. Records that cannot be processed are sent to a dead-letter topic for inspection. Dead-letter messages preserve the original payload and diagnostic headers.

### Avro and Schema Registry

Event schemas are defined under `common-events/src/main/avro`. Maven generates Java classes from the schemas. The Schema Registry stores schema versions and enforces the configured compatibility policy.

### Kafka Streams analytics

The analytics module builds read models from order events, including:

- Order counts grouped into one-minute windows
- Confirmed revenue grouped by customer

These are example stream-processing read models rather than a general-purpose analytics database.

## Useful Scripts

The `scripts/` directory contains helper scripts for local development and demonstration. Review each script before running it, and use a Bash-compatible environment for scripts that rely on Unix shell utilities.

Examples from the project include:

```bash
scripts/load.sh
scripts/status.sh
scripts/poison.sh order-events
scripts/peek.sh order-events.DLT 3
```

These scripts exercise order creation, consumer lag inspection, and dead-letter handling.

## Testing

Run the complete Maven verification lifecycle:

```bash
mvn -B verify
```

The project includes tests for event serialization, schema compatibility, order API behavior, saga transitions, payment and inventory handling, idempotency, dead-letter processing, outbox behavior, and analytics.

Integration tests that use Testcontainers require Docker to be available.

## Kubernetes

The `k8s/base` directory contains Kustomize manifests for the application and supporting infrastructure.

Render the manifests locally:

```bash
kubectl kustomize k8s/base
```



## Current Scope and Limitations

This project is a local demonstration of event-driven order processing. It does not include a customer-facing UI, authentication, real payment-provider integration, or production-hardened multi-broker infrastructure. Review the configuration and security requirements before deploying it beyond a development environment.

