# Weather Stations Monitoring System

A data-intensive IoT pipeline that simulates distributed weather stations streaming high-frequency data to a centralized system for real-time processing, storage, and analytics. The system demonstrates modern stream processing and distributed systems principles using messaging, efficient storage, and visualization technologies.

---

## Architecture Overview

<img width="1007" height="427" alt="image" src="https://github.com/user-attachments/assets/65eeadd0-75b7-4144-9a72-0ef4d9090a93" />

The system is organized into three main layers:

**Data Acquisition** — Multiple simulated weather stations publish a reading every second, including temperature, humidity, wind speed, and battery status. Messages are streamed to Apache Kafka.

**Data Processing and Storage** — A central station consumes streams from Kafka and stores data using two complementary strategies: the latest reading per station is kept in a custom BitCask key-value store for fast O(1) access, while the full historical record is archived in partitioned Parquet files. A Kafka Streams processor runs in parallel to detect rain conditions in real time (humidity > 70%) and publish alerts to a dedicated topic.

**Indexing and Visualization** — Archived Parquet files are continuously indexed into Elasticsearch via a file-watching service, and the resulting dataset is explored through Kibana dashboards.

---

## Key Features

- Real-time data streaming with Apache Kafka
- Stream processing and rain-alert event detection via Kafka Streams DSL
- Dual storage strategy: BitCask for current state, Parquet for historical analytics
- Custom BitCask implementation with Hint Files, compaction, and a REST API
- Snappy-compressed, Hive-partitioned Parquet archives (`date=YYYY-MM-DD/station=N/`)
- Automatic Elasticsearch indexing triggered by a Java WatchService
- Kibana dashboards for battery distribution and dropped-message analysis
- Graceful shutdown with ordered teardown of Kafka consumer, BitCask, and Parquet writer
- Enterprise Integration Patterns: Dead-Letter Channel, Invalid Message Channel, Idempotent Receiver, Envelope Wrapper, Channel Adapter, Polling Consumer, Event-Driven Consumer
- Open-Meteo API adapter that injects real-world weather readings into the same pipeline
- Fully containerized with multi-stage Docker builds
- Kubernetes deployment via `kind`, using a StatefulSet for weather stations and a shared PersistentVolumeClaim for durable storage
- Performance profiling with Java Flight Recorder (JFR)

---

## Simulation Details

Each weather station:

- Publishes one JSON message per second to Kafka
- Randomly drops approximately 10% of messages to simulate real-world network conditions
- Assigns battery status according to the following distribution:
    - Low: 30%
    - Medium: 40%
    - High: 30%

The sequence number (`s_no`) increments monotonically per station and is persisted to a checkpoint file so stations resume correctly after a restart. It also serves as the deduplication key in the central station's idempotent receiver.

### Message Schema

```json
{
  "metadata": {
    "station_id": 1,
    "s_no": 1,
    "battery_status": "low",
    "status_timestamp": 12345
  },
  "payload": {
    "weather": {
      "humidity": 35,
      "temperature": 100,
      "wind_speed": 13
    }
  }
}
```

---

## Storage

### BitCask (Hot Storage)

The latest reading for each station is persisted in a custom BitCask log-structured store:

- Append-only sequential writes to active data files for high write throughput
- In-memory `KeyDir` hash map provides O(1) key lookup
- Hint Files generated on segment rotation allow fast KeyDir reconstruction after a crash
- A scheduled `BitCaskCompactor` merges old segments and removes stale entries
- Exposed via a REST API on port 8080; a Python CLI (`bitcask_client.sh`) supports bulk export, single-key queries, and concurrent stress tests

### Parquet (Cold Storage)

All incoming records are archived in Apache Parquet format:

- Records are buffered and flushed in batches of 10,000 to minimize disk I/O
- Partitioned by `date=YYYY-MM-DD/station=N/` for efficient predicate pushdown
- Background `ThreadPoolExecutor` handles Avro-to-Parquet conversion without blocking ingestion
- Snappy compression reduces storage footprint

---

## Historical Analysis

A Java WatchService monitors the Parquet root directory and triggers indexing whenever a new file or directory appears. Each record is converted to a `WeatherRecord` model, assigned a document ID of `station_id + s_no`, and bulk-indexed into Elasticsearch. Kibana's Lens tool is used to build dashboards that verify battery-level distributions and compute dropped-message percentages per station using the formula:

```
((max(s_no) - min(s_no) + 1 - count(s_no)) / count(s_no)) * 100
```

Observed results closely matched the configured targets (e.g., 30.29% Low, 40.17% Medium, 29.54% High), confirming correct data generation and end-to-end delivery.

---

## Kubernetes Deployment

The system runs on a local `kind` (Kubernetes in Docker) cluster. Docker images are built with multi-stage Dockerfiles and loaded directly into the cluster with `kind load docker-image`, avoiding the need for a private registry.

Notable deployment decisions:

- Weather stations are modeled as a **StatefulSet** so each pod has a stable hostname; the `entrypoint.sh` script derives `STATION_ID` from the pod ordinal at runtime
- A single **PersistentVolumeClaim** is shared between the central station and the Elasticsearch client to provide durable, consistent access to BitCask segments and Parquet archives
- Kafka runs in KRaft mode (broker + controller) without a separate Zookeeper dependency in newer configurations; Zookeeper 3.9.2 is also supported
- All environment variables (Kafka topics, batch sizes, API endpoints, etc.) are managed through a `ConfigMap`

### Quick Start

```bash
# Create the cluster
kind create cluster --name weather-monitoring

# Build images, load them into the cluster, and apply all manifests
./deploy.sh
```

The `deploy.sh` script handles Maven builds, Docker builds, `kind load`, pulling third-party images (Kafka, Elasticsearch, Kibana), and `kubectl apply -f k8s/`.

### Directory Structure

```
k8s/
  apps/
    central-station.yaml
    elasticsearch-client.yaml
    kafka-stream.yaml
    open-meteo-adapter.yaml
    weather-station.yaml
  configs/
    hadoop-config.yaml
    pvc.yaml
    weather-config.yaml
  infrastructure/
    elasticsearch.yaml
    kafka.yaml
    kibana.yaml
    zookeeper.yaml
```

---

## GitHub Actions CI Integration

A `deploy.yml` workflow is triggered on pushes to the `main` branch and can also be manually dispatched. It performs the following steps:
1. Checks out the code and sets up Java 21
2. Builds the project with Maven, skipping tests for speed
3. Provisions a `kind` cluster using `helm/kind-action`
4. Applies all Kubernetes manifests to the cluster
5. Updates all application workloads to use the latest images from GitHub Container Registry (GHCR
6. If any step fails, a rollback job attempts to undo the last deployment using `kubectl rollout undo`
7. The workflow uses the `GITHUB_TOKEN` secret to authenticate with GHCR for pushing and pulling images
8. The workflow is designed to be idempotent and can be safely re-run if needed, with proper error handling and logging for debugging.
9. The workflow also includes a step to verify the health of the deployed services after deployment, ensuring that the central station, Kafka Streams processor, and Open-Meteo adapter are running correctly before marking the deployment as successful.
10. The workflow is structured to allow for easy extension in the future, such as adding integration tests or performance benchmarks after deployment.


## Tech Stack

| Layer | Technology |
|---|---|
| Language | Java 21 |
| Messaging | Apache Kafka 3.9, Kafka Streams |
| Hot Storage | Custom BitCask implementation |
| Cold Storage | Apache Parquet with Snappy compression |
| Search & Analytics | Elasticsearch 8.13.4, Kibana 8.13.4 |
| Containerization | Docker (multi-stage builds) |
| Orchestration | Kubernetes (kind) |
| Profiling | Java Flight Recorder (JFR) |
| External Data | Open-Meteo API |

---

## Enterprise Integration Patterns Applied

| Pattern | Implementation |
|---|---|
| Envelope Wrapper | All messages carry a `metadata` + `payload` structure |
| Polling Consumer | `KafkaConsumer` polls on its own schedule with blocking |
| Idempotent Receiver | Deduplication via `s_no` sequence number |
| Dead-Letter Channel | Messages exceeding retry limits are routed to a dead-letter topic |
| Invalid Message Channel | Messages failing validation (schema, time skew) are routed to an invalid topic |
| Event-Driven Consumer & Message Filter | Kafka Streams listens and filters messages to the rain-alerts topic based on humidity and timestamp |
| Channel Adapter | Open-Meteo adapter maps external API responses to the internal message schema |

---

## Contributors

- [Yomna Yasser](https://github.com/yomnay888)
- [Mostafa Elkaranshawy](https://github.com/MostafaElKaranshawy)
- [Youssef Mahmoud](https://github.com/Youssef-Mahmoud0)
- [Ali Mahmoud Al-Jayyar](https://github.com/Ali29Mahmoud)