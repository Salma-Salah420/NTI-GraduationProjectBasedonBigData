# MariTime

MariTime is a Big Data engineering project focused on building a data pipeline for maritime data.

The project combines historical NOAA AIS data with a real-time AIS streaming pipeline using distributed data-processing and messaging technologies.

## Architecture

```text
                         Historical Pipeline
                         ──────────────────

      NOAA AIS Dataset
             │
             ▼
      Airflow DAG
             │
             ▼
        HDFS /raw
             │
             ▼
          Spark
             │
             ▼
      HDFS /processed


                          Streaming Pipeline
                          ──────────────────

       AISStream.io
             │
             ▼
    aisstream_producer.py
             │
             ▼
          Kafka
        ais-events
             │
             ▼
    Spark Structured
       Streaming
             │
             ▼
      HDFS /streaming
             │
             ▼
       Parquet files
```

## Technology Stack

* **HDFS** — distributed storage
* **Apache Spark 3.5.6** — batch processing and Structured Streaming
* **Apache Kafka 3.9.0** — event streaming
* **Apache Airflow** — workflow orchestration
* **PostgreSQL** — Airflow metadata database
* **Docker Compose** — local distributed infrastructure
* **AISStream.io** — real-time AIS data source
* **NOAA AIS** — historical AIS data source
* **Python** — ingestion and processing scripts
* **Parquet** — processed/streaming storage format

---

# Project Structure

```text
MariTime/
├── airflow/
│   ├── dags/
│   │   └── noaa_historical_ingestion.py
│   ├── logs/
│   └── plugins/
│
├── scripts/
│   ├── ingestion/
│   │   ├── download_noaa.py
│   │   ├── upload_to_hdfs.py
│   │   └── validate_noaa.py
│   │
│   └── streaming/
│       └── aisstream_producer.py
│
├── spark/
│   ├── jobs/
│   │   ├── ais_streaming.py
│   │   └── process_ais.py
│   │
│   └── run_ais_streaming.sh
│
├── data/
│   └── raw/
│
├── docker-compose.yml
├── .env
├── .gitignore
└── README.md
```

---

# 1. Start the Infrastructure

From the project root:

```bash
docker compose up -d
```

Check the services:

```bash
docker compose ps
```

The main services include:

```text
namenode
datanode
spark-master
spark-worker
kafka
airflow
postgres
```

Airflow:

```text
http://localhost:8082
```

Spark Master UI:

```text
http://localhost:8080
```

---

# 2. Environment Variables

The project uses `.env` for environment configuration.

The AISStream API key must be available to the `spark-master` container:

```env
AISSTREAM_API_KEY=your_api_key
```

Do **not** commit `.env`.

Verify that Docker received the key:

```bash
docker exec maritime-spark-master \
    bash -c 'echo "API key length: ${#AISSTREAM_API_KEY}"'
```

A non-zero length confirms that the variable was passed into the container.

Do not print the actual API key.

---

# 3. Historical NOAA Pipeline

The historical pipeline downloads NOAA AIS data and stores it in HDFS.

## Airflow DAG

The DAG is:

```text
noaa_historical_ingestion
```

File:

```text
airflow/dags/noaa_historical_ingestion.py
```

The DAG accepts:

```text
year
date
```

through Airflow DAG run configuration.

Example:

```json
{
  "year": 2024,
  "date": "2024-01-01"
}
```

The resulting file is stored under:

```text
/maritime/raw/<year>/
```

For example:

```text
/maritime/raw/2024/ais-2024-01-01.csv.zst
```

Check HDFS:

```bash
docker exec maritime-namenode \
    hdfs dfs -ls -h /maritime/raw/2024
```

---

# 4. Historical Spark Processing

The historical processing job is:

```text
spark/jobs/process_ais.py
```

It reads:

```text
hdfs://namenode:9000/maritime/raw/2024/ais-2024-01-01.csv
```

and writes cleaned Parquet data to:

```text
hdfs://namenode:9000/maritime/processed/2024
```

The job:

* reads the CSV with Spark
* infers the schema
* casts important fields
* removes records without MMSI
* removes records without latitude
* removes records without longitude
* writes Parquet

The current processed dataset can be checked with:

```bash
docker exec maritime-namenode \
    hdfs dfs -ls -h /maritime/processed/2024
```

The processing pipeline has already been successfully tested.

---

# 5. Kafka Streaming Pipeline

Kafka topic:

```text
ais-events
```

Check the topic:

```bash
docker exec maritime-kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 \
    --list
```

A successful setup should contain:

```text
ais-events
```

To monitor messages:

```bash
docker exec maritime-kafka \
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 \
    --topic ais-events
```

To consume existing messages:

```bash
docker exec maritime-kafka \
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 \
    --topic ais-events \
    --from-beginning
```

---

# 6. Spark Structured Streaming

The Spark streaming job is:

```text
spark/jobs/ais_streaming.py
```

It consumes:

```text
Kafka → ais-events
```

and writes Parquet to:

```text
hdfs://namenode:9000/maritime/streaming
```

Checkpoint:

```text
hdfs://namenode:9000/maritime/checkpoints/ais-streaming
```

Run it with:

```bash
./spark/run_ais_streaming.sh
```

The script submits:

```text
spark/jobs/ais_streaming.py
```

to the Spark cluster.

Check the output:

```bash
docker exec maritime-namenode \
    hdfs dfs -ls -h /maritime/streaming
```

A working streaming pipeline produces:

```text
_spark_metadata/
part-....snappy.parquet
```

---

# 7. Inspect Streaming Data

To inspect the Parquet output:

```bash
docker exec maritime-spark-master bash -c '
echo '\''spark.read.parquet("hdfs://namenode:9000/maritime/streaming")
.createOrReplaceTempView("ais")
spark.sql("""
SELECT mmsi,
       base_date_time,
       longitude,
       latitude,
       sog,
       cog,
       topic,
       partition,
       offset,
       processed_at
FROM ais
ORDER BY processed_at DESC
LIMIT 10
""").show(false)
:quit'\'' | /opt/spark/bin/spark-shell --master local[2]
'
```

Expected columns:

```text
mmsi
base_date_time
longitude
latitude
sog
cog
topic
partition
offset
processed_at
```

The `topic`, `partition`, and `offset` fields are useful for tracing a record back to Kafka.

---

# 8. AISStream Real-Time Producer

The real-time producer is:

```text
scripts/streaming/aisstream_producer.py
```

It connects to:

```text
wss://stream.aisstream.io/v0/stream
```

and publishes normalized AIS PositionReport events to:

```text
Kafka → ais-events
```

The producer converts AISStream messages into the format expected by Spark:

```json
{
  "mmsi": 123456789,
  "base_date_time": "2026-08-19 12:00:00",
  "longitude": 32.123,
  "latitude": 30.456,
  "sog": 10.2,
  "cog": 125.4
}
```

Run it with:

```bash
docker exec -it maritime-spark-master \
    python3 /opt/scripts/streaming/aisstream_producer.py
```

Expected startup output:

```text
Connected to AISStream
Subscription sent
Streaming AIS PositionReports to Kafka...
```

When AISStream delivers data:

```text
Sent AIS event: MMSI=...
```

---

# 9. IMPORTANT: Current AISStream Status

The AISStream integration is **not currently confirmed end-to-end**.

The producer successfully establishes a WebSocket connection:

```text
Connected to AISStream
Subscription sent
```

However, during testing, AISStream did not deliver any messages even after:

* widening the geographic bounding box
* removing `FilterMessageTypes`
* waiting for more than 30 seconds
* testing the connection directly inside the Docker container

The minimal test produced:

```text
Connected
Subscription sent
Waiting 30 seconds...
NO MESSAGE RECEIVED WITHIN 30 SECONDS
```

Therefore:

```text
AISStream connection       ✅
API key passed to Docker   ✅
WebSocket connection       ✅
Subscription sent          ✅
AISStream message received ❌ currently unconfirmed
Kafka publishing           ❌ blocked by no incoming AIS events
```

**Do not assume that the producer is broken.**

The current evidence suggests that the issue occurs between:

```text
AISStream
    ↓
WebSocket
    ↓
AISStream message delivery
```

rather than between Kafka and Spark.

Before modifying the Kafka/Spark pipeline, test AISStream again.

---

# 10. Testing the AISStream Connection

A standalone test can be executed directly inside the Spark container without modifying the project:

```bash
docker exec -it maritime-spark-master python3 -c '
import asyncio
import json
import os
import websockets

async def test():
    print("Connecting...")

    async with websockets.connect(
        "wss://stream.aisstream.io/v0/stream"
    ) as ws:

        print("Connected")

        subscription = {
            "APIKey": os.environ["AISSTREAM_API_KEY"],
            "BoundingBoxes": [
                [
                    [-180.0, -90.0],
                    [180.0, 90.0]
                ]
            ]
        }

        await ws.send(json.dumps(subscription))

        print("Subscription sent")
        print("Waiting...")

        while True:
            try:
                message = await asyncio.wait_for(
                    ws.recv(),
                    timeout=120
                )

                print("MESSAGE RECEIVED:")
                print(message)
                break

            except asyncio.TimeoutError:
                print("No AIS message after 2 minutes.")
                break

            except websockets.exceptions.ConnectionClosed as e:
                print(f"Connection closed: {e}")

asyncio.run(test())
'
```

This test is useful because it bypasses:

```text
Kafka
Spark
Spark Structured Streaming
```

and tests only:

```text
Docker → AISStream
```

---

# 11. Testing the Pipeline Without AISStream

The Kafka → Spark → HDFS portion has already been tested using AIS-shaped messages.

Example:

```json
{
  "mmsi": 338075892,
  "base_date_time": "2024-01-01 00:21:03",
  "longitude": -70.252,
  "latitude": 43.654,
  "sog": 8.2,
  "cog": 130.1
}
```

This successfully produced Parquet output under:

```text
/maritime/streaming
```

and Spark was able to read it with:

```text
mmsi
base_date_time
longitude
latitude
sog
cog
topic
partition
offset
processed_at
```

Therefore, if AISStream is unavailable, teammates can continue development using test events to validate the downstream pipeline.

---

# 12. HDFS Layout

The current HDFS layout is:

```text
/maritime
├── raw
│   ├── 2024
│   │   ├── ais-2024-01-01.csv
│   │   └── ais-2024-01-01.csv.zst
│   └── 2025
│
├── processed
│   └── 2024
│       └── *.parquet
│
├── streaming
│   ├── _spark_metadata
│   └── *.parquet
│
├── rejected
│   ├── _spark_metadata
│   └── *.parquet
│
└── checkpoints
    ├── ais-streaming
    └── ais-rejected
```

Inspect everything:

```bash
docker exec maritime-namenode \
    hdfs dfs -ls -R /maritime
```

---

# 13. Rejected Streaming Events

The streaming validation/rejection pipeline writes invalid events to:

```text
/maritime/rejected
```

Checkpoint:

```text
/maritime/checkpoints/ais-rejected
```

This is useful for testing malformed Kafka events and data-quality handling.

Inspect rejected records:

```bash
docker exec maritime-namenode \
    hdfs dfs -ls -h /maritime/rejected
```

---

# 14. Useful Docker Commands

### View all services

```bash
docker compose ps
```

### View service logs

```bash
docker compose logs -f spark-master
```

```bash
docker compose logs -f kafka
```

```bash
docker compose logs -f airflow
```

### Restart Spark

```bash
docker compose restart spark-master spark-worker
```

### Recreate Spark Master after compose changes

```bash
docker compose up -d --force-recreate spark-master
```

### Enter a container

```bash
docker exec -it maritime-spark-master bash
```

```bash
docker exec -it maritime-namenode bash
```

### Stop everything

```bash
docker compose down
```

---

# 15. Important Container Paths

The project directory is mounted differently inside different containers.

For Spark jobs:

```text
Host:
./spark/

Container:
/opt/spark-apps/
```

For streaming scripts:

```text
Host:
./scripts/

Container:
/opt/scripts/
```

Therefore:

```text
Host:
scripts/streaming/aisstream_producer.py

Container:
/opt/scripts/streaming/aisstream_producer.py
```

and:

```text
Host:
spark/jobs/ais_streaming.py

Container:
/opt/spark-apps/jobs/ais_streaming.py
```

If a file appears on the host but cannot be found inside a container, check the corresponding Docker volume mount in `docker-compose.yml`.

---

# 16. Recommended Demo Order

For a clean project demonstration:

### Step 1 — Start infrastructure

```bash
docker compose up -d
```

### Step 2 — Verify services

```bash
docker compose ps
```

### Step 3 — Show historical data

```bash
docker exec maritime-namenode \
    hdfs dfs -ls -h /maritime/raw/2024
```

### Step 4 — Show processed data

```bash
docker exec maritime-namenode \
    hdfs dfs -ls -h /maritime/processed/2024
```

### Step 5 — Start Spark Streaming

```bash
./spark/run_ais_streaming.sh
```

### Step 6 — Start Kafka consumer in another terminal

```bash
docker exec maritime-kafka \
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 \
    --topic ais-events
```

### Step 7 — Start AISStream producer

```bash
docker exec -it maritime-spark-master \
    python3 /opt/scripts/streaming/aisstream_producer.py
```

If AISStream is delivering events, they should flow:

```text
AISStream
    ↓
Producer
    ↓
Kafka
    ↓
Spark Structured Streaming
    ↓
HDFS
```

### Step 8 — Verify HDFS

```bash
docker exec maritime-namenode \
    hdfs dfs -ls -h /maritime/streaming
```

### Step 9 — Inspect the records

Use the Spark SQL command from the streaming section above.

---

# 17. Git Workflow

Before committing:

```bash
git status
```

Make sure generated files are not being committed, especially:

```text
airflow/logs/
.env
large datasets
Spark checkpoints
```

Then:

```bash
git add .
```

Review:

```bash
git status
```

Commit:

```bash
git commit -m "feat: add maritime ingestion and streaming pipeline"
```

Push:

```bash
git push
```

---

# Current Project Status

## Working

* Docker-based distributed infrastructure
* HDFS storage
* NOAA historical data ingestion
* Historical AIS Spark processing
* Parquet output
* Kafka infrastructure
* Kafka `ais-events` topic
* Spark Structured Streaming
* Kafka → Spark → HDFS streaming pipeline
* Streaming checkpoints
* Rejected-event storage
* AISStream WebSocket connection
* AISStream API key injection through Docker environment

## Currently Investigating

* AISStream → producer message delivery

The producer can connect and send the subscription successfully, but no AIS messages were received during the latest tests.

## Next Recommended Tasks

1. Verify AISStream service/API availability.
2. Confirm live AIS messages can be received.
3. Complete AISStream → Kafka integration.
4. Verify live events appear in `/maritime/streaming`.
5. Add better retry/reconnection logic to the producer.
6. Add structured logging to the producer.
7. Improve data-quality validation.
8. Add more historical dates.
9. Add analytics/aggregation jobs.
10. Integrate the completed pipeline with Airflow orchestration.

---

# Important Handoff Note

The project should **not** be considered broken because AISStream is currently silent.

The downstream pipeline has already been demonstrated successfully using AIS-shaped Kafka events:

```text
Kafka
  ↓
Spark Structured Streaming
  ↓
Parquet
  ↓
HDFS
```

The remaining uncertainty is specifically the live external data source:

```text
AISStream
  ↓
Kafka
```

When continuing development, test that boundary independently before changing the Spark or Kafka components.
