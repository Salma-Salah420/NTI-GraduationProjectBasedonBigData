# MariTime

MariTime is a Big Data engineering project focused on building a data pipeline for maritime data.

The project uses a distributed data-processing stack to ingest, store, process, orchestrate, and stream maritime AIS data.

## Architecture

* **HDFS** — distributed storage
* **Apache Spark** — distributed data processing and Structured Streaming
* **Apache Kafka** — event streaming / live AIS ingestion
* **Apache Airflow** — workflow orchestration
* **PostgreSQL** — Airflow metadata database
* **Docker Compose** — local infrastructure

```text
                         ┌─────────────┐
                         │    Kafka    │
                         │  ais-events │
                         └──────┬──────┘
                                │
                                ▼
                         ┌─────────────┐
                         │    Spark    │
                         │  Streaming  │
                         └──────┬──────┘
                                │
                                ▼
                         ┌─────────────┐
                         │    HDFS     │
                         │  Streaming  │
                         │   Parquet   │
                         └─────────────┘

Historical NOAA Data
        │
        ▼
   Airflow DAG
        │
        ▼
   Ingestion Scripts
        │
        ▼
       HDFS
        │
        ▼
      Spark
        │
        ▼
     Processed
```

## Project Structure

```text
MariTime/
├── airflow/
│   ├── dags/
│   │   └── noaa_historical_ingestion.py
│   └── plugins/
├── scripts/
│   └── ingestion/
│       ├── download_noaa.py
│       ├── upload_to_hdfs.py
│       └── validate_noaa.py
├── spark/
│   ├── jobs/
│   │   ├── ais_streaming.py
│   │   └── process_ais.py
│   └── run_ais_streaming.sh
├── docker-compose.yml
├── .env
└── README.md
```

## Running the Infrastructure

Start the services with:

```bash
docker compose up -d
```

Check the running containers:

```bash
docker compose ps
```

Airflow is available at:

```text
http://localhost:8082
```

## Historical Data Pipeline

The historical pipeline uses NOAA AIS data.

The ingestion workflow is:

```text
NOAA AIS Data
     │
     ▼
Download
     │
     ▼
Validate
     │
     ▼
Upload to HDFS
     │
     ▼
Spark Processing
     │
     ▼
Processed Data
```

The Airflow DAG responsible for this workflow is:

```text
airflow/dags/noaa_historical_ingestion.py
```

The ingestion utilities are located under:

```text
scripts/ingestion/
```

## Streaming Demo

The current streaming pipeline demonstrates:

```text
AIS Event
   │
   ▼
Kafka (ais-events)
   │
   ▼
Spark Structured Streaming
   │
   ▼
HDFS
   │
   ▼
Parquet
```

### 1. Start the infrastructure

```bash
docker compose up -d
```

Verify the services:

```bash
docker compose ps
```

### 2. Create the Kafka topic

```bash
docker exec maritime-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create \
  --topic ais-events \
  --partitions 3 \
  --replication-factor 1
```

Verify it:

```bash
docker exec maritime-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --list
```

Expected:

```text
ais-events
```

### 3. Start the Spark streaming job

Run:

```bash
docker exec -it maritime-spark-master \
    /opt/spark-apps/run_ais_streaming.sh
```

The Spark job consumes events from:

```text
kafka:9092
```

and the `ais-events` topic.

### 4. Send an AIS event

In another terminal:

```bash
docker exec -it maritime-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 \
  --topic ais-events
```

Send:

```json
{"mmsi":338075892,"base_date_time":"2024-01-01 00:10:03","longitude":-70.25298,"latitude":43.65322,"sog":5.2,"cog":120.4}
```

The Spark streaming job should process the event and write it to HDFS.

### 5. Verify the HDFS output

```bash
docker exec maritime-namenode \
    hdfs dfs -ls -h /maritime/streaming
```

The directory contains Spark-generated Parquet files and streaming metadata:

```text
/maritime/streaming/
├── _spark_metadata/
└── part-*.snappy.parquet
```

### 6. Read the streaming data from HDFS

```bash
docker exec maritime-spark-master bash -c '
echo '\''spark.read.parquet("hdfs://namenode:9000/maritime/streaming").show(false)
:quit'\'' | /opt/spark/bin/spark-shell --master local[2]
'
```

The resulting records contain fields such as:

```text
mmsi
base_date_time
longitude
latitude
sog
cog
```

### Streaming Checkpoint

Spark maintains a checkpoint for the streaming query:

```text
/tmp/kafka-ais-processor-checkpoint
```

The checkpoint allows Spark Structured Streaming to track Kafka offsets and process newly arriving events incrementally.

## Current Status

The following components have been implemented and verified:

* Docker-based Big Data infrastructure
* HDFS storage
* NOAA historical data ingestion
* NOAA data validation
* Airflow ingestion DAG
* Spark data processing
* Kafka topic and event ingestion
* Spark Structured Streaming
* Kafka → Spark → HDFS streaming pipeline
* Parquet output in HDFS
* Streaming checkpointing and Kafka offset tracking

## Next Steps

* Improve the historical processing pipeline
* Connect Airflow workflows with Spark jobs
* Integrate historical and streaming data
* Add data quality and validation checks
* Add monitoring and failure handling
* Build maritime analytics on top of the processed data

## Goal

The goal is to move beyond a simple:

```text
load dataset → process → analyze
```

workflow and build a more complete Big Data platform using distributed storage, batch processing, workflow orchestration, and real-time data streaming.
