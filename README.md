# MariTime

MariTime is a Big Data engineering project focused on building a data pipeline for maritime data.

The project is being built around a distributed data-processing stack, with the goal of eventually ingesting, processing, orchestrating, and analyzing maritime data.

## Current Architecture

- **HDFS** — distributed storage
- **Apache Spark** — distributed data processing
- **Apache Kafka** — data streaming / event ingestion
- **Apache Airflow** — workflow orchestration
- **PostgreSQL** — Airflow metadata database
- **Docker Compose** — local infrastructure

```text
                    ┌─────────────┐
                    │    Kafka    │
                    └──────┬──────┘
                           │
                           ▼
┌──────────┐       ┌─────────────┐
│   HDFS   │ ◄──── │    Spark    │
└──────────┘       └──────┬──────┘
                           │
                           ▼
                    ┌─────────────┐
                    │   Airflow   │
                    └─────────────┘
                           │
                           ▼
                    ┌─────────────┐
                    │  PostgreSQL │
                    └─────────────┘
````

## Project Structure

```text
MariTime/
├── airflow/
│   ├── dags/
│   ├── logs/
│   └── plugins/
├── spark/
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

## Status

The initial infrastructure is currently being set up and verified.

Next steps include:

* Create the first Airflow DAG
* Connect Airflow with Spark
* Define the Kafka ingestion flow
* Build the HDFS data pipeline
* Implement processing and transformation jobs
* Add monitoring and validation

## Goal

The goal is to move beyond a simple "load dataset → process → analyze" workflow and build a more complete Big Data pipeline using real distributed-system components.
