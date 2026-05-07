# Real-Time Streaming Pipeline
### Apache Kafka · Spark Structured Streaming · Python · Parquet · GCP

A production-grade real-time data pipeline that ingests financial transaction events via Kafka, processes and validates them using Spark Structured Streaming, and writes clean, partitioned Parquet output to Google Cloud Storage — queryable via BigQuery.

---

## Architecture

```
Python Producer → Kafka (topic: transactions) → Spark Structured Streaming
                                                        ↓
                                              Data Quality Checks
                                                        ↓
                                         Parquet (partitioned by currency)
                                                        ↓
                                         GCP Cloud Storage → BigQuery
```

**Why Kafka instead of direct Spark ingestion?**

Kafka decouples the producer and consumer, meaning if Spark goes down for any reason, no messages are lost — they wait in the topic and are consumed when Spark restarts. This is what makes the pipeline fault-tolerant and production-ready. Kafka also scales horizontally via partitions, enabling multiple consumers to read in parallel as throughput grows.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Message broker | Apache Kafka 7.4.0 |
| Stream processing | Apache Spark 3.5.0 (Structured Streaming) |
| Language | Python 3.x, PySpark |
| Output format | Parquet (Snappy compressed, partitioned by currency) |
| Cloud storage | GCP Cloud Storage (europe-west2) |
| Analytical layer | Google BigQuery |
| Containerisation | Docker + Docker Compose |
| Orchestration | Apache Kafka topic with 3 partitions |

---

## Project Structure

```
streaming-pipeline/
├── docker-compose.yml          # Kafka + Zookeeper containers
├── requirements.txt
├── README.md
├── producer/
│   ├── producer.py             # Generates and sends transactions to Kafka
│   └── transaction_schema.avsc # Avro schema definition
├── consumer/
│   ├── spark_consumer.py       # Spark Structured Streaming job
│   └── quality_checks.py       # Data quality validation functions
├── monitoring/
│   └── monitor.py              # Completeness checks and alerting
├── upload/
│   └── upload_to_gcp.py        # Uploads Parquet files to GCP Cloud Storage
└── config/
    └── settings.py             # Central configuration
```

---

## Data Quality Framework

Each micro-batch goes through three validation checks before being written to Parquet:

| Check | Logic | Action |
|---|---|---|
| Null check | Validates `transaction_id`, `user_id`, `amount`, `timestamp` are not null | Drops invalid rows and logs count |
| Amount range | Validates `amount` is between £0 and £50,000 | Removes implausible values |
| Status validation | Validates `status` is one of `completed`, `pending`, `failed` | Filters unknown statuses |

Rejected records are logged with counts per batch, enabling root-cause analysis and downstream alerting.

---

## Schema

Each transaction event contains:

```json
{
  "transaction_id": "uuid",
  "user_id":        "user_123",
  "amount":         199.99,
  "currency":       "GBP",
  "merchant":       "Amazon",
  "timestamp":      "2024-10-01T12:00:00",
  "status":         "completed"
}
```

**Schema evolution** is handled by defining an explicit `StructType` in Spark — new fields added to the producer are safely ignored rather than breaking the pipeline, and field type changes are caught at ingestion rather than propagating silently into the warehouse.

---

## Monitoring

The monitoring module (`monitoring/monitor.py`) runs independently and checks every 60 seconds that Parquet files are being written. If no files are written in a 5-minute window it triggers an alert — in production this would dispatch a Slack or email notification. This pattern mirrors production data reliability monitoring where data completeness SLAs need to be tracked continuously.

---

## Output

Parquet files are partitioned by currency for query performance:

```
output/parquet/
├── currency=GBP/
│   ├── part-00000-xxx.snappy.parquet
│   └── part-00001-xxx.snappy.parquet
├── currency=USD/
└── currency=EUR/
```

Partitioning by currency means analytical queries filtering on a single currency skip irrelevant partitions entirely — critical for performance at scale.

---

## BigQuery Results

After uploading to GCP Cloud Storage, files are queryable as an external table in BigQuery:

```sql
SELECT merchant, COUNT(*) as tx_count, ROUND(AVG(amount), 2) as avg_amount
FROM transactions.raw_transactions
GROUP BY merchant
ORDER BY tx_count DESC
```

| merchant | tx_count | avg_amount |
|---|---|---|
| Monzo | 396 | 1015.80 |
| Deliveroo | 392 | 977.18 |
| Netflix | 378 | 961.70 |
| Amazon | 370 | 1032.50 |
| Uber | 340 | 980.16 |
| Tesco | 333 | 971.61 |

---

## Running Locally

### Prerequisites
- Docker Desktop
- Python 3.x
- Java 17 (required for Spark)
- GCP account with Cloud Storage and BigQuery APIs enabled

### Setup

**1. Clone the repo**
```bash
git clone https://github.com/yourusername/streaming-pipeline.git
cd streaming-pipeline
```

**2. Create virtual environment**
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**3. Start Kafka**
```bash
docker-compose up -d
```

**4. Create the Kafka topic**
```bash
docker exec -it streaming-pipeline-kafka-1 kafka-topics \
  --create --topic transactions \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1
```

**5. Run the producer (terminal 1)**
```bash
python3 -m producer.producer
```

**6. Run the Spark consumer (terminal 2)**
```bash
python3 -m consumer.spark_consumer
```

**7. Run the monitor (terminal 3)**
```bash
python3 -m monitoring.monitor
```

**8. Upload to GCP (once data has accumulated)**
```bash
gcloud auth application-default login
python3 -m upload.upload_to_gcp
```

### Stopping the pipeline
```bash
# Ctrl+C in producer and consumer terminals
docker-compose down
```

---

## Design Decisions

**Micro-batch over continuous streaming:** Spark processes data every 30 seconds rather than record-by-record. This is a deliberate trade-off — micro-batching provides higher throughput and simpler failure recovery than continuous streaming, which is appropriate for analytical workloads where sub-second latency is not required.

**Parquet over CSV:** Parquet is columnar, compressed, and schema-aware. Queries on a single column (e.g. `amount`) read only that column from disk rather than entire rows — critical for analytical performance at scale. Snappy compression reduces storage costs without significant CPU overhead.

**Partitioning by currency:** Queries filtering on currency (the most common analytical dimension in financial data) skip irrelevant partitions entirely via partition pruning, reducing both query time and BigQuery scan costs.

**Explicit schema definition:** Rather than inferring schema from data, the Spark consumer defines the schema explicitly. This catches upstream data quality issues at ingestion rather than propagating bad data silently into the warehouse.

---

## Author

Deepthi Christina Victor Sagayanathan
[LinkedIn](https://www.linkedin.com/in/deepthi-christina-victor-sagayanathan-691105232/) | deepthiv1709@gmail.com