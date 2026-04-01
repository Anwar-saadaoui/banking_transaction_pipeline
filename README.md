# 🏦 Banking Transaction Pipeline

> A production-grade data engineering pipeline for real-time banking transaction processing — built with Apache Kafka, Spark, Airflow, PostgreSQL, and Docker.

![Pipeline](https://img.shields.io/badge/Pipeline-Data%20Engineering-blue?style=for-the-badge)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5.0-231F20?style=for-the-badge&logo=apachekafka)
![Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-E25A1C?style=for-the-badge&logo=apachespark)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8.1-017CEE?style=for-the-badge&logo=apacheairflow)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=for-the-badge&logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker)

---

## 📐 Architecture

```
CSV Files ──► Airflow DAG ──► Kafka Topic ──► Spark Transform ──► PostgreSQL DW ──► BI / Dashboards
               (orchestrate)   (stream)        (clean & enrich)    (store & query)
```

### Stack

| Layer | Technology | Role |
|---|---|---|
| Orchestration | Apache Airflow 2.8.1 | DAG scheduling & monitoring |
| Streaming | Apache Kafka 7.5.0 | Real-time event bus |
| Processing | Apache Spark 3.5.1 | Distributed transformations |
| Storage | PostgreSQL 15 | Analytical data warehouse |
| Infrastructure | Docker Compose | Container orchestration |
| Monitoring | Kafka UI, Spark UI | Pipeline observability |

---

## 📁 Project Structure

```
banking_transaction_pipeline/
├── 🐳 docker-compose.yml          # All services wired together
├── 📂 dags/
│   └── banking_pipeline_dag.py    # Airflow DAG (CSV → Kafka → Spark → PG)
├── 📂 spark/
│   └── transform.py               # PySpark transformation job
├── 📂 sql/
│   └── init.sql                   # PostgreSQL schema
├── 📂 data/
│   └── raw/                       # Drop your CSV files here ⬅️
└── 📂 config/
    └── requirements.txt           # Python dependencies
```

---

## 🗃️ Data Schema

The pipeline processes banking transactions with the following fields:

| Column | Type | Description |
|---|---|---|
| `transaction_id` | VARCHAR | Unique transaction identifier |
| `client_id` | VARCHAR | Client identifier |
| `date_transaction` | DATE | Transaction date |
| `montant` | NUMERIC | Transaction amount |
| `devise` | VARCHAR | Currency (MAD, EUR, USD) |
| `taux_change_eur` | NUMERIC | Exchange rate to EUR |
| `montant_eur` | NUMERIC | Amount converted to EUR |
| `categorie` | VARCHAR | Transaction category |
| `produit` | VARCHAR | Banking product |
| `agence` | VARCHAR | Branch agency |
| `type_operation` | VARCHAR | Operation type (DEBIT/CREDIT/...) |
| `statut` | VARCHAR | Transaction status |
| `score_credit_client` | NUMERIC | Client credit score |
| `segment_client` | VARCHAR | Client segment (VIP/PREMIUM/...) |
| `solde_avant` | NUMERIC | Balance before transaction |
| `taux_interet` | NUMERIC | Interest rate |

### Database Tables

```
banking_dw
├── raw_transactions      ← Raw CSV data landing zone
├── fact_transactions     ← Cleaned & enriched fact table
├── dim_clients           ← Client dimension (upserted)
└── agg_daily_summary     ← Daily aggregations by category & agency
```

---

## 🚀 Quick Start

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop) installed and running
- Python 3.8+

### 1. Clone the repo

```bash
git clone https://github.com/your-username/banking-transaction-pipeline.git
cd banking-transaction-pipeline
```

### 2. Start all services

```bash
docker compose up -d
```

### 3. Wait ~1 minute then verify all containers are healthy

```bash
docker compose ps
```

Expected output — all 8 services running:

```
banking_transaction_pipeline-airflow-scheduler   Up
banking_transaction_pipeline-airflow-webserver   Up   0.0.0.0:8082->8080
banking_transaction_pipeline-kafka               Up   (healthy)
banking_transaction_pipeline-kafka-ui            Up   0.0.0.0:8080->8080
banking_transaction_pipeline-postgres            Up   (healthy)
banking_transaction_pipeline-spark-master        Up   0.0.0.0:8081->8080
banking_transaction_pipeline-spark-worker        Up
banking_transaction_pipeline-zookeeper           Up
```

### 4. Initialize Airflow

```bash
docker exec banking_transaction_pipeline-airflow-webserver-1 airflow db init

docker exec banking_transaction_pipeline-airflow-webserver-1 airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com
```

### 5. Create the database schema

```powershell
# Windows PowerShell
Get-Content sql/init.sql | docker exec -i banking_transaction_pipeline-postgres-1 psql -U airflow
```

```bash
# Linux / Mac
docker exec -i banking_transaction_pipeline-postgres-1 psql -U airflow < sql/init.sql
```

### 6. Generate sample data (optional)

```bash
python generate_data.py
```

This creates `data/raw/transactions.csv` with 1000 realistic banking transactions.

### 7. Trigger the pipeline

Go to **http://localhost:8082** → login with `admin / admin` → click ▶️ on `banking_transaction_pipeline`

---

## 🌐 Service URLs

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8082 | admin / admin |
| Kafka UI | http://localhost:8080 | — |
| Spark UI | http://localhost:8081 | — |
| PostgreSQL | localhost:5432 | airflow / airflow |

---

## ⚙️ Pipeline DAG

The Airflow DAG runs **daily** and consists of 3 tasks:

```
ingest_csv_to_kafka  ──►  load_raw_to_postgres  ──►  spark_transform
   (PythonOperator)           (PythonOperator)          (BashOperator)
```

| Task | Description |
|---|---|
| `ingest_csv_to_kafka` | Reads CSV from `data/raw/`, publishes each row to Kafka topic `transactions` |
| `load_raw_to_postgres` | Consumes Kafka topic and loads into `raw_transactions` table |
| `spark_transform` | Runs PySpark job: cleans, enriches, computes derived fields, writes to fact tables |

---

## 🔧 Spark Transformations

The Spark job applies the following transformations:

- **Deduplication** — removes duplicate `transaction_id`
- **Type casting** — converts strings to proper DATE, NUMERIC types
- **EUR conversion** — fills missing `montant_eur` using `montant × taux_change_eur`
- **Derived fields** — computes `solde_apres = solde_avant + montant`
- **Debit flag** — sets `is_debit = TRUE` for DEBIT/RETRAIT/VIREMENT_SORTANT
- **Normalization** — uppercases and trims `categorie`, `statut`, `devise`
- **Aggregations** — daily summary by category and agency

---

## 🛠️ Troubleshooting

**Kafka fails to start:**
```bash
docker compose down -v   # clears stale Zookeeper data
docker compose up -d
```

**Airflow DB not initialized:**
```bash
docker exec banking_transaction_pipeline-airflow-webserver-1 airflow db init
```

**No CSV files found error:**
Make sure your CSV is inside `data/raw/` before triggering the DAG.

**PowerShell redirection error (`<` not supported):**
```powershell
Get-Content sql/init.sql | docker exec -i <container> psql -U airflow
```

---

## 📦 Dependencies

```
apache-airflow==2.8.1
kafka-python==2.0.2
pandas==2.1.4
psycopg2-binary==2.9.9
pyspark==3.5.0
```

---

## 📊 Sample Output

After a successful pipeline run, query your data warehouse:

```sql
-- Top agencies by transaction volume
SELECT agence, COUNT(*) as nb_transactions, SUM(montant_eur) as total_eur
FROM fact_transactions
GROUP BY agence
ORDER BY total_eur DESC;

-- Client segments distribution
SELECT segment_client, COUNT(DISTINCT client_id) as nb_clients
FROM dim_clients
GROUP BY segment_client;

-- Daily summary
SELECT * FROM agg_daily_summary
ORDER BY summary_date DESC
LIMIT 10;
```

---

## 🤝 Contributing

Pull requests are welcome! For major changes, please open an issue first.

---

## 📄 License

MIT License — feel free to use this project for learning and portfolio purposes.

---

<p align="center">Built with ❤️ for Data Engineering</p>
