# Real-Time CDC Pipeline with Debezium, Kafka & Apache Flink

An end-to-end real-time Change Data Capture (CDC) pipeline that captures MySQL database changes using Debezium, streams them through Apache Kafka, processes and validates them using Apache Flink SQL, and maintains the latest state in PostgreSQL.

The complete stack is containerized with Docker Compose and was also deployed on an AWS EC2 `t3.large` instance with automated CI/CD using GitHub Actions, OIDC-based AWS authentication, and AWS Systems Manager.

---

## Architecture

```text
MySQL 8.4
   │
   │ Binlog CDC
   ▼
Debezium MySQL Connector
   │
   ▼
Apache Kafka
   │
   │ mysql.shop.orders
   ▼
Apache Flink SQL
   │
   ├── CDC normalization
   ├── Validation
   ├── Valid / Invalid routing
   ├── Audit & Reject handling
   ├── Deduplication / latest-state processing
   └── PostgreSQL upserts
   │
   ▼
PostgreSQL 17
```

---

## Key Features

- MySQL binlog-based Change Data Capture
- Debezium MySQL Connector for CDC event capture
- Kafka-based real-time event streaming
- Apache Flink SQL processing
- Kafka and JDBC connectors in Flink
- Valid and invalid record separation
- Audit and reject tables
- PostgreSQL latest-state upserts
- Flink checkpointing for fault tolerance
- Python source-to-target reconciliation
- Spike/load generation for CDC testing
- Docker Compose-based local and EC2 deployment
- GitHub Actions CI/CD
- OIDC-based AWS authentication
- Automated EC2 deployment through AWS Systems Manager

---

## PostgreSQL Tables

The pipeline maintains multiple downstream tables for current state, validation, auditing, rejects, and reconciliation.

```text
orders_current
orders_valid_current
cdc_raw_events
cdc_event_audit
cdc_rejects
pipeline_reconciliation
```

---

## Validation

A Python reconciliation script compares the MySQL source with the PostgreSQL target and checks for:

- Missing records
- Extra records
- Row-level mismatches
- Source and target counts
- Audit and reject records

Run validation with:

```bash
python scripts/validate.py
```

---

## Spike Test

Generate INSERT, UPDATE, and DELETE activity in MySQL:

```bash
python scripts/generate_spike.py \
  --count 2000 \
  --batch-size 250 \
  --update-percent 10 \
  --delete-percent 2
```

---

## Running the Pipeline

Create the environment file:

```bash
cp .env.example .env
```

Start the complete stack:

```bash
bash scripts/start.sh
```

The startup script:

1. Builds the custom Flink image
2. Starts MySQL, PostgreSQL, Kafka, Kafka Connect, Flink JobManager and TaskManager
3. Fixes Flink checkpoint/savepoint volume permissions
4. Creates the Python virtual environment and installs dependencies
5. Registers the Debezium MySQL connector through Kafka Connect REST API
6. Submits the Flink SQL CDC job

---

## CI/CD

### CI

Pull requests to `main` run automated validation including:

- Python syntax validation
- Pytest
- Debezium JSON validation
- Docker Compose validation
- Flink SQL structural checks
- Custom Flink Docker image build

### CD

After a PR is merged into `main`:

```text
GitHub Actions
      │
      │ OIDC
      ▼
AWS IAM Deployment Role
      │
      ▼
AWS Systems Manager
      │
      ▼
SSM Agent on EC2
      │
      ▼
Fetch latest main
      │
      ▼
Run scripts/start.sh
      │
      ▼
Dockerized CDC Pipeline
```

The deployment was tested on:

```text
AWS EC2
Instance Type: t3.large
OS: Ubuntu 24.04 LTS
```

---

## Flink Dashboard

For local deployment:

```text
http://localhost:8081
```

For EC2, the Flink UI can be accessed securely through an SSH tunnel:

```bash
ssh -i <key.pem> -L 8081:localhost:8081 ubuntu@<EC2_PUBLIC_IP>
```

Then open:

```text
http://localhost:8081
```

---

## Project Structure

```text
.
├── .github/workflows/
│   ├── ci.yml
│   └── cd.yml
├── debezium/
│   └── mysql-shop-connector.json
├── flink/
│   ├── Dockerfile
│   └── sql/
│       └── cdc_pipeline.sql
├── mysql/
├── postgres/
├── scripts/
│   ├── generate_spike.py
│   ├── register_connector.py
│   ├── start.sh
│   └── validate.py
├── tests/
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

---

## Tech Used

**MySQL • Debezium • Apache Kafka • Apache Flink • Flink SQL • PostgreSQL • Python • Docker • Docker Compose • GitHub Actions • AWS EC2 • IAM • OIDC • AWS Systems Manager**

---

## Author

**Neelabhra Sinha**

GitHub: https://github.com/ForgedFlesh