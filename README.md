# 🏗️ Data Lakehouse ELT Pipeline — GCP + BigLake + BigQuery + Iceberg + Airflow (Astro)

A production-grade **Data Lakehouse ELT pipeline** on Google Cloud Platform using the **Yelp Open Dataset**, orchestrated by **Airflow on Astronomer (Astro)**.

![Architecture](docs/architecture-badge.svg)

## 🏛️ Architecture

```
                    ┌─────────────────────────────────────┐
                    │     Airflow (Astronomer / Astro)     │
                    │   Data-Aware Scheduling · Alerting   │
                    └────────────────┬────────────────────┘
                                    │
           ┌────────────────────────┼────────────────────────┐
           ▼                        ▼                        ▼
   ┌──────────────┐      ┌──────────────────┐     ┌──────────────────┐
   │ 🥉 BRONZE    │      │ 🥈 SILVER        │     │ 🥇 GOLD          │
   │ GCS + Iceberg│ ───▶ │ BigQuery Native  │ ──▶ │ BigQuery + MVs   │
   │ Raw JSON     │      │ Partitioned      │     │ Aggregated       │
   │ BigLake Ext. │      │ PII-Masked       │     │ ML Features      │
   └──────────────┘      └──────────────────┘     └────────┬─────────┘
                                                           │
                            ┌──────────────────────────────┼────────┐
                            ▼                              ▼        ▼
                   ┌──────────────────┐         ┌──────────────┐  ┌─────────┐
                   │ Dataflow DLQ     │         │ BigQuery ML  │  │ Cloud   │
                   │ ParDo Validation │         │ CREATE MODEL │  │ Monitor │
                   │ Side Outputs     │         │ ML.EVALUATE  │  │ Alerts  │
                   └──────────────────┘         └──────────────┘  └─────────┘
```

## 🎯 Features

| Feature | Implementation |
|---------|---------------|
| **Medallion Architecture** | Bronze (Iceberg/GCS) → Silver → Gold (BigQuery) |
| **Batch Ingestion** | Yelp JSON → GCS with dynamic task mapping |
| **Apache Iceberg** | BigLake managed Iceberg tables on GCS |
| **PII Handling** | GCP Sensitive Data Protection (DLP) + policy tags |
| **Partitioned Tables** | Date/Month partitioning + clustering per entity |
| **Schema Evolution** | Auto-detect new columns, ALTER TABLE, type widening |
| **ML Pipeline** | BigQuery ML `CREATE MODEL` (3 models) |
| **Data Validation** | Dataflow ParDo/DoFn DLQ + Soda-style checks |
| **Alerting** | Email on failure + Cloud Monitoring (duration degradation) |
| **JSON Treatment** | Nested JSON flattening, type coercion, safe parsing |
| **Data-Aware Scheduling** | Airflow Datasets (Bronze → Silver → Gold → ML) |

## 📂 Project Structure

```
├── Dockerfile                          # Astro Runtime + GCP packages
├── requirements.txt                    # Python dependencies
├── packages.txt                        # System packages (Java for Iceberg)
├── .env                                # GCP configuration
├── airflow_settings.yaml               # Connections, pools, variables
│
├── dags/
│   ├── bronze_batch_ingest.py          # 🥉 Batch ingest Yelp → GCS
│   ├── silver_transform.py            # 🥈 Transform + PII masking
│   ├── gold_aggregate.py              # 🥇 Business analytics aggregations
│   ├── ml_training_pipeline.py        # 🤖 BigQuery ML training
│   ├── data_validation.py             # ✅ Dataflow DLQ + Soda checks
│   └── common/
│       ├── dag_config.py              # Shared config & defaults
│       └── callbacks.py               # Email alerting callbacks
│
├── include/
│   ├── schemas/
│   │   └── yelp_schemas.py            # Schema definitions (all entities)
│   ├── utils/
│   │   ├── json_handler.py            # JSON flattening & type coercion
│   │   └── gcs_helpers.py             # GCS upload/download wrappers
│   ├── pii/
│   │   └── sensitive_data_protection.py # DLP inspection & masking
│   ├── schema_evolution/
│   │   └── evolve.py                  # Schema change detection
│   └── sql/
│       ├── silver/                    # Silver DDL & transforms
│       ├── gold/                      # Gold aggregation queries
│       └── ml/                        # CREATE MODEL & EVALUATE
│
├── dataflow/
│   └── validation_pipeline.py         # Beam ParDo/DoFn DLQ pipeline
│
├── infrastructure/
│   └── terraform/
│       ├── main.tf                    # GCS, BigQuery, BigLake, KMS
│       ├── variables.tf               # Input variables
│       ├── outputs.tf                 # Resource outputs
│       └── monitoring.tf             # Alert policies & notifications
│
├── data/                              # Place Yelp JSON files here
└── tests/                             # DAG and unit tests
```

## 🚀 Quick Start

### Prerequisites

- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)
- [Docker Desktop](https://www.docker.com/products/docker-desktop)
- [Google Cloud SDK](https://cloud.google.com/sdk/docs/install)
- [Terraform](https://developer.hashicorp.com/terraform/install) (optional, for infra provisioning)

### 1. Clone and Configure

```bash
cd complete-data-pipeline-gcp-airflow

# Edit .env with your GCP project ID and region
nano .env
```

### 2. Download the Yelp Dataset

1. Go to [Yelp Open Dataset](https://www.yelp.com/dataset/download)
2. Accept the license agreement
3. Download and extract the JSON files
4. Place them in the `data/` directory:
   ```
   data/
   ├── yelp_academic_dataset_business.json
   ├── yelp_academic_dataset_review.json
   ├── yelp_academic_dataset_user.json
   ├── yelp_academic_dataset_checkin.json
   └── yelp_academic_dataset_tip.json
   ```

### 3. Provision GCP Infrastructure

```bash
cd infrastructure/terraform
terraform init
terraform plan -var="project_id=YOUR_GCP_PROJECT"
terraform apply -var="project_id=YOUR_GCP_PROJECT"
```

### 4. Start Airflow Locally

```bash
astro dev start
```

Access the Airflow UI at http://localhost:8080 (admin/admin).

### 5. Run the Pipeline

The DAGs are connected via **data-aware scheduling**:

```
bronze_batch_ingest → silver_transform → gold_aggregate → ml_training_pipeline
                                                     └──→ data_validation
```

1. Trigger `bronze_batch_ingest` manually
2. `silver_transform` fires automatically when Bronze completes
3. `gold_aggregate` fires when Silver completes
4. `ml_training_pipeline` fires when Gold completes

## 📊 ML Models (BigQuery ML)

| Model | Type | Target | Features |
|-------|------|--------|----------|
| Star Rating Prediction | `LINEAR_REG` | Review stars (1-5) | Business avg stars, user history, text length |
| Elite User Prediction | `LOGISTIC_REG` | Elite status (0/1) | Review count, compliments, friend count |
| Business Clustering | `KMEANS` | 6 segments | Stars, review count, location |

## 🔐 PII Handling

Uses **GCP Sensitive Data Protection** (DLP):
- **Detection**: 150+ InfoTypes (PERSON_NAME, EMAIL, PHONE, ADDRESS)
- **Masking**: Column-level policy tags with data masking rules
- **Reversible**: `DLP_DETERMINISTIC_ENCRYPT` with KMS keys
- **Scope**: `user.name`, `review.text`, `tip.text`, `business.address`

## 🔔 Alerting

| Alert | Channel | Trigger |
|-------|---------|---------|
| DAG Failure | Email (wira.hutomo2@gmail.com) | Any task failure |
| Duration Degradation | Email | DAG run > threshold |
| Scheduler Down | Email | No heartbeat for 10min |

## 📝 License

This project uses the [Yelp Open Dataset](https://www.yelp.com/dataset) which is subject to the Yelp Dataset License Agreement.
