# EduLakehouse Platform

Pipeline de dados educacionais com arquitetura **Medallion (Bronze → Silver → Gold)** usando Apache Spark, Delta Lake e Apache Airflow.

---

## Arquitetura

```
API IBGE
    │
    ▼
data/landing/         ← JSON bruto
    │
    ▼  Bronze Pipeline
data/bronze/ibge/     ← Parquet  (partição: year/month)
    │
    ▼  Silver Pipeline
data/silver/ibge/     ← Delta Lake (partição: uf_sigla)
    │
    ▼  Gold Pipeline
data/gold/ibge_dashboard/  ← Delta Lake (partição: regiao_nome)
    │
    ▼
Power BI / Looker Studio / Metabase
```

---

## Stack de Tecnologias

| Camada        | Tecnologia                              |
|---------------|-----------------------------------------|
| Processamento | PySpark 4.0.3                           |
| Storage       | Delta Lake 4.0.1 (ACID + Time Travel)  |
| Orquestração  | Apache Airflow 2.8 (via Docker)         |
| Qualidade     | Framework próprio (DataQuality)         |
| Testes        | pytest 8.3 + pytest-cov 5.0            |
| CI/CD         | GitHub Actions                          |
| Containers    | Docker + Docker Compose                 |
| Object Store  | MinIO (S3-compatible)                   |
| Banco         | PostgreSQL 15 (metadata Airflow)        |
| Linguagem     | Python 3.9+                             |

---

## Estrutura do Projeto

```
edulakehouse-platform/
├── dags/                          # Sprint 22 — Airflow DAGs
│   ├── ibge_lakehouse_dag.py      # DAG master: Bronze → Silver → Gold
│   ├── bronze_dag.py              # DAG Bronze independente
│   ├── silver_dag.py              # DAG Silver (aguarda Bronze)
│   └── gold_dag.py                # DAG Gold (aguarda Silver)
├── data/
│   ├── landing/ibge/              # JSON bruto da API IBGE
│   ├── bronze/ibge/               # Parquet (year/month)
│   ├── silver/ibge/               # Delta Lake (uf_sigla)
│   └── gold/ibge_dashboard/       # Delta Lake (regiao_nome)
├── framework/                     # Módulos reutilizáveis
│   ├── audit.py                   # AuditManager
│   ├── delta_utils.py             # Sprint 23 — MERGE, OPTIMIZE, VACUUM, Time Travel
│   ├── monitoring.py              # PipelineMonitor
│   ├── quality.py                 # DataQuality (Silver)
│   └── spark.py                   # SparkManager (singleton)
├── pipelines/
│   ├── bronze/ibge_pipeline.py    # Ingestão JSON → Parquet
│   ├── silver/ibge_silver_pipeline.py  # Flatten + Delta MERGE
│   └── gold/ibge_gold_pipeline.py      # Agregação + Delta MERGE
├── tests/                         # Sprint 26 — pytest
│   ├── conftest.py                # SparkSession fixture
│   ├── test_bronze_transform.py
│   ├── test_silver_transform.py
│   ├── test_gold_transform.py
│   └── test_quality.py
├── logs/
│   ├── audit/                     # audit_*.json
│   ├── metrics/                   # metrics_*.json
│   └── quality/                   # quality_*.json
├── .github/workflows/ci.yml       # Sprint 25 — CI/CD
├── docker-compose.yml             # Sprint 24 — Airflow + MinIO + Postgres
├── Dockerfile
└── requirements.txt
```

---

## Como Executar

### Pré-requisitos

- Python 3.11
- Java 11 (obrigatório para PySpark)
- Git

### 1. Setup local

```bash
git clone <repo>
cd edulakehouse-platform

python -m venv .venv
source .venv/bin/activate        # Linux/Mac
.venv\Scripts\activate           # Windows

pip install -r requirements.txt
```

### 2. Executar as pipelines manualmente

```bash
# Bronze — ingestão do JSON
python -m pipelines.bronze.ibge_pipeline

# Silver — flatten + Delta Lake
python -m pipelines.silver.ibge_silver_pipeline

# Gold — agregação + Delta Lake
python -m pipelines.gold.ibge_gold_pipeline
```

### 3. Executar com Docker (Airflow completo)

```bash
cp .env.example .env

docker compose up --build -d

# Acesse o Airflow UI em http://localhost:8080
# Login: admin / admin

# MinIO Console em http://localhost:9001
```

### 4. Ativar a DAG no Airflow

1. Acesse `http://localhost:8080`
2. Habilite a DAG `ibge_lakehouse`
3. Clique em **Trigger DAG** para executar manualmente

### 5. Executar os testes

```bash
pytest tests/ -v --cov=pipelines --cov=framework
```

---

## Pipelines

### Bronze

- **Fonte:** `data/landing/ibge/municipios.json` (API IBGE)
- **Destino:** `data/bronze/ibge/` (Parquet, particionado por year/month)
- **Metadados adicionados:** `ingestion_timestamp`, `source_system`, `file_name`, `year`, `month`

### Silver

- **Fonte:** Bronze Parquet
- **Destino:** `data/silver/ibge/` (Delta Lake, particionado por uf_sigla)
- **Transformações:** Flatten dos structs aninhados → colunas flat
- **Delta:** MERGE incremental por `municipio_id` + OPTIMIZE + VACUUM

### Gold

- **Fonte:** Silver Delta
- **Destino:** `data/gold/ibge_dashboard/` (Delta Lake, particionado por regiao_nome)
- **Agregações:**
  - Municípios por UF (`uf_sigla`, `total_municipios`)
  - Municípios por Região
  - Estados por Região
- **Delta:** MERGE incremental por `uf_sigla` + OPTIMIZE + VACUUM

---

## Delta Lake — Funcionalidades (Sprint 23)

```python
from framework.delta_utils import DeltaUtils

# Time Travel — ler versão anterior
df_v0 = DeltaUtils.read(spark, "data/silver/ibge", version=0)
df_ontem = DeltaUtils.read(spark, "data/silver/ibge", timestamp="2025-06-01")

# Histórico de operações
DeltaUtils.print_history(spark, "data/silver/ibge")

# Compactação de small files
DeltaUtils.optimize(spark, "data/silver/ibge")

# Limpeza de arquivos antigos (7 dias)
DeltaUtils.vacuum(spark, "data/silver/ibge", retention_hours=168)
```

---

## Apache Airflow — DAGs (Sprint 22)

| DAG                | Schedule     | Dependência         |
|--------------------|--------------|---------------------|
| `ibge_bronze`      | `0 1 * * *`  | —                   |
| `ibge_silver`      | `0 2 * * *`  | aguarda Bronze      |
| `ibge_gold`        | `0 3 * * *`  | aguarda Silver      |
| `ibge_lakehouse`   | `@daily`     | Bronze → Silver → Gold (master) |

Cada task tem **3 retries** com intervalo de 5 minutos e timeout de 2 horas.

---

## Resultados

### Gold — Municípios por UF (amostra)

| UF | Nome           | Total Municípios |
|----|----------------|-----------------|
| MG | Minas Gerais   | 853             |
| SP | São Paulo      | 645             |
| BA | Bahia          | 417             |
| PR | Paraná         | 399             |

### Gold — Estados por Região

| Região       | Estados |
|--------------|---------|
| Nordeste     | 9       |
| Sudeste      | 4       |
| Norte        | 7       |
| Sul          | 3       |
| Centro-Oeste | 4       |

---

## CI/CD — GitHub Actions (Sprint 25)

```
push/PR
  │
  ├─► Lint (flake8)
  │
  ├─► Tests (pytest + coverage)
  │
  └─► Pipeline Integration (apenas em main)
        ├─ Bronze
        ├─ Silver
        └─ Gold → upload artifacts
```

---

## Sprints

| Sprint | Tema                    | Status |
|--------|-------------------------|--------|
| 20     | Camada Silver           | ✅     |
| 21     | Camada Gold             | ✅     |
| 22     | Apache Airflow (DAGs)   | ✅     |
| 23     | Delta Lake              | ✅     |
| 24     | Docker + Docker Compose | ✅     |
| 25     | GitHub Actions (CI/CD)  | ✅     |
| 26     | Testes Automatizados    | ✅     |
| 27     | Dashboard (Power BI)    | 🚧     |
| 28     | Documentação            | ✅     |
| 29     | Deploy na Cloud         | 🚧     |
| 30     | Projeto Enterprise      | 🚧     |
