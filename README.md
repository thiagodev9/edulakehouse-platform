# EduLakehouse Platform

Pipeline de dados educacionais com arquitetura **Medallion (Bronze → Silver → Gold)**, construído com **Apache Spark**, **Delta Lake** e **Apache Airflow**.

O projeto ingere dados públicos da **API do IBGE** (municípios do Brasil), transforma-os em camadas incrementais versionadas (Delta Lake) e produz datasets analíticos agregados prontos para consumo em ferramentas de BI.

---

## Índice

- [Visão geral](#visão-geral)
- [Arquitetura](#arquitetura)
- [Stack de tecnologias](#stack-de-tecnologias)
- [Estrutura do projeto](#estrutura-do-projeto)
- [Como executar](#como-executar)
- [Pipelines](#pipelines)
- [Framework interno](#framework-interno)
- [Configuração](#configuração)
- [Delta Lake](#delta-lake)
- [Apache Airflow — DAGs](#apache-airflow--dags)
- [Testes](#testes)
- [CI/CD — GitHub Actions](#cicd--github-actions)
- [Resultados](#resultados)

---

## Visão geral

O EduLakehouse automatiza o ciclo completo de um pipeline de dados:

1. **Extração** — baixa o JSON bruto da API pública do IBGE (municípios, UFs e regiões do Brasil).
2. **Bronze** — ingere o JSON e grava em Parquet, particionado por ano/mês, com metadados de auditoria.
3. **Silver** — normaliza (flatten) as estruturas aninhadas e grava em Delta Lake, particionado por UF, com MERGE incremental.
4. **Gold** — agrega os dados (municípios por UF, municípios por região, estados por região) em Delta Lake, particionado por região, pronto para dashboards.

Cada camada é orquestrada por DAGs do Airflow, tem testes unitários próprios, validação de schema, checagem de qualidade de dados e auditoria de execução.

---

## Arquitetura

```text
API IBGE
    │
    ▼
data/landing/ibge/        ← JSON bruto
    │
    ▼  Bronze Pipeline
data/bronze/ibge/         ← Parquet  (partição: year/month)
    │
    ▼  Silver Pipeline
data/silver/ibge/         ← Delta Lake (partição: uf_sigla)
    │
    ▼  Gold Pipeline
data/gold/ibge_dashboard/ ← Delta Lake (partição: regiao_nome)
    │
    ▼
Power BI / Looker Studio / Metabase
```

Cada pipeline herda de `BasePipeline` (`framework/base_pipeline.py`), que define o contrato `extract → transform → load → run`, garantindo que Bronze, Silver e Gold sigam a mesma estrutura e recebam logging, auditoria e sessão Spark de forma consistente.

---

## Stack de tecnologias

| Camada        | Tecnologia                              |
|---------------|-----------------------------------------|
| Processamento | PySpark 4.0.3                           |
| Storage       | Delta Lake 4.0.1 (ACID + Time Travel)   |
| Orquestração  | Apache Airflow 2.8 (via Docker)         |
| Qualidade     | Framework próprio (DataQuality)         |
| Testes        | pytest 8.3 + pytest-cov 5.0             |
| CI/CD         | GitHub Actions                          |
| Containers    | Docker + Docker Compose                 |
| Object Store  | MinIO (S3-compatible)                   |
| Banco         | PostgreSQL 15 (metadata Airflow)        |
| Linguagem     | Python 3.11+                            |

---

## Estrutura do projeto

```text
edulakehouse-platform/
├── dags/                               # Airflow DAGs
│   ├── ibge_lakehouse_dag.py           # DAG master: Bronze → Silver → Gold
│   ├── bronze_dag.py                   # DAG Bronze independente
│   ├── silver_dag.py                   # DAG Silver (aguarda Bronze)
│   └── gold_dag.py                     # DAG Gold (aguarda Silver)
├── data/
│   ├── landing/ibge/                   # JSON bruto da API IBGE
│   ├── bronze/ibge/                    # Parquet (year/month)
│   ├── silver/ibge/                    # Delta Lake (uf_sigla)
│   └── gold/ibge_dashboard/            # Delta Lake (regiao_nome)
├── framework/                          # Módulos reutilizáveis
│   ├── audit.py                        # AuditManager — registra execuções
│   ├── base_pipeline.py                # BasePipeline (ABC): extract/transform/load/run
│   ├── config.py                       # Leitura do config/config.yaml
│   ├── delta_utils.py                  # MERGE, OPTIMIZE, VACUUM, Time Travel
│   ├── logger.py                       # LoggerManager (singleton, via loguru)
│   ├── monitoring.py                   # PipelineMonitor — métricas de execução
│   ├── quality.py                      # DataQuality — validação de nulos/duplicados
│   ├── schema_validator.py             # SchemaValidator
│   └── spark.py                        # SparkManager (singleton da SparkSession)
├── pipelines/
│   ├── bronze/ibge_pipeline.py         # Ingestão JSON → Parquet
│   ├── silver/ibge_silver_pipeline.py  # Flatten + Delta MERGE
│   └── gold/ibge_gold_pipeline.py      # Agregação + Delta MERGE
├── scripts/
│   └── download/
│       ├── download_dataset.py         # Baixa dados da API IBGE
│       └── setup_hadoop.py             # Configura winutils/Hadoop no Windows
├── config/
│   └── config.yaml                     # Paths, retries, logging, quality gates
├── tests/
│   ├── conftest.py                     # SparkSession + fixtures de pipeline
│   ├── test_bronze_transform.py
│   ├── test_silver_transform.py
│   ├── test_gold_transform.py
│   └── test_quality.py
├── logs/
│   ├── audit/                          # audit_*.json
│   ├── metrics/                        # metrics_*.json
│   └── quality/                        # quality_*.json
├── .github/workflows/ci.yml            # Lint → Tests → Pipeline Integration
├── docker-compose.yml                  # Airflow + MinIO + Postgres
├── Dockerfile
├── .env.example
└── requirements.txt
```

---

## Como executar

### Pré-requisitos

- Python 3.11
- Java 11 (obrigatório para PySpark)
- Git
- Docker + Docker Compose (para rodar o Airflow completo)

### 1. Setup local

```bash
git clone https://github.com/thiagodev9/edulakehouse-platform.git
cd edulakehouse-platform

python -m venv .venv
source .venv/bin/activate        # Linux/Mac
.venv\Scripts\activate           # Windows

pip install -r requirements.txt
```

> No Windows, PySpark precisa do `winutils.exe`/Hadoop. Use `python scripts/download/setup_hadoop.py` para configurar automaticamente.

### 2. Baixar os dados da API IBGE

```bash
python scripts/download/download_dataset.py
```

### 3. Executar as pipelines manualmente

```bash
# Bronze — ingestão do JSON
python -m pipelines.bronze.ibge_pipeline

# Silver — flatten + Delta Lake
python -m pipelines.silver.ibge_silver_pipeline

# Gold — agregação + Delta Lake
python -m pipelines.gold.ibge_gold_pipeline
```

### 4. Executar com Docker (Airflow completo)

```bash
cp .env.example .env
# Edite .env com suas credenciais se necessário

docker compose up --build -d

# Airflow UI:   http://localhost:8080  (login: admin / admin)
# MinIO Console: http://localhost:9001
```

### 5. Ativar a DAG no Airflow

1. Acesse `http://localhost:8080`
2. Habilite a DAG `ibge_lakehouse`
3. Clique em **Trigger DAG** para executar manualmente

### 6. Executar os testes

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
- **Destino:** `data/silver/ibge/` (Delta Lake, particionado por `uf_sigla`)
- **Transformações:** flatten dos structs aninhados → colunas planas
- **Delta:** MERGE incremental por `municipio_id` + OPTIMIZE + VACUUM

### Gold

- **Fonte:** Silver Delta
- **Destino:** `data/gold/ibge_dashboard/` (Delta Lake, particionado por `regiao_nome`)
- **Agregações:**
  - Municípios por UF (`uf_sigla`, `total_municipios`)
  - Municípios por Região
  - Estados por Região
- **Delta:** MERGE incremental por `uf_sigla` + OPTIMIZE + VACUUM

---

## Framework interno

Módulos reutilizados por todas as pipelines, em `framework/`:

| Módulo               | Responsabilidade                                                   |
|-----------------------|--------------------------------------------------------------------|
| `base_pipeline.py`    | Contrato ABC `extract → transform → load → run` para toda pipeline |
| `spark.py`            | `SparkManager` — singleton da SparkSession, já configurado com Delta |
| `logger.py`           | `LoggerManager` — singleton de logging estruturado (loguru)         |
| `audit.py`            | `AuditManager` — grava metadados de cada execução em `logs/audit/`  |
| `monitoring.py`       | `PipelineMonitor` — métricas de execução em `logs/metrics/`         |
| `quality.py`          | `DataQuality` — valida nulos e duplicados, grava relatório em `logs/quality/` |
| `schema_validator.py` | `SchemaValidator` — valida schema esperado antes da transformação   |
| `delta_utils.py`      | Helpers de MERGE, OPTIMIZE, VACUUM e Time Travel para tabelas Delta |
| `config.py`           | Leitura tipada do `config/config.yaml`                              |

---

## Configuração

Parâmetros globais ficam em `config/config.yaml`:

```yaml
pipeline:
  retries: 3
  timeout: 600
  repartitions: 4

quality:
  fail_on_nulls: true
  fail_on_duplicates: true

save:
  audit: true
  quality: true
  metrics: true
```

Credenciais e variáveis de ambiente (MinIO, Airflow) ficam em `.env` (veja `.env.example`).

---

## Delta Lake

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

## Apache Airflow — DAGs

| DAG                | Schedule     | Dependência                      |
|---------------------|--------------|-----------------------------------|
| `ibge_bronze`       | `0 1 * * *`  | —                                 |
| `ibge_silver`       | `0 2 * * *`  | aguarda Bronze                    |
| `ibge_gold`         | `0 3 * * *`  | aguarda Silver                    |
| `ibge_lakehouse`    | `@daily`     | Bronze → Silver → Gold (master)   |

Cada task tem **3 retries** com intervalo de 5 minutos e timeout de 2 horas (configurável em `config/config.yaml`).

---

## Testes

```bash
pytest tests/ -v --cov=pipelines --cov=framework --cov-report=term-missing
```

| Arquivo                    | Cobre                                       |
|------------------------------|----------------------------------------------|
| `test_bronze_transform.py` | Ingestão e enriquecimento de metadados Bronze |
| `test_silver_transform.py`| Flatten de structs e transformação Silver     |
| `test_gold_transform.py`  | Agregações da camada Gold                     |
| `test_quality.py`         | Validação de nulos e duplicados (DataQuality) |

`tests/conftest.py` fornece uma `SparkSession` de teste e fixtures compartilhadas entre as camadas.

---

## CI/CD — GitHub Actions

```text
push/PR
  │
  ├─► Lint (flake8)
  │
  ├─► Tests (pytest + coverage)
  │
  └─► Pipeline Integration (apenas em main)
        ├─ Download IBGE data
        ├─ Bronze
        ├─ Silver
        └─ Gold → upload artifacts
```

---

## Resultados

### Gold — Municípios por UF (amostra)

| UF | Nome           | Total Municípios |
|----|----------------|-------------------|
| MG | Minas Gerais   | 853               |
| SP | São Paulo      | 645               |
| BA | Bahia          | 417               |
| PR | Paraná         | 399               |

### Gold — Estados por Região

| Região       | Estados |
|---------------|---------|
| Nordeste      | 9       |
| Sudeste       | 4       |
| Norte         | 7       |
| Sul           | 3       |
| Centro-Oeste  | 4       |
