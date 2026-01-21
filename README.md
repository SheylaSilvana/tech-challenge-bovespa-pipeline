# Tech Challenge - Fase 2: Pipeline de Dados da Bovespa

## Machine Learning Engineering - FIAP PosTech

Pipeline de dados completo para coleta, processamento e analise de dados de acoes da B3 utilizando AWS S3, Glue, Lambda e Athena.

## Arquitetura

```
┌─────────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Script Python  │────▶│  S3 (raw/)  │────▶│   Lambda    │────▶│  Glue Job   │
│  (coleta dados) │     │  .parquet   │     │  (trigger)  │     │   (ETL)     │
└─────────────────┘     └─────────────┘     └─────────────┘     └──────┬──────┘
                                                                       │
                                                                       ▼
┌─────────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Athena        │◀────│ Glue Catalog│◀────│S3 (refined/)│◀────│             │
│  (queries)      │     │  (tabelas)  │     │  .parquet   │     │             │
└─────────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

## Requisitos Atendidos

| # | Requisito | Status |
|---|-----------|--------|
| 1 | Web Scraping para coletar informacoes das acoes | ✅ |
| 2 | Dados brutos ingeridos no S3 em formato parquet | ✅ |
| 3 | Ingestao particionada por data de processamento | ✅ |
| 4 | Lambda para iniciar Glue Job quando arquivo chegar no S3 | ✅ |
| 5A | Transformacao: Agrupamento de dados | ✅ |
| 5B | Transformacao: Renomear colunas | ✅ |
| 5C | Transformacao: Calculo com base em datas (media movel) | ✅ |
| 6 | Dados refinados particionados por data e nome_acao | ✅ |
| 7 | Catalogar automaticamente no Glue Catalog | ✅ |
| 8 | Consultas SQL no Athena | ✅ |

## Estrutura do Projeto

```
fase-2/
├── coleta_bovespa.py         # Script principal de coleta (conforme aulas)
├── lambda/
│   └── trigger_glue.py       # Lambda que inicia o Glue Job
├── glue/
│   └── bovespa_etl_job.py    # ETL Job com as 3 transformacoes
├── terraform/                 # Infrastructure as Code (opcional)
├── app/                       # Modulo Python organizado (opcional)
│   ├── collectors/           # Coletor de dados
│   ├── infrastructure/       # Adapter S3
│   ├── services/             # Pipeline service
│   └── cli.py                # Interface de linha de comando
├── tests/                     # Testes
└── docs/                      # Documentacao
```

## Quick Start

### 1. Configurar Credenciais AWS

```bash
# Copie as credenciais do AWS Academy para ~/.aws/credentials
[default]
aws_access_key_id=XXXXXX
aws_secret_access_key=XXXXXX
aws_session_token=XXXXXX
```

### 2. Instalar Dependencias

```bash
# Com pip
pip install yfinance pandas pyarrow boto3

# OU com Poetry
pip install poetry
poetry install
```

### 3. Configurar Bucket S3

Edite o arquivo `coleta_bovespa.py` e altere:
```python
BUCKET_NAME = "seu-bucket-aqui"
```

### 4. Executar Coleta

```bash
# Metodo simples (conforme aulas)
python coleta_bovespa.py

# OU com Poetry CLI
poetry run bovespa-collect collect
```

### 5. Verificar no AWS

1. **S3**: Verifique se o arquivo apareceu em `raw/data=YYYY-MM-DD/`
2. **Lambda**: Veja os logs em CloudWatch
3. **Glue**: Verifique o status do job `etl-bovespa-job`
4. **Athena**: Execute queries na tabela `bovespa_refined`

## Transformacoes do Glue Job

### Requisito 5A: Agrupamento
```python
df_agregado = df.groupBy("ticker", "nome_acao", "data_processamento").agg(
    F.avg("preco_abertura").alias("media_abertura"),
    F.sum("volume").alias("volume_total"),
    F.count("*").alias("total_registros")
)
```

### Requisito 5B: Renomear Colunas
```python
df_renamed = df_raw \
    .withColumnRenamed("open", "preco_abertura") \
    .withColumnRenamed("close", "preco_fechamento")
```

### Requisito 5C: Media Movel (Calculo Temporal)
```python
window_7d = Window.partitionBy("ticker").orderBy("date").rowsBetween(-6, 0)
df = df.withColumn("media_movel_7d", F.avg("preco_fechamento").over(window_7d))
```

## Queries SQL para Athena

```sql
-- Consulta basica
SELECT * FROM bovespa_refined LIMIT 10;

-- Top 5 por volume
SELECT ticker, nome_acao, volume_total
FROM bovespa_refined
ORDER BY volume_total DESC
LIMIT 5;

-- Acoes em tendencia de alta
SELECT ticker, nome_acao, tendencia, variacao_percentual
FROM bovespa_refined
WHERE tendencia = 'ALTA'
ORDER BY variacao_percentual DESC;

-- Analise de medias moveis
SELECT ticker, media_fechamento, ultima_media_movel_7d, ultima_media_movel_14d
FROM bovespa_refined;
```

## Acoes Monitoradas

| Ticker | Empresa |
|--------|---------|
| PETR4.SA | Petrobras |
| VALE3.SA | Vale |
| ITUB4.SA | Itau |
| BBDC4.SA | Bradesco |
| ABEV3.SA | Ambev |
| B3SA3.SA | B3 |
| WEGE3.SA | Weg |
| RENT3.SA | Localiza |
| BBAS3.SA | Banco do Brasil |
| MGLU3.SA | Magazine Luiza |

## Recursos AWS Necessarios

1. **S3 Bucket** com pastas: `raw/`, `refined/`, `athena-results/`
2. **Lambda Function**: `iniciar-glue-job-bovespa`
3. **Glue Database**: `bovespa_db`
4. **Glue Job**: `etl-bovespa-job`
5. **IAM Role**: `LabRole` (AWS Academy)

