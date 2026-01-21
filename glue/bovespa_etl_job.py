"""
AWS Glue ETL Job - BOVESPA
FIAP Tech Challenge Fase 2

Transformacoes: Renomeia colunas, calcula medias moveis e agrega dados por ticker.
Saida: Parquet particionado em refined/ + catalogo Glue.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Inicializacao do Job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_PATH', 'OUTPUT_PATH', 'DATABASE_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

INPUT_PATH = args['INPUT_PATH']
OUTPUT_PATH = args['OUTPUT_PATH']
DATABASE_NAME = args['DATABASE_NAME']

print(f"[INICIO] Input: {INPUT_PATH} | Output: {OUTPUT_PATH} | DB: {DATABASE_NAME}")

# 1. Leitura dos dados brutos
df_raw = spark.read.parquet(INPUT_PATH)
print(f"[1] Leitura: {df_raw.count()} registros")

# 2. Renomeia colunas (Req 5B): open -> preco_abertura, close -> preco_fechamento
df_renamed = df_raw \
    .withColumnRenamed("open", "preco_abertura") \
    .withColumnRenamed("close", "preco_fechamento")

# 3. Calcula medias moveis e variacao diaria (Req 5C)
window_7d = Window.partitionBy("ticker").orderBy("date").rowsBetween(-6, 0)
window_14d = Window.partitionBy("ticker").orderBy("date").rowsBetween(-13, 0)

df_with_moving_avg = df_renamed \
    .withColumn("media_movel_7d", F.round(F.avg("preco_fechamento").over(window_7d), 2)) \
    .withColumn("media_movel_14d", F.round(F.avg("preco_fechamento").over(window_14d), 2)) \
    .withColumn("variacao_diaria", F.round(
        ((F.col("preco_fechamento") - F.col("preco_abertura")) / F.col("preco_abertura")) * 100, 2))

# 4. Agrega metricas por ticker (Req 5A): medias, soma volume, contagem
df_agregado = df_with_moving_avg.groupBy("ticker", "nome_acao", "data_processamento") \
    .agg(
        F.avg("preco_abertura").alias("media_abertura"),
        F.avg("preco_fechamento").alias("media_fechamento"),
        F.sum("volume").alias("volume_total"),
        F.count("*").alias("total_registros"),
        F.max("high").alias("preco_maximo"),
        F.min("low").alias("preco_minimo"),
        F.last("media_movel_7d").alias("ultima_media_movel_7d"),
        F.last("media_movel_14d").alias("ultima_media_movel_14d"),
        F.avg("variacao_diaria").alias("variacao_media_diaria")
    )
print(f"[4] Agregacao: {df_agregado.count()} registros")

# 5. Metricas derivadas: variacao percentual, amplitude e tendencia
df_final = df_agregado \
    .withColumn("variacao_percentual", F.round(
        ((F.col("media_fechamento") - F.col("media_abertura")) / F.col("media_abertura")) * 100, 2)) \
    .withColumn("amplitude", F.round(F.col("preco_maximo") - F.col("preco_minimo"), 2)) \
    .withColumn("tendencia", F.when(
        F.col("ultima_media_movel_7d") > F.col("ultima_media_movel_14d"), "ALTA"
    ).otherwise("BAIXA")) \
    .withColumn("data_extracao", F.current_date())

df_final.show(5, truncate=False)

# 6. Salva dados refinados em parquet particionado (Req 6)
df_final.write \
    .mode("overwrite") \
    .partitionBy("data_processamento", "nome_acao") \
    .parquet(OUTPUT_PATH)
print(f"[6] Dados salvos em: {OUTPUT_PATH}")

# 7. Cataloga no Glue Catalog (Req 7)
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
df_final.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", OUTPUT_PATH) \
    .partitionBy("data_processamento", "nome_acao") \
    .saveAsTable(f"{DATABASE_NAME}.bovespa_refined")
print(f"[7] Tabela catalogada: {DATABASE_NAME}.bovespa_refined")

print("[FIM] Job concluido com sucesso!")
job.commit()
