from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ─── Session ─────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("BankingTransformations") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,"
            "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

PG_URL  = "jdbc:postgresql://postgres:5432/banking_dw"
PG_PROPS = {"user": "airflow", "password": "airflow", "driver": "org.postgresql.Driver"}

# ─── Read raw from PostgreSQL ─────────────────────────────
raw = spark.read.jdbc(url=PG_URL, table="raw_transactions", properties=PG_PROPS)

# ─── Transformations ──────────────────────────────────────
cleaned = raw \
    .dropDuplicates(["transaction_id"]) \
    .filter(F.col("transaction_id").isNotNull()) \
    .filter(F.col("client_id").isNotNull()) \
    .filter(F.col("montant").isNotNull()) \
    .withColumn("date_transaction",
                F.to_date(F.col("date_transaction"), "yyyy-MM-dd")) \
    .withColumn("montant",         F.col("montant").cast(DoubleType())) \
    .withColumn("montant_eur",     F.col("montant_eur").cast(DoubleType())) \
    .withColumn("taux_change_eur", F.col("taux_change_eur").cast(DoubleType())) \
    .withColumn("score_credit_client", F.col("score_credit_client").cast(DoubleType())) \
    .withColumn("solde_avant",     F.col("solde_avant").cast(DoubleType())) \
    .withColumn("taux_interet",    F.col("taux_interet").cast(DoubleType())) \
    .withColumn("montant_eur",
                F.when(F.col("montant_eur").isNull(),
                       F.col("montant") * F.col("taux_change_eur"))
                .otherwise(F.col("montant_eur"))) \
    .withColumn("solde_apres",
                F.col("solde_avant") + F.col("montant")) \
    .withColumn("is_debit",
                F.lower(F.col("type_operation")).isin(["debit", "retrait", "virement_sortant"])) \
    .withColumn("categorie",   F.upper(F.trim(F.col("categorie")))) \
    .withColumn("statut",      F.upper(F.trim(F.col("statut")))) \
    .withColumn("devise",      F.upper(F.trim(F.col("devise")))) \
    .withColumn("processed_at", F.current_timestamp())

# ─── Write fact table ─────────────────────────────────────
cleaned.select(
    "transaction_id", "client_id", "date_transaction", "montant",
    "devise", "taux_change_eur", "montant_eur", "categorie", "produit",
    "agence", "type_operation", "statut", "score_credit_client",
    "segment_client", "solde_avant", "solde_apres", "taux_interet",
    "is_debit", "processed_at"
).write.jdbc(
    url=PG_URL,
    table="fact_transactions",
    mode="append",
    properties=PG_PROPS
)

# ─── Upsert dim_clients ───────────────────────────────────
clients = cleaned.groupBy("client_id").agg(
    F.first("segment_client").alias("segment_client"),
    F.avg("score_credit_client").alias("score_credit_client"),
    F.min("date_transaction").alias("first_seen"),
    F.max("date_transaction").alias("last_seen"),
    F.count("*").alias("total_transactions")
)

clients.write.jdbc(
    url=PG_URL,
    table="dim_clients",
    mode="append",
    properties=PG_PROPS
)

# ─── Daily aggregation ────────────────────────────────────
daily = cleaned.groupBy("date_transaction", "categorie", "agence").agg(
    F.count("*").alias("nb_transactions"),
    F.sum("montant_eur").alias("total_montant_eur"),
    F.avg("score_credit_client").alias("avg_score_credit"),
    F.sum(F.when(F.col("is_debit"), 1).otherwise(0)).alias("nb_debits"),
    F.sum(F.when(~F.col("is_debit"), 1).otherwise(0)).alias("nb_credits")
)

daily.write.jdbc(
    url=PG_URL,
    table="agg_daily_summary",
    mode="append",
    properties=PG_PROPS
)

print("✅ Spark transformations complete")
spark.stop()
