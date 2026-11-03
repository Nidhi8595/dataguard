from pyspark import pipelines as dp
from pyspark.sql import functions as F

REGISTRY_TABLE = "dataguard.bronze.batch_registry_phase5"
HISTORY_TABLE = "dataguard.silver.reading_history_phase5"
VALIDATION_TABLE = "deduplicated_readings_phase5"


@dp.materialized_view(
    name="dataguard.gold.batch_delivery_metrics", comment="Operational metrics for incoming energy reading deliveries."
)
def batch_delivery_metrics():

    registry = spark.read.table(REGISTRY_TABLE)

    accepted_metrics = (
        spark.read.table(HISTORY_TABLE).groupBy("ingestion_batch_id").agg(F.count("*").alias("accepted_records"))
    )
    rejected_metrics = (
        spark.read.table(VALIDATION_TABLE)
        .filter(F.col("validation_reason") != "")
        .groupBy("ingestion_batch_id")
        .agg(F.count("*").alias("rejected_records"))
    )
    metrics = registry.join(accepted_metrics, on="ingestion_batch_id", how="left").join(
        rejected_metrics, on="ingestion_batch_id", how="left"
    )
    return metrics.select(
        "ingestion_batch_id",
        "file_name",
        "batch_status",
        "canonical_batch_id",
        "duplicate_of_batch_id",
        F.col("record_count").alias("delivered_records"),
        F.coalesce(F.col("accepted_records"), F.lit(0)).alias("accepted_records"),
        F.coalesce(F.col("rejected_records"), F.lit(0)).alias("rejected_records"),
        "batch_created_at",
    )
