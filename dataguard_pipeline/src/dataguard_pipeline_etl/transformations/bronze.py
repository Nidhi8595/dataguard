from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
from pyspark.sql.functions import regexp_extract, to_timestamp
from pyspark.sql.window import Window as W

SOURCE_PATH = "/Volumes/dataguard/sample_data/dataguard_files/energy_readings.csv"

raw_schema = StructType(
    [
        StructField("building_id", StringType(), True),
        StructField("meter_id", StringType(), True),
        StructField("reading_timestamp", StringType(), True),
        StructField("energy_consumption", StringType(), True),
        StructField("source_batch_id", StringType(), True),
    ]
)


@dp.table(name="dataguard.bronze.bronze_energy_readings", comment="Raw energy meter readings with ingestion metadata.")
def bronze_energy_readings():

    raw_data = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .option("header", "true")
        .schema(raw_schema)
        .load(SOURCE_PATH)
    )

    return raw_data.withColumn("ingested_at", current_timestamp()).withColumn("source_file", col("_metadata.file_path"))


PHASE5_SOURCE_PATH = "/Volumes/dataguard/sample_data/dataguard_files/phase5_incoming/"
PHASE5_MANIFEST_PATH = "/Volumes/dataguard/sample_data/dataguard_files/phase5_manifests/"


@dp.table(
    name="dataguard.bronze.bronze_energy_readings_phase5",
    comment="Phase 5 raw readings enriched with delivery manifest metadata.",
)
def bronze_energy_readings_phase5():

    # Read incoming CSV files incrementally.
    raw_data = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .option("header", "true")
        .option("pathGlobFilter", "*.csv")
        .schema(raw_schema)
        .load(PHASE5_SOURCE_PATH)
    )

    # Extract the delivered filename from file metadata.
    raw_data = (
        raw_data.withColumn("source_file", col("_metadata.file_path"))
        .withColumn("file_name", regexp_extract(col("_metadata.file_path"), r"([^/]+)$", 1))
        .withColumn("ingested_at", current_timestamp())
    )

    # Read the delivery manifests separately.
    manifests = (
        spark.read.option("multiLine", "true")
        .json(PHASE5_MANIFEST_PATH)
        .select(
            "file_name",
            "ingestion_batch_id",
            "content_hash",
            "record_count",
            to_timestamp("created_at").alias("batch_created_at"),
        )
    )

    # Enrich each reading with its delivery identity.
    enriched_data = raw_data.join(manifests, on="file_name", how="left")

    return enriched_data.withColumn(
        "manifest_match_status",
        F.when(F.col("ingestion_batch_id").isNotNull(), "MATCHED").otherwise("MISSING_MANIFEST"),
    )


@dp.materialized_view(
    name="dataguard.bronze.batch_registry_phase5",
    comment="Tracks delivery identity and detects duplicate file content.",
)
def batch_registry_phase5():

    manifests = (
        spark.read.option("multiLine", "true")
        .json(PHASE5_MANIFEST_PATH)
        .select(
            "ingestion_batch_id",
            "file_name",
            "content_hash",
            "record_count",
            to_timestamp("created_at").alias("batch_created_at"),
        )
    )

    delivery_window = (
        W.partitionBy("content_hash")
        .orderBy(F.col("batch_created_at").asc_nulls_last(), F.col("ingestion_batch_id").asc())
        .rowsBetween(W.unboundedPreceding, W.unboundedFollowing)
    )

    registry = manifests.withColumn("canonical_batch_id", F.first("ingestion_batch_id").over(delivery_window))

    return registry.withColumn(
        "batch_status",
        F.when(F.col("ingestion_batch_id") == F.col("canonical_batch_id"), "ACCEPTED").otherwise("DUPLICATE"),
    ).withColumn("duplicate_of_batch_id", F.when(F.col("batch_status") == "DUPLICATE", F.col("canonical_batch_id")))
