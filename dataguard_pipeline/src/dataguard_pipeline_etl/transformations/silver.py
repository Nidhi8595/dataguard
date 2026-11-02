from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


BRONZE_TABLE = "dataguard.bronze.bronze_energy_readings"


@dp.temporary_view
def validated_energy_readings():

    df = spark.read.table(BRONZE_TABLE)

    df = df.withColumn("parsed_timestamp", F.try_to_timestamp("reading_timestamp")).withColumn(
        "parsed_consumption", F.expr("try_cast(energy_consumption AS DOUBLE)")
    )

    consumption = F.col("parsed_consumption")

    invalid_consumption = consumption.isNull() | F.isnan(consumption) | (F.abs(consumption) == float("inf"))

    return df.withColumn(
        "validation_reason",
        F.concat_ws(
            "; ",
            F.when(F.col("building_id").isNull() | (F.trim("building_id") == ""), "Missing building_id"),
            F.when(F.col("meter_id").isNull() | (F.trim("meter_id") == ""), "Missing meter_id"),
            F.when(
                F.col("reading_timestamp").isNull() | (F.trim("reading_timestamp") == ""), "Missing reading_timestamp"
            ),
            F.when(
                F.col("parsed_timestamp").isNull()
                & F.col("reading_timestamp").isNotNull()
                & (F.trim("reading_timestamp") != ""),
                "Invalid reading_timestamp",
            ),
            F.when(F.col("source_batch_id").isNull() | (F.trim("source_batch_id") == ""), "Missing source_batch_id"),
            F.when(
                F.col("energy_consumption").isNull() | (F.trim("energy_consumption") == ""),
                "Missing energy_consumption",
            ),
            F.when(invalid_consumption, "Invalid energy_consumption"),
            F.when(consumption < 0, "Negative energy consumption"),
            F.when(F.col("_rescued_data").isNotNull(), "Unexpected source columns or incompatible values"),
        ),
    )


@dp.temporary_view
def deduplicated_energy_readings():

    df = spark.read.table("validated_energy_readings")

    valid_key = (
        F.col("building_id").isNotNull()
        & (F.trim("building_id") != "")
        & F.col("meter_id").isNotNull()
        & (F.trim("meter_id") != "")
        & F.col("parsed_timestamp").isNotNull()
    )

    duplicate_window = Window.partitionBy("building_id", "meter_id", "parsed_timestamp").orderBy(
        F.when(F.col("validation_reason") == "", 0).otherwise(1),
        F.col("source_file").asc_nulls_last(),
        F.col("source_batch_id").asc_nulls_last(),
        F.col("ingested_at").asc_nulls_last(),
        F.sha2(F.to_json(F.struct(*[F.col(c) for c in df.columns if c != "ingested_at"])), 256).asc(),
    )

    ranked = df.withColumn("duplicate_rank", F.when(valid_key, F.row_number().over(duplicate_window)).otherwise(1))

    return ranked.withColumn(
        "validation_reason",
        F.concat_ws("; ", F.col("validation_reason"), F.when(F.col("duplicate_rank") > 1, "Duplicate reading")),
    )


@dp.materialized_view(
    name="dataguard.silver.silver_energy_readings", comment="Validated and deduplicated energy meter readings."
)
def silver_energy_readings():

    df = spark.read.table("deduplicated_energy_readings")

    return df.filter(F.col("validation_reason") == "").select(
        "building_id",
        "meter_id",
        F.col("parsed_timestamp").alias("reading_timestamp"),
        F.col("parsed_consumption").alias("energy_consumption"),
        "source_batch_id",
        "ingested_at",
        "source_file",
    )


@dp.materialized_view(
    name="dataguard.silver.quarantined_energy_readings", comment="Rejected readings with traceable validation reasons."
)
def quarantined_energy_readings():

    df = spark.read.table("deduplicated_energy_readings")

    return df.filter(F.col("validation_reason") != "").select(
        "building_id",
        "meter_id",
        "reading_timestamp",
        "energy_consumption",
        "source_batch_id",
        "source_file",
        "ingested_at",
        "validation_reason",
    )
