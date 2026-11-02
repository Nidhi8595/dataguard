from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import StructType, StructField, StringType

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
