from pyspark import pipelines as dp
from pyspark.sql import functions as F


SILVER_TABLE = "dataguard.silver.silver_energy_readings"


@dp.materialized_view(name="dataguard.gold.gold_energy_metrics")
def gold_energy_metrics():

    df = spark.read.table(SILVER_TABLE)

    return (
        df.withColumn("reading_date", F.to_date("reading_timestamp"))
        .groupBy("building_id", "reading_date")
        .agg(
            F.count("*").alias("total_readings"),
            F.sum("energy_consumption").alias("total_energy_consumption"),
            F.avg("energy_consumption").alias("average_energy_consumption"),
            F.max("energy_consumption").alias("maximum_energy_consumption"),
            F.countDistinct("meter_id").alias("active_meter_count"),
        )
    )
