from pyspark import pipelines as dp
from pyspark.sql.functions import col

@dp.expect_all_or_drop({
    "cpu_valid": "cpu IS NOT NULL AND cpu BETWEEN 0 AND 100",
    "memory_valid": "memory IS NOT NULL AND memory BETWEEN 0 AND 100",
    "disk_valid": "disk IS NOT NULL AND disk BETWEEN 0 AND 100"
})
@dp.table()
def server_metrics_silver():
    df_stream = spark.readStream.table("server_metrics_bronze")
    df_casted = df_stream \
        .withColumn("timestamp", col("timestamp").cast("timestamp")) \
        .withColumn("cpu", col("cpu").cast("double")) \
        .withColumn("memory", col("memory").cast("double")) \
        .withColumn("disk", col("disk").cast("double"))
    df_watermarked = df_casted.withWatermark("timestamp", "5 minutes")
    df_deduped = df_watermarked.dropDuplicates(["event_id"])
    return df_deduped
