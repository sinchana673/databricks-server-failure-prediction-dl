from pyspark import pipelines as dp
from pyspark.sql.functions import col

@dp.expect_or_drop("valid_event_type", "event_type IN ('server_crash', 'service_restart', 'deployment', 'config_update', 'scaling_event')")
@dp.table()
def server_events_silver():
    df_stream = spark.readStream.table("server_events_bronze")
    df_casted = df_stream.withColumn("timestamp", col("timestamp").cast("timestamp"))
    df_cleaned = df_casted.filter(col("timestamp").isNotNull() & col("server_id").isNotNull())
    df_deduped = df_cleaned.dropDuplicates(["event_id"])
    return df_deduped
