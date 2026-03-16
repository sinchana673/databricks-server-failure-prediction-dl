from pyspark import pipelines as dp
from pyspark.sql.functions import col

@dp.expect_or_drop("valid_event_type", "event_type IN ('server_crash', 'service_restart', 'deployment', 'config_update', 'scaling_event')")
@dp.expect_all_or_drop({
    "timestamp_not_null": "timestamp IS NOT NULL",
    "server_id_not_null": "server_id IS NOT NULL"
})
@dp.table()
def server_events_silver():
    df_stream = spark.readStream.table("server_events_bronze")
    df_casted = df_stream.withColumn("timestamp", col("timestamp").cast("timestamp"))
    df_deduped = df_casted.dropDuplicates(["event_id"])
    return df_deduped
@dp.table(name="server_events_quarantine")
def server_events_quarantine():

    df_stream = spark.readStream.table("server_events_bronze")

    df_casted = df_stream.withColumn(
        "timestamp",
        col("timestamp").cast("timestamp")
    )

    df_invalid = df_casted.filter(
        (~col("event_type").isin(
            "server_crash",
            "service_restart",
            "deployment",
            "config_update",
            "scaling_event"
        )) |
        col("timestamp").isNull() |
        col("server_id").isNull()
    )

    return df_invalid