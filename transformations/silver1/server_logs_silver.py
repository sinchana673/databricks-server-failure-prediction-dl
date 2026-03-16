from pyspark import pipelines as dp
from pyspark.sql.functions import col, split

@dp.expect_all_or_drop({
    "timestamp_not_null": "timestamp IS NOT NULL",
    "server_id_not_null": "server_id IS NOT NULL",
    "log_level_not_null": "log_level IS NOT NULL",
    "message_not_null": "message IS NOT NULL"
})
@dp.table()
def server_logs_silver():
    # Read streaming logs
    df_stream = spark.readStream.table("server_logs_bronze")
    # log format: <timestamp> <server_id> <log_level> <message>
    split_col = split(col("value"), " ", 4)
    df_parsed = df_stream \
        .withColumn("timestamp", split_col.getItem(0)) \
        .withColumn("server_id", split_col.getItem(1)) \
        .withColumn("log_level", split_col.getItem(2)) \
        .withColumn("message", split_col.getItem(3))
    df_casted = df_parsed.withColumn("timestamp", col("timestamp").cast("timestamp"))
    # Drop duplicates
    df_deduped = df_casted.dropDuplicates(["timestamp", "server_id", "log_level", "message"])
    # Drop 'value' column
    df_final = df_deduped.drop("value")
    return df_final

@dp.table(name="server_logs_quarantine")
def server_logs_quarantine():

    df_stream = spark.readStream.table("server_logs_bronze")

    # log format: <timestamp> <server_id> <log_level> <message>
    split_col = split(col("value"), " ", 4)

    df_parsed = df_stream \
        .withColumn("timestamp", split_col.getItem(0)) \
        .withColumn("server_id", split_col.getItem(1)) \
        .withColumn("log_level", split_col.getItem(2)) \
        .withColumn("message", split_col.getItem(3))

    df_casted = df_parsed.withColumn("timestamp", col("timestamp").cast("timestamp"))

    df_invalid = df_casted.filter(
        col("timestamp").isNull() |
        col("server_id").isNull() |
        col("log_level").isNull() |
        col("message").isNull()
    )

    return df_invalid.drop("value")