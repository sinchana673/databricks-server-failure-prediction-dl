from pyspark import pipelines as dp
from pyspark.sql.functions import col, split

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
    # Remove corrupted: timestamp/server_id not null
    df_cleaned = df_casted.filter(col("timestamp").isNotNull() & col("server_id").isNotNull())
    # Drop duplicates
    df_deduped = df_cleaned.dropDuplicates(["timestamp", "server_id", "log_level", "message"])
    # Drop 'value' column
    df_final = df_deduped.drop("value")
    return df_final
