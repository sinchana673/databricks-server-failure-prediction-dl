from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

@dp.table()
def server_logs_bronze():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.schemaLocation", "/Volumes/server_catalog/server_schema/my_volume/schema/logs/")
        .option("badRecordsPath", "/Volumes/server_catalog/server_schema/my_volume/quarantine/logs/")
        .load("/Volumes/server_catalog/server_schema/my_volume/raw_data/logs/")
    )
    return df.withColumn("source_file_name", col("_metadata.file_path")) \
            .withColumn("ingestion_time", current_timestamp())
