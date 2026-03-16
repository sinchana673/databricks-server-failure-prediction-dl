from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

@dp.table()
def server_events_bronze():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", "/Volumes/server_catalog/server_schema/my_volume/schema/events/")
        .option("badRecordsPath", "/Volumes/server_catalog/server_schema/my_volume/quarantine/events/")
        .load("/Volumes/server_catalog/server_schema/my_volume/raw_data/events/")
    )
    return df.withColumn("source_file_name", col("_metadata.file_path")) \
            .withColumn("ingestion_time", current_timestamp())
