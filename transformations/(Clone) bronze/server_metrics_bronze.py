from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

@dp.table()
def server_metrics_bronze():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", "/Volumes/server_catalog/server_schema/my_volume/schema/metrics/")
        .option("badRecordsPath", "/Volumes/server_catalog/server_schema/my_volume/quarantine/metrics/")
        .load("/Volumes/server_catalog/server_schema/my_volume/raw_data/metrics/")
    )
    return df.withColumn("source_file_name", col("_metadata.file_path")) \
            .withColumn("ingestion_time", current_timestamp())
