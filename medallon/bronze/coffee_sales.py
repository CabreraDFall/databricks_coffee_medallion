from pyspark import pipelines as dp
from pyspark import pandas as pd
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.functions import md5, concat_ws, sha2

#configuracion
SOURCE_PATH = "s3://coffee-sales-ab1/data-store/sales/"

@dp.materialized_view(
    name="coffee_sales.bronze.coffee",
    comment="Coffee Raw Data Processing",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.columnMapping.mode": "name",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def coffee_bronze():
    # 1. Lectura del archivo CSV con opciones de seguridad
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("mode", "PERMISSIVE") \
        .option("mergeSchema", "true") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .load(SOURCE_PATH)
    # 2. Añadir metadatos para trazabilidad
    df = df.withColumn("file_name", col("_metadata.file_path")) \
           .withColumn("ingest_datetime", current_timestamp())

    return df