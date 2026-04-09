# --- Archivo: gld_coffee_sales.py (CORREGIDO) ---
from pyspark.sql import functions as F
from pyspark import pipelines as dp

# 1. DIMENSIÓN: PRODUCTO
@dp.materialized_view(
    name="gold.dim_item", # <-- AÑADIDO "gold."
    comment="Maestro de productos"
)
def gold_dim_item():
    return (
        spark.read.table("coffee_sales.silver.coffee")
        .select("item", "price_per_unit").distinct()
        .withColumn("item_key", F.sha2(F.col("item"), 256)) 
    )

# 2. DIMENSIÓN: UBICACIÓN
@dp.materialized_view(
    name="gold.dim_location", # <-- AÑADIDO "gold."
    comment="Maestro de tiendas"
)
def gold_dim_location():
    return (
        spark.read.table("coffee_sales.silver.coffee")
        .select("location").distinct()
        .withColumn("location_key", F.sha2(F.col("location"), 256))
    )

# 3. TABLA DE HECHOS (FACTS)
@dp.materialized_view(
    name="gold.fact_sales", # <-- AÑADIDO "gold."
    comment="Hechos de ventas"
)
def gold_fact_sales():
    df_silver = spark.read.table("coffee_sales.silver.coffee")
    return (
        df_silver.select(
            "transaction_id",
            F.sha2(F.col("item"), 256).alias("item_key"),
            F.sha2(F.col("location"), 256).alias("location_key"),
            F.date_format(F.col("transaction_date"), "yyyyMMdd").cast("int").alias("date_key"),
            "quantity",
            "total_spent"
        )
    )

# 4. REPORTE FINAL
@dp.materialized_view(
    name="gold.rpt_sales_summary", # <-- AÑADIDO "gold."
    comment="Reporte de negocio"
)
def gold_rpt_sales_summary():
    df_fact = spark.read.table("gold.fact_sales")
    df_cal = spark.read.table("coffee_sales.silver.calendar")
    
    return (
        df_fact.join(df_cal, on="date_key")
        .groupBy("year", "month_name", "day_of_week", "is_holiday", "holiday_name")
        .agg(
            F.sum("quantity").alias("total_items_sold"),
            F.sum("total_spent").alias("total_revenue"),
            F.count("transaction_id").alias("total_transactions")
        )
    )
