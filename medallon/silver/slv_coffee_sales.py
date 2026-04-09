from pyspark.sql import functions as F
from pyspark import pipelines as dp

# --- 1. TABLA DE APRENDIZAJE DE PRECIOS ---
@dp.materialized_view(
  name = "item_prices_reference",
  comment = "Calcula precios maestros basados en la mediana histórica de los datos correctos"
)
def item_prices_reference():
    return (
        spark.read.table("coffee_sales.bronze.coffee")
        .select(
            F.trim(F.col("Item")).alias("item"),
            F.expr("TRY_CAST(`Price Per Unit` AS FLOAT)").alias("price")
        )
        .filter("item NOT IN ('ERROR', 'UNKNOWN') AND item IS NOT NULL AND price IS NOT NULL")
        .groupBy("item")
        .agg(F.percentile_approx("price", 0.5).alias("master_price"))
    )

# --- 2. TABLA SILVER (LA VERSIÓN DEFINITIVA) ---
@dp.materialized_view(
  name = "coffee_sales.silver.coffee",
  comment = "Data estandarizada y reparada dinámicamente: Hechos de ventas limpios",
  table_properties={
    "quality": "silver",
    "layer": "silver",
    "delta.enableChangeDataFeed": "true"
  }
)
# Regla de Oro: Solo subir lo que tenga info financiera real al final del proceso
@dp.expect_or_drop("financial_integrity", "total_spent IS NOT NULL AND quantity IS NOT NULL")

def coffee_silver():
    # A. Carga de Datos
    df_bronze = spark.read.table("coffee_sales.bronze.coffee")
    df_master_prices = spark.read.table("item_prices_reference")

    # B. LIMPIEZA INICIAL, CASTEO Y CATEGORIZACIÓN
    df_step1 = df_bronze.select(
        F.col("Transaction ID").alias("transaction_id"),
        
        # Estandarización de categorías (Item, Payment, Location)
        F.when(F.trim(F.col("Item")).isin("ERROR", "UNKNOWN") | F.col("Item").isNull(), "Other")
         .otherwise(F.trim(F.col("Item"))).alias("item"),
        
        # TRY_CAST para evitar que el pipeline falle por texto basura
        F.expr("TRY_CAST(Quantity AS INT)").alias("qty"),
        F.expr("TRY_CAST(`Price Per Unit` AS FLOAT)").alias("p_unit"),
        F.expr("TRY_CAST(`Total Spent` AS FLOAT)").alias("total"),

        F.when(F.col("`Payment Method`").isin("ERROR", "UNKNOWN") | F.col("`Payment Method`").isNull(), "Unknown")
         .otherwise(F.col("`Payment Method`")).alias("payment_method"),
        
        F.when(F.col("Location").isin("ERROR", "UNKNOWN") | F.col("Location").isNull(), "Unknown")
         .otherwise(F.col("Location")).alias("location"),

        # Fallback de fecha automático
        F.coalesce(F.to_date(F.col("Transaction Date")), F.to_date(F.col("ingest_datetime"))).alias("transaction_date"),
        F.col("ingest_datetime").alias("bronze_ingest_datetime")
    )

    # C. UNIÓN CON PRECIOS DINÁMICOS
    df_joined = df_step1.join(df_master_prices, on="item", how="left")

    # D. LÓGICA DE REPARACIÓN MULTICAPA (Jerarquía de supervivencia de datos)
    df_repaired = (
        df_joined
        # 1. Recuperar el Precio (Si no lo tengo, usar el Maestro)
        .withColumn("price_per_unit", 
            F.when(F.col("p_unit").isNull(), F.col("master_price")).otherwise(F.col("p_unit")))
        
        # 2. Reparar el Total (Cantidad * Precio Limpio)
        .withColumn("total_spent",
            F.when(F.col("total").isNull() & F.col("qty").isNotNull() & F.col("price_per_unit").isNotNull(), 
                   F.round(F.col("qty") * F.col("price_per_unit"), 2))
             .otherwise(F.col("total")))
             
        # 3. Reparar la Cantidad (Total Limpio / Precio Limpio)
        .withColumn("quantity",
            F.when(F.col("qty").isNull() & F.col("total_spent").isNotNull() & (F.col("price_per_unit") > 0), 
                   (F.col("total_spent") / F.col("price_per_unit")).cast("int"))
             .otherwise(F.col("qty")))
             
        # 4. Caso especial 'Other': Si calculamos el precio de los Items 'Other' (para el futuro)
        .withColumn("price_per_unit",
            F.when(F.col("price_per_unit").isNull() & F.col("quantity").isNotNull() & F.col("total_spent").isNotNull(),
                   F.round(F.col("total_spent") / F.col("quantity"), 2))
             .otherwise(F.col("price_per_unit")))
    )

    # E. SELECCIÓN FINAL Y AUDITORÍA
    # El decorador '@dp.expect_or_drop' filtrará las filas que ni con todo esto se pudieron salvar
    return df_repaired.select(
        "transaction_id",
        "item",
        "quantity",
        "price_per_unit",
        "total_spent",
        "payment_method",
        "location",
        "transaction_date",
        "bronze_ingest_datetime",
        F.current_timestamp().alias("silver_processed_timestamp")
    )
