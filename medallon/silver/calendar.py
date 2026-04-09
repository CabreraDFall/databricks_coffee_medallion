from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Parámetros de configuración (asegúrate de que estén definidos en tu pipeline de DLT)
start_date = spark.conf.get("start_date")
end_date = spark.conf.get("end_date")

@dp.materialized_view(
    name="coffee_sales.silver.calendar",
    comment="Dimensión de calendario con atributos detallados y festivos nacionales de España",
    table_properties={
        "quality": "silver", 
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def calendar():
    # 1. Generación de base secuencial
    df = spark.sql(
        f"""
        SELECT explode(sequence(
            to_date('{start_date}'),
            to_date('{end_date}'),
            interval 1 day
        )) as date
        """
    )

    # 2. Atributos básicos y claves
    df = df.withColumn("date_key", F.date_format(F.col("date"), "yyyyMMdd").cast("int")) \
           .withColumn("year", F.year(F.col("date"))) \
           .withColumn("month", F.month(F.col("date"))) \
           .withColumn("quarter", F.quarter(F.col("date"))) \
           .withColumn("day_of_month", F.dayofmonth(F.col("date"))) \
           .withColumn("day_of_week_num", F.dayofweek(F.col("date")))

    # 3. Nombres y formatos (puedes cambiar "es" por "en" si prefieres nombres en inglés)
    df = df.withColumn("day_of_week", F.date_format(F.col("date"), "EEEE")) \
           .withColumn("day_of_week_abbr", F.date_format(F.col("date"), "EEE")) \
           .withColumn("month_name", F.date_format(F.col("date"), "MMMM")) \
           .withColumn("month_year", F.concat(F.date_format(F.col("date"), "MMMM"), F.lit(" "), F.col("year"))) \
           .withColumn("quarter_year", F.concat(F.lit("Q"), F.col("quarter"), F.lit(" "), F.col("year"))) \
           .withColumn("week_of_year", F.weekofyear(F.col("date"))) \
           .withColumn("day_of_year", F.dayofyear(F.col("date")))

    # 4. Indicadores de fin de semana (en PySpark 1=Domingo, 7=Sábado)
    df = df.withColumn("is_weekend", F.col("day_of_week_num").isin([1, 7])) \
           .withColumn("is_weekday", ~F.col("is_weekend"))

    # 5. Festivos Nacionales de España (Fechas fijas)
    df = df.withColumn(
        "holiday_name",
        F.when((F.col("month") == 1) & (F.col("day_of_month") == 1), F.lit("Año Nuevo"))
        .when((F.col("month") == 1) & (F.col("day_of_month") == 6), F.lit("Epifanía del Señor / Reyes"))
        .when((F.col("month") == 5) & (F.col("day_of_month") == 1), F.lit("Fiesta del Trabajo"))
        .when((F.col("month") == 8) & (F.col("day_of_month") == 15), F.lit("Asunción de la Virgen"))
        .when((F.col("month") == 10) & (F.col("day_of_month") == 12), F.lit("Fiesta Nacional de España"))
        .when((F.col("month") == 11) & (F.col("day_of_month") == 1), F.lit("Todos los Santos"))
        .when((F.col("month") == 12) & (F.col("day_of_month") == 6), F.lit("Día de la Constitución Española"))
        .when((F.col("month") == 12) & (F.col("day_of_month") == 8), F.lit("Inmaculada Concepción"))
        .when((F.col("month") == 12) & (F.col("day_of_month") == 25), F.lit("Navidad"))
        .otherwise(None)
    ).withColumn("is_holiday", F.col("holiday_name").isNotNull())

    # 6. Auditoría y selección final
    return df.withColumn("silver_processed_timestamp", F.current_timestamp()) \
             .select(
                "date", "date_key", "year", "month", "day_of_month", 
                "day_of_week", "day_of_week_abbr", "month_name", "month_year", 
                "quarter", "quarter_year", "week_of_year", "day_of_year", 
                "is_weekday", "is_weekend", "is_holiday", "holiday_name", 
                "silver_processed_timestamp"
            )
