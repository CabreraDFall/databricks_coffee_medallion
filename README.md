# Proyecto: Databricks Coffee Medallion Pipeline

Este proyecto implementa una arquitectura Medallion (Bronze, Silver, Gold) utilizando Delta Live Tables (DLT) en Databricks para procesar y limpiar datos de ventas de una cafetería. Incluye lógica avanzada de auto-reparación de datos y un modelo dimensional listo para BI.

## Arquitectura del Pipeline

El pipeline está dividido en tres capas lógicas para asegurar la trazabilidad y calidad del dato:

1. Capa Bronze (Ingesta Raw):
   - Carga datos crudos desde S3/DBFS en formato CSV.
   - Utiliza modo PERMISSIVE para manejar registros corruptos sin detener el pipeline.
   - Añade metadatos de trazabilidad (file_name, ingest_datetime).

2. Capa Silver (Limpieza y Enriquecimiento):
   - Referencia Dinámica de Precios: Calcula automáticamente los precios maestros basados en la mediana histórica para corregir valores nulos o erróneos.
   - Lógica de Reparación Multicapa: Utiliza relaciones matemáticas para reconstruir datos faltantes (ej: total = cantidad * precio o cantidad = total / precio).
   - Calidad del Dato (Expectations): Implementa reglas de integridad financiera para descartar registros que no se pueden salvar.
   - Calendario Maestro: Genera una dimensión de tiempo enriquecida con festivos nacionales (España) y atributos de negocio.

3. Capa Gold (Modelo de Negocio):
   - Modelo en Estrella: Implementa dimensiones (dim_item, dim_location) y una tabla de hechos (fact_sales) con claves subrogadas (SHA2).
   - Reportes Agregados: Incluye una vista de resumen de ventas lista para consumo por herramientas de BI (PowerBI/Tableau).

## Cómo ejecutarlo

1. Configura un Pipeline de Delta Live Tables en tu workspace de Databricks.
2. Añade las rutas de los archivos .py en la configuración del pipeline:
   - medallon/bronze/coffee_sales.py
   - medallon/silver/calendar.py
   - medallon/silver/slv_coffee_sales.py
   - medallon/gold/gld_coffee_sales.py
3. Configura los parámetros necesarios:
   - start_date: Fecha de inicio para el calendario (ej: 2024-01-01).
   - end_date: Fecha de fin para el calendario (ej: 2025-12-31).

## Tecnologías utilizadas

- Databricks & Delta Live Tables (DLT)
- PySpark (Spark SQL & Functions)
- Delta Lake (con Change Data Feed habilitado)
