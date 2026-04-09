import os

path = r'c:\Users\Sr. Fall\Desktop\Data Git\databricks_s3_\image'
renames = {
    "Captura de pantalla 2026-04-08 100923.png": "aws_s3_bucket_creation.png",
    "Captura de pantalla 2026-04-08 101057.png": "aws_s3_folder_creation.png",
    "Captura de pantalla 2026-04-08 103027.png": "databricks_project_setup_notebook.png",
    "Captura de pantalla 2026-04-08 103206.png": "databricks_external_location_setup.png",
    "Captura de pantalla 2026-04-08 103510.png": "databricks_catalog_explorer_files.png",
    "Captura de pantalla 2026-04-08 155232.png": "databricks_dlt_pipeline_graph_bronze.png",
    "Captura de pantalla 2026-04-08 170155.png": "databricks_dlt_calendar_silver.png",
    "Captura de pantalla 2026-04-08 180634.png": "databricks_dlt_pipeline_lineage.png"
}

for old_name, new_name in renames.items():
    old_file = os.path.join(path, old_name)
    new_file = os.path.join(path, new_name)
    if os.path.exists(old_file):
        print(f"Renaming {old_name} to {new_name}")
        os.rename(old_file, new_file)
    else:
        print(f"Skipping {old_name}: file not found or already renamed.")
