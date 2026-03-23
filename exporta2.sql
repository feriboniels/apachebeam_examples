from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryToGCSOperator, BigQueryDeleteTableOperator
from datetime import datetime

PROJECT_ID = "tu-proyecto"
DATASET_ID = "tu_dataset"
BUCKET_NAME = "tu-bucket"
# Suponiendo que ya calculaste que 5 archivos cubren tus 85MB cada uno
TOTAL_ARCHIVOS = 5 
FILAS_POR_ARCHIVO = 100000 

with DAG(
    dag_id='export_limpio_sin_ceros',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # 1. Preparamos una tabla con índices (una sola vez)
    preparar_tabla = BigQueryInsertJobOperator(
        task_id='preparar_indices',
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.temp_export_indexada` AS
                    SELECT *, ROW_NUMBER() OVER() as fila_id FROM `{PROJECT_ID}.{DATASET_ID}.tu_tabla_original`
                """,
                "useLegacySql": False,
            }
        }
    )

    # 2. Bucle para crear sub-tablas y exportarlas con nombre exacto
    for i in range(1, TOTAL_ARCHIVOS + 1):
        inicio = ((i - 1) * FILAS_POR_ARCHIVO) + 1
        fin = i * FILAS_POR_ARCHIVO
        
        nombre_tabla_temp = f"temp_parte_{i}"
        nombre_archivo = f"archivo_{i}_de_{TOTAL_ARCHIVOS}.csv"

        # Crear sub-tabla pequeña
        crear_sub_tabla = BigQueryInsertJobOperator(
            task_id=f'crear_tabla_{i}',
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.{nombre_tabla_temp}` AS
                        SELECT * EXCEPT(fila_id) FROM `{PROJECT_ID}.{DATASET_ID}.temp_export_indexada`
                        WHERE fila_id BETWEEN {inicio} AND {fin}
                    """,
                    "useLegacySql": False,
                }
            }
        )

        # Exportar SIN comodín (esto genera el nombre EXACTO)
        exportar_gcs = BigQueryToGCSOperator(
            task_id=f'exportar_gcs_{i}',
            source_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.{nombre_tabla_temp}",
            destination_cloud_storage_uris=[f"gs://{BUCKET_NAME}/{nombre_archivo}"],
            export_format='CSV',
        )

        # Limpieza de la sub-tabla
        borrar_temp = BigQueryDeleteTableOperator(
            task_id=f'borrar_tabla_{i}',
            deletion_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.{nombre_tabla_temp}",
            ignore_if_missing=True
        )

        preparar_tabla >> crear_sub_tabla >> exportar_gcs >> borrar_temp
