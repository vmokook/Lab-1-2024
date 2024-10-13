import os
import csv
import pandas as pd
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from elasticsearch import Elasticsearch


INPUT_DIR = "data/initial"
TEMP_DIR = "data/temp/"
OUTPUT_FILE = "data/final_output.csv"

# Проверка наличия директорий
os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)

# Чтение файлов
def read_files(**kwargs):
    all_files = [
        os.path.join(INPUT_DIR, f) for f in os.listdir(INPUT_DIR) if f.endswith(".csv")
    ]
    kwargs["ti"].xcom_push(key="all_files", value=all_files)

# Разделение файлов на строки
def split_files_into_rows(**kwargs):
    all_files = kwargs["ti"].xcom_pull(key="all_files", task_ids="read_files")
    dataframes = [pd.read_csv(file, engine="python") for file in all_files]
    kwargs["ti"].xcom_push(key="dataframes", value=dataframes)

# Фильтрация строк
def filter_rows(**kwargs):
    dataframes = kwargs["ti"].xcom_pull(key="dataframes", task_ids="split_files_into_rows")
    filtered_dfs = [df[(df["designation"].notnull()) & (df["region_1"].notnull())] for df in dataframes]
    kwargs["ti"].xcom_push(key="filtered_dfs", value=filtered_dfs)

# Замена null в price на 0.0
def replace_null_price(**kwargs):
    filtered_dfs = kwargs["ti"].xcom_pull(key="filtered_dfs", task_ids="filter_rows")
    updated_dfs = [df.fillna({"price": 0.0}) for df in filtered_dfs]
    kwargs["ti"].xcom_push(key="updated_dfs", value=updated_dfs)

# Объединение всех DataFrame в один
def merge_dataframes(**kwargs):
    updated_dfs = kwargs["ti"].xcom_pull(key="updated_dfs", task_ids="replace_null_price")
    merged_df = pd.concat(updated_dfs, ignore_index=True)
    temp_file = os.path.join(TEMP_DIR, "merged_df.pkl")
    merged_df.to_pickle(temp_file)
    kwargs["ti"].xcom_push(key="merged_df_path", value=temp_file)

# Сохранение в CSV файл
def save_to_csv(**kwargs):
    merged_df_path = kwargs["ti"].xcom_pull(key="merged_df_path", task_ids="merge_dataframes")
    merged_df = pd.read_pickle(merged_df_path)
    merged_df.to_csv(OUTPUT_FILE, index=False)
    os.remove(merged_df_path)

# Сохранение в Elasticsearch
def save_to_elasticsearch(**kwargs):
    merged_df_path = kwargs["ti"].xcom_pull(key="merged_df_path", task_ids="merge_dataframes")
    df = pd.read_pickle(merged_df_path) 
    es = Elasticsearch([{'host': 'elasticsearch-kibana', 'port': 9200}])
    for _, row in df.iterrows():
        es.index(index='lab1', body=row.to_json())



default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}

with DAG(
    dag_id="csv_processing_dag_sequential_steps",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    read_files_task = PythonOperator(
        task_id='read_files',
        python_callable=read_files,
        dag=dag,
    )

    # Оператор 2: разделение файлов на строки
    split_files_task = PythonOperator(
        task_id='split_files_into_rows',
        python_callable=split_files_into_rows,
        provide_context=True,
        dag=dag,
    )

    # Оператор 3: фильтрация строк
    filter_rows_task = PythonOperator(
        task_id='filter_rows',
        python_callable=filter_rows,
        provide_context=True,
        dag=dag,
    )

    # Оператор 4: замена null в price на 0.0
    replace_null_price_task = PythonOperator(
        task_id='replace_null_price',
        python_callable=replace_null_price,
        provide_context=True,
        dag=dag,
    )

    # Оператор 5: объединение всех DataFrame
    merge_dataframes_task = PythonOperator(
        task_id='merge_dataframes',
        python_callable=merge_dataframes,
        provide_context=True,
        dag=dag,
    )

    # Оператор 6: сохранение в итоговый CSV файл
    save_to_csv_task = PythonOperator(
        task_id='save_to_csv',
        python_callable=save_to_csv,
        provide_context=True,
        dag=dag,
    )

    # Оператор 7: сохранение данных в Elasticsearch
    save_to_elasticsearch_task = PythonOperator(
        task_id='save_to_elasticsearch',
        python_callable=save_to_elasticsearch,
        provide_context=True,
        dag=dag,
    )

    # Определение порядка выполнения задач
    (
        read_files_task
        >> split_files_task
        >> filter_rows_task
        >> replace_null_price_task
        >> merge_dataframes_task
        >> [save_to_csv_task, save_to_elasticsearch_task]
    )