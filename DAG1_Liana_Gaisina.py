import os
import sys
from datetime import timedelta

import pandas as pd
import pendulum
import requests
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator

# Настройка логирования
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Добавляем путь к директории, где находится transform_script.py
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from transform_script import transform  # импортируем функцию transform из скрипта

# Получаем текущую директорию скрипта
script_directory = os.path.abspath(os.path.dirname(__file__))
base_dir = os.path.join(script_directory, 'data')
os.makedirs(base_dir, exist_ok=True)

# Настройки для DAG
DAG_ID = 'DAG1_Liana_Gaisina'
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2024, 4, 5, tz=pendulum.timezone("Europe/Moscow")),
    'retries': 3,
    'retry_delay': timedelta(seconds=60),
    'description': 'ETL DAG для ежемесячного расчета активности клиентов на основе транзакций.',
    'max_active_runs': 1,
    'catchup': False,
}

def download_data(execution_date, **kwargs):
    """
    Скачивание данных по указанному URL и сохранение их в формате CSV. 
    """
    # Конфигурация URL и пути сохранения файла 
    data_url = (
        'https://drive.usercontent.google.com/download?id=1hkkOIxnYQTa7WD1oSIDUFgEoBoWfjxK2&'
        'export=download&authuser=0&confirm=t&uuid=af8f933c-070d-4ea5-857b-2c31f2bad050&at='
        'APZUnTVuHs3BtcrjY_dbuHsDceYr:1716219233729'
    )
    data_dir = base_dir
    output_file = os.path.join(data_dir, f'profit_table_{execution_date}.csv')

    # Запрос данных и их сохранение
    response = requests.get(data_url)
    response.raise_for_status()

    with open(output_file, 'wb') as file:
        file.write(response.content)

    # Логирование успешного скачивания данных
    task_instance = kwargs['ti']
    task_instance.xcom_push(key='download_success', value=True)
    logger.info(f"File {output_file} has been successfully created.")

def process_product_data(execution_date, **kwargs):
    """
    Обработка данных о продуктах, загруженных ранее, и агрегация результатов.
    """
    data_file = os.path.join(base_dir, f'profit_table_{execution_date}.csv')
    data_frame = pd.read_csv(data_file)
    product_codes = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']
    all_transformed_data = []
    
    # Обработка данных для каждого продукта
    for product_code in product_codes:
        transformed_data = transform(data_frame, execution_date, product_code)
        all_transformed_data.append(transformed_data)

    # Агрегация результатов
    final_data_frame = pd.concat(all_transformed_data, axis=1).T.drop_duplicates().T

    task_instance = kwargs['ti']
    task_instance.xcom_push(key='processed_data', value=final_data_frame.to_json())  # Use JSON to handle DataFrame

def save_transformed_data(execution_date, **kwargs):
    """
    Сохранение обработанных данных по каждому продукту в отдельные CSV файлы.
    """
    # Получение экземпляра задачи для доступа к XCom
    task_instance = kwargs['ti']
    data_json = task_instance.xcom_pull(key='processed_data', task_ids='transform')
    if data_json is None:
        logger.error("No data received from transform task.")
        raise ValueError("No data available from transform task.")
    
    # Преобразование JSON данных в DataFrame
    data_frame = pd.read_json(data_json)

    # Список кодов продуктов для сохранения отдельных файлов
    product_codes = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']

    # Проход по каждому коду продукта
    for product_code in product_codes:
        output_file = os.path.join(base_dir, f'flags_activity_{product_code}_{execution_date}.csv')
        
        # Проверка существования файла для возможного обновления данных
        if os.path.exists(output_file):
            existing_data_frame = pd.read_csv(output_file)
            updated_data_frame = pd.concat([existing_data_frame, data_frame], ignore_index=True)
            updated_data_frame.to_csv(output_file, index=False)
        # Сохранение нового файла, если он не существовал    
        else:
            data_frame.to_csv(output_file, index=False)

def create_dag():
    """
    Создание и конфигурация DAG (Directed Acyclic Graph) в Apache Airflow.
    """
    # Инициализация объекта DAG с параметрами по умолчанию
    with DAG(
        DAG_ID,
        default_args=default_args,
        description=default_args.get("description"),
        start_date=default_args.get("start_date"),
        schedule_interval=default_args.get("schedule_interval"),
        catchup=default_args.get("catchup"),
        max_active_runs=default_args.get("max_active_runs")
    ) as dag:

        # Задача для загрузки данных
        extract = PythonOperator(
            task_id='extract',
            python_callable=download_data,
            op_kwargs={'execution_date': '{{ ds }}'},
            provide_context=True
        )
        # Задача для трансформации данных
        transform = PythonOperator(
            task_id='transform',
            python_callable=process_product_data,
            op_kwargs={'execution_date': '{{ ds }}'},
            provide_context=True
        )
        # Задача для сохранения данных
        load = PythonOperator(
            task_id='load',
            python_callable=save_transformed_data,
            op_kwargs={'execution_date': '{{ ds }}'},
            provide_context=True
        )

        # Задаем порядок выполнения задач
        extract >> transform >> load

    return dag

# Создаем DAG
dag = create_dag()
