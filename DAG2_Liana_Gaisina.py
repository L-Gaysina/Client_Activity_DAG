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

# Основная директория для хранения данных
base_dir = '/tmp/airflow/data_2/'

# Настройки по умолчанию для DAG
DAG_ID = 'DAG2_Liana_Gaisina'
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2024, 4, 5, tz=pendulum.timezone("Europe/Moscow")),
    'schedule_interval': '0 0 5 * *',
    'retries': 3,
    "retry_delay": timedelta(seconds=60),
    'description': 'ETL DAG для ежемесячного расчета активности клиентов на основе транзакций.',
    'max_active_runs': 1,
    'catchup': False,
}

def download_data(execution_date, **kwargs):
    """
    Скачивание данных по указанному URL и сохранение их в формате CSV. 

    Args:
        execution_date (str): Дата выполнения задачи, используется для именования файла данных.
        kwargs (dict): Словарь с дополнительными параметрами (используется для взаимодействия с Airflow).

    Основные шаги:
        1. Формирование пути к файлу для сохранения данных.
        2. Выполнение HTTP GET запроса к указанному URL.
        3. Проверка успешности HTTP запроса.
        4. Запись полученных данных в файл.
        5. Логирование успешного завершения операции.
    """
    # Конфигурация URL и пути сохранения файла 
    data_url = (
        'https://drive.usercontent.google.com/download?id=1hkkOIxnYQTa7WD1oSIDUFgEoBoWfjxK2&'
        'export=download&authuser=0&confirm=t&uuid=af8f933c-070d-4ea5-857b-2c31f2bad050&at='
        'APZUnTVuHs3BtcrjY_dbuHsDceYr:1716219233729'
    )
    
    data_dir = base_dir
    os.makedirs(data_dir, exist_ok=True)
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

def process_single_product_data(product_code, execution_date, **kwargs):
    """
    Функция для обработки данных для одного продукта.
 
    Args:
        product_code: Код продукта, например 'a', 'b', ...
        execution_date (str): Дата выполнения задачи, используется для именования и доступа к файлам данных.
        kwargs (dict): Словарь с дополнительными параметрами (используется для взаимодействия с Airflow).
    """
    data_file = f'{base_dir}profit_table_{execution_date}.csv'
    data_frame = pd.read_csv(data_file)
    transformed_data = transform(data_frame, execution_date, product_code)
    
    # Сохраняем преобразованные данные в XCom для последующего использования
    task_instance = kwargs['ti']
    task_instance.xcom_push(key=f'processed_data_{product_code}', value=transformed_data.to_json())

def save_transformed_data(execution_date, **kwargs):
    """
    Функция для сохранения преобразованных данных в файл.
    
    Args:
        execution_date (str): Дата выполнения задачи, используется для именования и доступа к файлам данных.
        kwargs (dict): Словарь с дополнительными параметрами (используется для взаимодействия с Airflow).

    """
    # Преобразование данных 
    task_instance = kwargs['ti']
    product_codes = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']
    for product_code in product_codes:
        data_json = task_instance.xcom_pull(key=f'processed_data_{product_code}', task_ids=f'transform_{product_code}')
        if data_json is None:
            logger.error(f"No data received for product {product_code}.")
            continue

       
        data_frame = pd.read_json(data_json)
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

    Функция создаёт DAG с заданной конфигурацией параметров и определяет последовательность выполнения задач:
    извлечение данных (extract), трансформация данных (transform) и загрузка данных (load).

    Возвращает:
        dag (DAG): Объект DAG, сконфигурированный с заданными параметрами и задачами.
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
        # Задачи для трансформации данных
        product_codes = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']
        transform_tasks = []
        for code in product_codes:
            task = PythonOperator(
                task_id=f'transform_{code}',
                python_callable=process_single_product_data,
                op_kwargs={'product_code': code, 'execution_date': '{{ ds }}'},
                provide_context=True
            )
            transform_tasks.append(task)
            extract >> task

        # Задача для сохранения данных
        load = PythonOperator(
            task_id='load',
            python_callable=save_transformed_data,
            op_kwargs={'execution_date': '{{ ds }}'},
            provide_context=True
        )

        # Задаем порядок выполнения задач
        for task in transform_tasks:
            task >> load

    return dag

# Создаем DAG
dag = create_dag()
