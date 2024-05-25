# DAG-for-Client-Transaction-Analysis
Этот проект предназначен для создания ориентированного ациклического графа (DAG) в Apache Airflow, который будет периодически рассчитывать витрину активности клиентов на основе суммы и количества их транзакций.

## Описание задач DAG
### DAG1
Этот DAG состоит из следующих задач:

* `extract`: Задача скачивает данные по указанному URL и сохраняет их в формате CSV.
* `transform`: Задача обрабатывает данные, применяя функцию transform, и собирает результаты по каждому продукту.
* `load`: Задача сохраняет обработанные данные в отдельные CSV файлы для каждого продукта.

### DAG2
Этот DAG состоит из следующих задач:

* `extract`: Задача скачивает данные по указанному URL и сохраняет их в формате CSV.
* `transform_<product_code>`: 10 задач паралелльно обрабатывают данные для каждого продукта (например, transform_a, transform_b и т.д.) с использованием функции transform.
* `load`: Задача сохраняет обработанные данные в отдельные CSV файлы для каждого продукта.

## Обзор файлов
1. [DAG1_Liana_Gaisina.py](https://github.com/L-Gaysina/DAG-for-Client-Transaction-Analysis/blob/main/DAG1_Liana_Gaisina.py)
Этот файл содержит определение первого DAG в Airflow, который организует процесс ETL.

* [Скриншот успешного запуска](https://github.com/L-Gaysina/DAG-for-Client-Transaction-Analysis/blob/main/Graph%20DAG1.png)

2. [DAG2_Liana_Gaisina.py](https://github.com/L-Gaysina/DAG-for-Client-Transaction-Analysis/blob/main/DAG2_Liana_Gaisina.py)
Этот файл содержит определение второго DAG в Airflow, который организует процесс ETL с распараллеливанием по продуктам.

* [Скриншот успешного запуска](https://github.com/L-Gaysina/DAG-for-Client-Transaction-Analysis/blob/main/Graph%20DAG2.png)


3. [transform_script.py](https://github.com/L-Gaysina/DAG-for-Client-Transaction-Analysis/blob/main/transform_script.py)
Содержит логику трансформации, необходимую для обработки входных данных и генерации флагов активности клиентов.

## Инструкции по настройке и запуску

### 1. Клонирование репозитория

```bash
git clone https://github.com/L-Gaysina/DAG-for-Client-Transaction-Analysis.git
cd DAG-for-Client-Transaction-Analysis
```
или скопируйте все файлы в директорию dags вашего Airflow. Обычно это ~/airflow/dags/.

### 2. Настроийка окружения
   
Выполните следующие команды, чтобы установить Apache Airflow и другие необходимые библиотеки:   
```bash
pip install apache-airflow
```
```bash
pip install pandas
```
```bash
pip install requests
```
```bash
pip install pendulum
```

### 3. Инициализация базы данных Airflow и запуск веб-сервера и планировщика.

Последовательно выполните следующие команды:

```bash

airflow db init
```
```bash
airflow webserver --port 8080
```
```bash
airflow scheduler
```

### 4. Доступ к интерфейсу Airflow

Откройте браузер и перейдите на http://localhost:8080. Вы должны увидеть DAG1_Liana_Gaisina и DAG2_Liana_Gaisina в списке.

### 5. Запуск DAG

Вы можете вручную запустить DAG из интерфейса Airflow или дождаться его запуска по расписанию (5-го числа каждого месяца).
