# DAG-for-Client-Transaction-Analysis
Этот проект предназначен для создания ориентированного ациклического графа (DAG) в Apache Airflow, который будет периодически рассчитывать витрину активности клиентов на основе суммы и количества их транзакций.


Инструкции по настройке и запуску
Требования
Установленный и запущенный Apache Airflow.
Убедитесь, что profit_table.csv находится в директории data для первого DAG и data_2 для второго DAG.
Шаги для запуска DAG
1. Клонирование репозитория

```bash
git clone https://github.com/L-Gaysina/DAG-for-Client-Transaction-Analysis.git
cd DAG-for-Client-Transaction-Analysis
```
2. Настроийка окружения
Выполните следующие команды, чтобы установить Apache Airflow и другие необходимые библиотеки:   
```bash
pip install apache-airflow
pip install pandas
pip install requests
pip install pendulum
```

3. Инициализация базы данных Airflow и запустите веб-сервер и планировщик.

```bash

airflow db init
airflow webserver --port 8080
airflow scheduler
Разместить файлы DAG
```
Убедитесь, что файлы DAG DAG1_Liana_Gaisina.py и DAG2_Liana_Gaisina.py находятся в директории DAG Airflow. Обычно это ~/airflow/dags/.

4. Доступ к интерфейсу Airflow

Откройте браузер и перейдите на http://localhost:8080. Вы должны увидеть DAG client_transaction_analysis в списке.

5. Запуск DAG

Вы можете вручную запустить DAG из интерфейса Airflow или дождаться его запуска по расписанию (5-го числа каждого месяца).
