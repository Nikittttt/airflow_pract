from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.sensors.python_sensor import PythonSensor
import psycopg2
from datetime import datetime
from random import randint, choice
from time import sleep
from datetime import datetime
import csv
import string


def random_string(n):
    return ''.join(choice(string.ascii_uppercase + string.digits) for _ in range(n))

def generate_csv(**kwargs):
    start_data = datetime(1970, 1, 1)
    file_name = f'/data/file_{randint(1, 999999999)}.csv'
    print(file_name)
    with open(file_name, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['name_user', 'reason_ban', 'time_ban'])
        for _ in range(randint(1, 1000)):
            data_from = (datetime(1970, 1, 2) - start_data).total_seconds()
            data_to = (datetime(2030, 1, 2) - start_data).total_seconds()
            writer.writerow([random_string(randint(1, 10)),
                             random_string(randint(1, 10)),
                             randint(int(data_from), int(data_to))*1000])

# Following are defaults which can be overridden later on
args = {
    'owner': 'nikita',
    'start_date': datetime(2022, 1, 1),
    'provide_context': True
}


with DAG('generate_data', description='generate data', schedule_interval='*/1 * * * *', catchup=False, default_args=args) as dag:  # 0 * * * *   */1 * * * *

    PythonOperator(
        task_id='generate_csv',
        python_callable=generate_csv)
