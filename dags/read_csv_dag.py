from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.sensors.python_sensor import PythonSensor
import psycopg2
from datetime import datetime

conn = psycopg2.connect(dbname="airflow", user="airflow",
                        password="airflow", host="postgres", port=5432)
conn.autocommit = True
cursor = conn.cursor()


def _wait_for_csv(**kwargs):
    from pathlib import Path

    ti = kwargs['ti']
    path_ = Path("/data/")
    data_files = path_.rglob("*.csv")
    files = [str(i) for i in data_files]
    ti.xcom_push(key='files', value=files)
    return bool(files)


def extract_data(**kwargs):
    import csv

    ti = kwargs['ti']
    list_files = ti.xcom_pull(key='files', task_ids=['_wait_for_csv'])[0]
    data = {'file_name': [], 'name_user': [], 'reason_ban': [], 'time_ban': []}
    work_columns = {'name_user', 'reason_ban', 'time_ban'}
    for file in list_files:
        with open(file) as csvfile:
            reader = csv.DictReader(csvfile)
            if set(reader.fieldnames) != work_columns:
                list_files.remove(file)
                continue
            for row in reader:
                for key_ in work_columns:
                    data[key_].append(row[key_])
                data['file_name'].append(file)
    print(data)
    print(list_files)
    if list_files:
        ti.xcom_push(key='files', value=list_files)
        ti.xcom_push(key='data', value=data)
    else:
        raise Warning('No files to process')


def transform_data(**kwargs):
    from datetime import datetime

    ti = kwargs['ti']
    data_taken = ti.xcom_pull(key='data', task_ids=['extract_data'])[0]
    final_data = {keys_: [] for keys_ in data_taken.keys()}
    final_data['file_name'] = data_taken['file_name']
    final_data['name_user'] = data_taken['name_user']
    final_data['reason_ban'] = data_taken['reason_ban']
    final_data['time_ban'] = []
    for time in data_taken['time_ban']:
        correct_time = int(time)//1000
        time_to_str = datetime.fromtimestamp(correct_time).strftime("%Y-%m-%d %H:%M:%S")
        final_data['time_ban'].append(time_to_str)
    print(final_data)
    ti.xcom_push(key='data', value=final_data)
        

def insert_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='data', task_ids=['transform_data'])[0]
    value = list(zip(*list(data.values())))
    sql_insert = f'''
    insert into savedcsv({", ".join(data.keys())}) values
    {", ".join(str(i) for i in value)}
    '''
    print(sql_insert)
    cursor.execute(sql_insert)
    conn.commit()


def del_csv_files(**kwargs):
    import os

    ti = kwargs['ti']
    for file in ti.xcom_pull(key='files', task_ids=['extract_data'])[0]:
        os.remove(file)

# Following are defaults which can be overridden later on
args = {
    'owner': 'nikita',
    'start_date': datetime(2022, 1, 1),
    'provide_context': True
}

sql_create_tables = """
CREATE TABLE IF NOT EXISTS savedcsv (
                            file_name VARCHAR NOT NULL,
                            name_user VARCHAR NOT NULL,
                            reason_ban VARCHAR NOT NULL,
                            time_ban TIMESTAMP NOT NULL
);
"""

with DAG('transform_data', description='transform data from csv to postgres', schedule_interval='*/2 * * * *',  catchup=False, default_args=args) as dag: #0 * * * *   */1 * * * *

    create_table = PostgresOperator(
        task_id="create_table",
        sql=sql_create_tables
    )
    sensor_data = PythonSensor(
        task_id="_wait_for_csv",
        python_callable=_wait_for_csv,
        poke_interval=30,
        timeout=1 * 60,
        mode='reschedule'
    )
    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data)
    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data)
    insert = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data)
    del_csv = PythonOperator(
        task_id='del_csv_files',
        python_callable=del_csv_files)
    create_table >> sensor_data
    sensor_data >> extract
    extract >> transform
    transform >> insert
    insert >> del_csv
