# Airflow task for university
## Set up
For run code:

``docker-compouse up --build``

But before start dags need to take next step from [official documentation](https://github.com/puckel/docker-airflow#usage):

    Go to Admin -> Connections and Edit "postgres_default" set this values :
        - Host : postgres
        - Schema : airflow
        - Login : airflow
        - Password : airflow

After it, you can start dags.
 - generate_data - create csv files in ./data/
 - transform_data - scrapes data from files and transfers it to the postgres database

Also, you can copy files from ./test_files/
