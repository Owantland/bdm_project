from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from epx_an import query_spatial_event
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1)
}

with DAG(
    dag_id='bdm_data_pipeline',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['bdm', 'connection_test'],
) as dag:

    events = SparkSubmitOperator(
        task_id='events',
        application='/opt/airflow/dags/events_zone.py',
        jars='/spark-shared/jars/postgresql-42.7.4.jar',
        conn_id='spark_master',
        verbose=True,
        dag=dag,
    )
    cultural = SparkSubmitOperator(
        task_id='cultural',
        application='/opt/airflow/dags/cultural_main.py',
        jars='/spark-shared/jars/postgresql-42.7.4.jar',
        conn_id='spark_master',
        verbose=True,
        dag=dag,
    )
    accomodation = SparkSubmitOperator(
        task_id='accomodation',
        application='/opt/airflow/dags/amain.py',
        jars='/spark-shared/jars/postgresql-42.7.4.jar',
        conn_id='spark_master',
        verbose=True,
        dag=dag,
    )
    weather = SparkSubmitOperator(
        task_id='weather',
        application='/opt/airflow/dags/wmain.py',
        jars='/spark-shared/jars/postgresql-42.7.4.jar',
        conn_id='spark_master',
        verbose=True,
        dag=dag,
    )
    queries = PythonOperator(
        task_id='queries',
        python_callable=query_spatial_event,
        op_args=['paris'],
        op_kwargs={'filter_sql': '"SOCIAL_LIFE" = 1'},
        dag=dag
    )
    [events] >> queries


  
      

