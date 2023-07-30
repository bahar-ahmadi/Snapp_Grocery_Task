from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from Main import prepare_FlatTable


args = {
 
    'owner': 'Bahar',
    'start_date': days_ago(1)
}
 
dag = DAG(dag_id = 'minio_files', default_args=args, schedule_interval='@daily')
 
 
def run_this_func():
    print('I am coming first')
 

 
with dag:
    run_this_task = PythonOperator(
        task_id='run_this_first',
        python_callable = run_this_func
    )
 
    run_this_task_too = PythonOperator(
        task_id='run_this_last',
        python_callable = prepare_FlatTable
    )
 
    run_this_task >> run_this_task_too