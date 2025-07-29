from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label 



#Dinh nghia DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
@dag( dag_id = 'condition_decarators_dag',
     start_date = datetime(2025,7,3),
     schedule = '@daily',
     catchup = False,
     default_args = default_args,
     description = 'DAG with condition decorators',
     tags  = ['example']
    )
def condition_decorators():
    @task.branch(task_id = 'branching')
    def choose_branch( a ):
        if a>5:
            return 'task_a'
        else:
            return 'task_b'
    @task(task_id ='task_a')
    def task_a_func():
        print("Chay task A - So lon hon 5")
        return 10
    @task(task_id = 'taks_b')
    def task_b_func():
        print("Chay task B - So nho hon 5")
        return 3
    
    choose = choose_branch(10)
    task_a = task_a_func()
    task_b = task_b_func()
    # sum = sum_task(task_a,task_b)
    '''
    choose
    choose>>task_>>sum
    choose>>task_b>>sum
    '''
    choose>>Label("neu a>5")>>task_a
    choose>>Label("neu a<5")>>task_b
condition_decorators_dag = condition_decorators()
