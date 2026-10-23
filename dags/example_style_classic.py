"""전통 방식(Operator) 예시: with DAG + Operator + >> 로 순서 연결.

example_style_taskflow.py 와 같은 일을 한다. 비교용.
- 태스크 = Operator 객체, 순서 = >>
- 태스크 간 값 전달은 ti.xcom_pull 로 직접 꺼냄
"""

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def extract():
    return [{"id": i, "amount": i * 1000} for i in range(1, 4)]


def transform(ti):
    rows = ti.xcom_pull(task_ids="extract")
    return sum(row["amount"] for row in rows)


with DAG(
    dag_id="example_style_classic",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example", "style"],
):
    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    notify_task = BashOperator(
        task_id="notify",
        bash_command="echo \"total={{ ti.xcom_pull(task_ids='transform') }}\"",
    )

    extract_task >> transform_task >> notify_task
