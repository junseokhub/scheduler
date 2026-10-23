from datetime import datetime

from airflow.sdk import dag, task


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False, tags=["example"])
def hello():
    @task
    def say_hello():
        print("hello from airflow")

    say_hello()


hello()
