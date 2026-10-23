"""HttpOperator 예시: 외부/내부 REST API 호출.

Connection `airflow_api` 필요 (UI: Admin → Connections → +)
- Connection Type: HTTP
- Host: http://airflow-api-server.airflow.svc.cluster.local
- Port: 8080
"""

import pendulum
from airflow.providers.http.operators.http import HttpOperator
from airflow.sdk import dag


@dag(
    dag_id="example_http",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example"],
)
def example_http():
    HttpOperator(
        task_id="health_check",
        http_conn_id="airflow_api",
        endpoint="api/v2/monitor/health",
        method="GET",
        response_check=lambda response: response.json()["scheduler"]["status"] == "healthy",
        log_response=True,
    )


example_http()
