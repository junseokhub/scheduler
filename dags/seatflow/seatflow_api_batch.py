"""API 방식: 떠 있는 seatflow 서비스에 HTTP로 요청만 보낸다.

짧은 작업용. 작업은 서비스 파드 안에서 돌기 때문에 오래 걸리면 HTTP 타임아웃·서비스 재배포에 같이 끊긴다.
→ 장시간 배치는 seatflow_pod_batch(파드 방식)를 쓴다.

Connection `seatflow_seat` 필요 (management 포트라 Kong을 안 거치고 인증도 없음)
- Connection Type: HTTP
- Host: seat-service.seatflow.svc.cluster.local
- Port: 9084
"""

import pendulum
from airflow.providers.http.operators.http import HttpOperator
from airflow.sdk import dag

from common.notify import discord_on_failure


@dag(
    dag_id="seatflow_api_batch",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["seatflow"],
    default_args={"on_failure_callback": discord_on_failure},
)
def seatflow_api_batch():
    HttpOperator(
        task_id="seat_health",
        http_conn_id="seatflow_seat",
        endpoint="actuator/health",
        method="GET",
        response_check=lambda response: response.json()["status"] == "UP",
        log_response=True,
    )


seatflow_api_batch()
