"""TaskFlow API 예시: 추출 → 변환 → 적재.

- 매일 09:00(Asia/Seoul) 실행, 실패 시 2번 재시도
- params: UI/API로 실행할 때 값을 넘길 수 있음 (limit)
- Variable: UI(Admin → Variables)에서 관리하는 설정값
- 태스크 간 값 전달은 return 값으로 (XCom 자동 처리)
"""

from datetime import timedelta

import pendulum
from airflow.sdk import Param, Variable, dag, task


@dag(
    dag_id="example_etl",
    schedule="0 9 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    params={"limit": Param(3, type="integer", minimum=1)},
    tags=["example"],
)
def example_etl():
    @task
    def extract(**context) -> list[dict]:
        limit = context["params"]["limit"]
        return [{"id": i, "amount": i * 1000} for i in range(1, limit + 1)]

    @task
    def transform(rows: list[dict]) -> int:
        return sum(row["amount"] for row in rows)

    @task
    def load(total: int):
        prefix = Variable.get("report_prefix", default="[report]")
        print(f"{prefix} total={total}")

    load(transform(extract()))


example_etl()
