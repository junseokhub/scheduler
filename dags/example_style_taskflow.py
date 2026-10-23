"""코드 방식(TaskFlow API) 예시: 평범한 Python 함수 + 데코레이터.

example_style_classic.py 와 같은 일을 한다. 비교용.
- 태스크 = @task 함수, 순서 = 함수 호출 관계
- 태스크 간 값 전달은 return / 인자로 (XCom 자동)
- @task.bash 처럼 셸 명령도 함수로 표현 가능
"""

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="example_style_taskflow",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example", "style"],
)
def example_style_taskflow():
    @task
    def extract() -> list[dict]:
        return [{"id": i, "amount": i * 1000} for i in range(1, 4)]

    @task
    def transform(rows: list[dict]) -> int:
        return sum(row["amount"] for row in rows)

    @task.bash
    def notify(total: int) -> str:
        return f'echo "total={total}"'

    notify(transform(extract()))


example_style_taskflow()
