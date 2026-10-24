# scheduler

Airflow DAG 저장소. 클러스터의 Airflow가 gitSync로 `main` 브랜치의 `dags/` 폴더를 주기적으로 가져간다.

- DAG 파일은 `dags/` 아래에 둔다.
- push 후 1~2분 내 UI에 반영된다. (새 DAG는 기본 pause → UI에서 토글 ON)

## 예시

| 파일 | 내용 |
|---|---|
| `hello_dag.py` | 최소 DAG |
| `example_etl.py` | TaskFlow ETL — params, Variable, 재시도, cron 스케줄 |
| `example_http.py` | HttpOperator로 REST API 호출 (Connection `airflow_api` 필요) |
| `example_java_pod.py` | KubernetesPodOperator로 Java 컨테이너 실행 |
| `example_style_classic.py` | 전통 방식 (with DAG + Operator + `>>`) |
| `example_style_taskflow.py` | 코드 방식 (TaskFlow `@dag` / `@task`) — classic과 같은 동작 |

## seatflow (`dags/seatflow/`)

| 파일 | 내용 |
|---|---|
| `seatflow_api_batch.py` | API 방식 — 떠 있는 seat-service에 HttpOperator로 호출 (Connection `seatflow_seat` 필요) |
| `seatflow_pod_batch.py` | 파드 방식 — seat-service 이미지를 배치 파드로 실행 후 종료, deferrable (`k8s/seatflow-pod-launcher.yaml` 적용 필요) |
