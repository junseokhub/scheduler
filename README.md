# scheduler

Airflow DAG 저장소. 클러스터의 Airflow가 gitSync로 `main` 브랜치의 `dags/` 폴더를 주기적으로 가져간다.

- DAG 파일은 `dags/` 아래에 둔다.
- push 후 1~2분 내 UI에 반영된다. (새 DAG는 기본 pause → UI에서 토글 ON)

## seatflow (`dags/seatflow/`)

| 파일 | 내용 |
|---|---|
| `seatflow_api_batch.py` | API 방식 — 떠 있는 seat-service에 HttpOperator로 호출 (Connection `seatflow_seat` 필요) |
| `seatflow_pod_batch.py` | 파드 방식 — seat-service 이미지를 배치 파드로 실행 후 종료, deferrable (`k8s/seatflow-pod-launcher.yaml` 적용 필요) |
