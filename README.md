# scheduler

Airflow DAG 저장소. 클러스터의 Airflow가 gitSync로 `main` 브랜치의 `dags/` 폴더를 주기적으로 가져간다.

- DAG 파일은 `dags/` 아래에 둔다.
- push 후 약 1분 내 UI에 반영된다.
