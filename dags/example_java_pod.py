"""KubernetesPodOperator 예시: 컨테이너 이미지를 태스크로 실행.

Java/Spring Batch 등 Python이 아닌 코드는 이미지로 빌드해서 이렇게 실행한다.
- image: 실제로는 직접 빌드한 이미지 (예: ghcr.io/junseokhub/batch-job:1.0.0)
- 노드가 arm64라 이미지도 arm64(또는 멀티아키텍처)여야 함
- env_vars 의 {{ ds }} 는 실행 날짜(YYYY-MM-DD)로 치환됨
"""

import pendulum
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import dag
from kubernetes.client import models as k8s


@dag(
    dag_id="example_java_pod",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example"],
)
def example_java_pod():
    KubernetesPodOperator(
        task_id="run_java",
        name="run-java",
        namespace="airflow",
        image="eclipse-temurin:21-jre",
        # 실제 jar 이미지라면: cmds=["java", "-jar", "/app/app.jar"], arguments=["--job.date={{ ds }}"]
        cmds=["sh", "-c"],
        arguments=['echo "JOB_DATE=$JOB_DATE" && java -version'],
        env_vars={"JOB_DATE": "{{ ds }}"},
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "100m", "memory": "256Mi"},
            limits={"cpu": "1", "memory": "1Gi"},
        ),
        node_selector={"kubernetes.io/arch": "arm64"},
        get_logs=True,
        on_finish_action="delete_pod",
    )


example_java_pod()
