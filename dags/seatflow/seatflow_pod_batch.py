"""파드 방식: seat-service 이미지를 배치 전용 파드로 따로 띄워서 실행하고, 끝나면 파드가 종료된다.

서비스 파드와 분리돼 있어서 장시간 배치도 HTTP 타임아웃·서비스 재배포와 무관하다.
- 파드는 seatflow ns에서 뜬다 (seat-service-secret, ghcr-secret이 거기 있음)
  → 먼저 k8s/seatflow-pod-launcher.yaml 적용 필요
- deferrable=True: 파드가 도는 동안 실행 슬롯을 반납하고 triggerer가 상태를 폴링 (장시간 배치용)
- image_tag: HelloBatchRunner가 들어간 seat-service 이미지 태그로 트리거
  (러너 없는 옛 이미지면 서비스가 그냥 떠버려서 active_deadline_seconds에 걸려 실패함)
"""

import pendulum
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import Param, dag
from kubernetes.client import models as k8s

IMAGE = "ghcr.io/junseokhub/seatflow-seat-service:{{ params.image_tag }}"

common = dict(
    namespace="seatflow",
    image=IMAGE,
    image_pull_secrets=[k8s.V1LocalObjectReference(name="ghcr-secret")],
    node_selector={"kubernetes.io/arch": "arm64"},
    get_logs=True,
    on_finish_action="delete_pod",
)


@dag(
    dag_id="seatflow_pod_batch",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    params={"image_tag": Param("20cb199", type="string")},
    tags=["seatflow"],
)
def seatflow_pod_batch():
    # 권한·이미지 pull 확인용. 자바 안 띄우고 문자열만 찍고 끝남 (지금 이미지로도 동작)
    smoke = KubernetesPodOperator(
        task_id="pod_smoke",
        name="seat-batch-smoke",
        cmds=["sh", "-c"],
        arguments=['echo "hello from seat-service image, date=$BATCH_DATE"'],
        env_vars={"BATCH_DATE": "{{ dag_run.run_after | ds }}"},
        container_resources=k8s.V1ResourceRequirements(requests={"cpu": "10m", "memory": "16Mi"}),
        **common,
    )

    # 실제 Spring 컨텍스트를 띄워 HelloBatchRunner 실행 → DB 조회 → System.exit
    hello = KubernetesPodOperator(
        task_id="hello_batch",
        name="seat-batch-hello",
        arguments=[
            "--batch.job=hello",
            "--batch.date={{ dag_run.run_after | ds }}",
            "--spring.kafka.listener.auto-startup=false",  # 서비스 파드의 컨슈머 그룹 파티션을 뺏지 않게
        ],
        env_from=[k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="seat-service-secret"))],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "100m", "memory": "384Mi"},
            limits={"cpu": "1", "memory": "768Mi"},
        ),
        active_deadline_seconds=1800,  # 30분 넘으면 강제 종료. 장시간 배치는 잡별로 늘린다
        deferrable=True,
        logging_interval=10,
        **common,
    )

    smoke >> hello


seatflow_pod_batch()
