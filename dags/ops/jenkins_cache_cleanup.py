"""Jenkins 빌드 캐시 정리 (노드 hostPath /var/cache/jenkins-agent).

buildkit local 캐시(--export-cache type=local)는 오래된 레이어를 스스로 지우지 않아서 계속 커진다.
노드마다 정리 파드를 띄워 buildkit 캐시 합계가 limit_gb를 넘으면 비운다 (다음 빌드 1회는 캐시 없이 빌드).

- gradle 캐시는 Gradle이 30일 지난 항목을 스스로 정리하므로 건드리지 않는다
- 최근 60분 안에 캐시가 쓰였으면 빌드 중일 수 있어 그 노드는 건너뛴다
- 결과(노드별 용량, 삭제 여부)는 태스크 로그에 남는다
"""

import pendulum
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import Param, dag
from kubernetes.client import models as k8s

from common.notify import discord_on_failure

NODES = ["ubuntu", "worker-node", "worker-node2"]

SCRIPT = """
D=/cache
du -sh $D/* 2>/dev/null || { echo "캐시 없음"; exit 0; }
SIZE=$(du -smc $D/buildkit-* 2>/dev/null | tail -1 | cut -f1)
LIMIT=$((LIMIT_GB * 1024))
if [ "${SIZE:-0}" -lt "$LIMIT" ]; then echo "buildkit ${SIZE:-0}MB < ${LIMIT}MB → 유지"; exit 0; fi
if [ -n "$(find $D -mmin -60 -print 2>/dev/null | head -1)" ]; then echo "최근 60분 안에 사용됨 (빌드 중일 수 있음) → 건너뜀"; exit 0; fi
rm -rf $D/buildkit-* && echo "buildkit ${SIZE}MB 삭제"
"""


@dag(
    dag_id="jenkins_cache_cleanup",
    schedule="0 4 * * 0",  # 매주 일요일 04:00 (빌드가 거의 없는 시간)
    start_date=pendulum.datetime(2026, 10, 25, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    params={"limit_gb": Param(10, type="integer", minimum=1)},
    tags=["ops"],
    default_args={"on_failure_callback": discord_on_failure},
)
def jenkins_cache_cleanup():
    KubernetesPodOperator.partial(
        task_id="cleanup",
        name="jenkins-cache-cleanup",
        namespace="airflow",
        image="busybox:1.36",
        cmds=["sh", "-c", SCRIPT],
        env_vars={"LIMIT_GB": "{{ params.limit_gb }}"},
        volumes=[k8s.V1Volume(
            name="cache",
            host_path=k8s.V1HostPathVolumeSource(path="/var/cache/jenkins-agent", type="DirectoryOrCreate"),
        )],
        volume_mounts=[k8s.V1VolumeMount(name="cache", mount_path="/cache")],
        container_resources=k8s.V1ResourceRequirements(requests={"cpu": "10m", "memory": "16Mi"}),
        get_logs=True,
        on_finish_action="delete_pod",
    ).expand(node_selector=[{"kubernetes.io/hostname": n} for n in NODES])


jenkins_cache_cleanup()
