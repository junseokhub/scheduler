"""Jenkins 장시간 대기/실행 감시 → 디스코드 알림.

에이전트를 못 받아 큐에서 멈춘 빌드는 파이프라인 코드가 돌기 전이라 파이프라인 안에서는 알릴 수 없다.
그래서 Airflow가 5분마다 Jenkins API를 보고 알린다 (모든 잡 공통, 같은 항목은 한 번만).

- 큐 대기 WAIT_MINUTES 이상: 대기 사유 + 에이전트 파드 스케줄링 실패 사유(Insufficient cpu 등)
- 실행 RUN_MINUTES 이상

필요
- Connection `jenkins_api`: HTTP, host jenkins.jenkins.svc.cluster.local, port 9090, login, password=API 토큰
- Variable `jenkins_discord_webhook_url`: Jenkins 알림 채널 웹훅
- k8s/jenkins-watch-reader.yaml (jenkins ns 파드·이벤트 조회 권한)
"""

import json
import time

import pendulum
from airflow.providers.http.hooks.http import HttpHook
from airflow.sdk import Param, Variable, dag, task

from common.notify import discord_on_failure, post_discord

ALERTED_KEY = "jenkins_watch_alerted"


def find_stuck(queue_items, jobs, now, wait_minutes, run_minutes):
    """알릴 항목 {키: 메시지}. 키로 중복 알림을 막는다."""
    found = {}
    for item in queue_items:
        waited = (now - item["inQueueSince"] / 1000) / 60
        if waited >= wait_minutes:
            found[f"queue:{item['id']}"] = (
                f"⏳ **{item['task']['name']}** {waited:.0f}분째 대기\n사유: {item['why']}"
            )
    for job in jobs:
        build = job.get("lastBuild")
        if build and build["building"]:
            ran = (now - build["timestamp"] / 1000) / 60
            if ran >= run_minutes:
                found[f"build:{job['name']}#{build['number']}"] = (
                    f"🐢 **{job['name']} #{build['number']}** {ran:.0f}분째 실행 중\n{build['url']}"
                )
    return found


def pending_agent_reasons():
    from kubernetes import client, config

    config.load_incluster_config()
    v1 = client.CoreV1Api()
    pending = {p.metadata.name for p in v1.list_namespaced_pod("jenkins", field_selector="status.phase=Pending").items}
    reasons = {}
    for ev in v1.list_namespaced_event("jenkins", field_selector="reason=FailedScheduling").items:
        if ev.involved_object.name in pending:
            reasons[ev.involved_object.name] = ev.message
    return [f"에이전트 파드 `{name}`: {msg}" for name, msg in reasons.items()]


@dag(
    dag_id="jenkins_watch",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 10, 24, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    params={"wait_minutes": Param(10, type="integer"), "run_minutes": Param(30, type="integer")},
    tags=["ops"],
    default_args={"on_failure_callback": discord_on_failure},
)
def jenkins_watch():
    @task
    def check(params=None):
        http = HttpHook(method="GET", http_conn_id="jenkins_api")
        queue = http.run("queue/api/json?tree=items[id,inQueueSince,why,task[name]]").json()["items"]
        jobs = http.run("api/json?tree=jobs[name,lastBuild[number,building,timestamp,url]]").json()["jobs"]
        found = find_stuck(queue, jobs, time.time(), params["wait_minutes"], params["run_minutes"])

        alerted = set(Variable.get(ALERTED_KEY, default="[]", deserialize_json=True))
        new = [msg for key, msg in found.items() if key not in alerted]
        if new:
            if any(key.startswith("queue:") for key in found):
                new += pending_agent_reasons()
            post_discord(Variable.get("jenkins_discord_webhook_url"), "\n\n".join(new)[:1900])
        # 끝난 항목은 지워서 다음에 같은 빌드 번호가 아닌 새 항목만 알리게
        Variable.set(ALERTED_KEY, json.dumps(sorted(found)))
        return sorted(found)

    check()


jenkins_watch()


if __name__ == "__main__":
    now = 10_000_000.0
    q = [{"id": 1, "inQueueSince": (now - 11 * 60) * 1000, "why": "Waiting", "task": {"name": "part of a #1"}},
         {"id": 2, "inQueueSince": (now - 1 * 60) * 1000, "why": "Waiting", "task": {"name": "b"}}]
    j = [{"name": "a", "lastBuild": {"number": 1, "building": True, "timestamp": (now - 31 * 60) * 1000, "url": "u"}},
         {"name": "b", "lastBuild": {"number": 2, "building": False, "timestamp": 0, "url": "u"}},
         {"name": "c", "lastBuild": None}]
    assert sorted(find_stuck(q, j, now, 10, 30)) == ["build:a#1", "queue:1"]
    assert find_stuck(q, j, now, 60, 60) == {}
    print("ok")
