"""태스크 실패 시 디스코드 웹훅으로 알림.

웹훅 URL은 Airflow Variable `discord_webhook_url` (없으면 알림만 건너뜀)
사용: @dag(default_args={"on_failure_callback": discord_on_failure})
"""

import json
import logging
import urllib.request

from airflow.sdk import Variable

log = logging.getLogger(__name__)

# 디스코드 메시지 최대 2000자
REASON_LIMIT = 1400


def discord_on_failure(context):
    url = Variable.get("discord_webhook_url", default=None)
    if not url:
        log.warning("discord_webhook_url Variable 없음 → 알림 생략")
        return

    ti = context["ti"]
    exc = context.get("exception")
    reason = f"{type(exc).__name__}: {exc}" if exc else "원인 정보 없음 (로그 확인)"
    if len(reason) > REASON_LIMIT:
        # 스택트레이스·컨테이너 로그는 끝부분에 원인이 있으므로 뒤를 남긴다
        reason = "..." + reason[-REASON_LIMIT:]

    content = (
        f"❌ **{ti.dag_id}.{ti.task_id}** 실패 (try {ti.try_number})\n"
        f"run_id: `{context['run_id']}`\n"
        f"원인:\n```{reason}```\n"
        f"로그: {ti.log_url}"
    )
    post_discord(url, content)


def post_discord(url, content):
    req = urllib.request.Request(
        url,
        data=json.dumps({"content": content}).encode(),
        # User-Agent 없으면 디스코드(Cloudflare)가 403
        headers={"Content-Type": "application/json", "User-Agent": "airflow"},
    )
    urllib.request.urlopen(req, timeout=10)
