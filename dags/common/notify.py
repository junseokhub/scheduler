"""태스크 실패 시 디스코드 웹훅으로 알림.

웹훅 URL은 Airflow Variable `discord_webhook_url` (없으면 알림만 건너뜀)
사용: @dag(default_args={"on_failure_callback": discord_on_failure})
"""

import json
import logging
import urllib.request

from airflow.sdk import Variable

log = logging.getLogger(__name__)


def discord_on_failure(context):
    url = Variable.get("discord_webhook_url", default=None)
    if not url:
        log.warning("discord_webhook_url Variable 없음 → 알림 생략")
        return

    ti = context["ti"]
    content = (
        f"❌ **{ti.dag_id}.{ti.task_id}** 실패 (try {ti.try_number})\n"
        f"run_id: `{context['run_id']}`\n"
        f"```{str(context.get('exception'))[:1500]}```"
    )
    req = urllib.request.Request(
        url,
        data=json.dumps({"content": content}).encode(),
        # User-Agent 없으면 디스코드(Cloudflare)가 403
        headers={"Content-Type": "application/json", "User-Agent": "airflow"},
    )
    urllib.request.urlopen(req, timeout=10)
