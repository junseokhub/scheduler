# scheduler

Airflow DAG 저장소. 클러스터의 Airflow가 gitSync로 `main` 브랜치의 `dags/` 폴더를 주기적으로 가져간다.

## 구조

```
dags/
  common/notify.py              공통 실패 알림 (디스코드 웹훅)
  ops/                          운영 감시 DAG
    jenkins_watch.py
  seatflow/                     seatflow 관련 DAG
    seatflow_api_batch.py
    seatflow_pod_batch.py
k8s/                            DAG 실행에 필요한 k8s 리소스
  jenkins-watch-reader.yaml
  seatflow-pod-launcher.yaml
```

## DAG 추가/반영

1. `dags/<프로젝트>/` 아래에 파일을 만든다. 하위 폴더도 재귀로 스캔된다.
2. UI에는 폴더 구분이 없으므로 `tags=["<프로젝트>"]`를 달아서 DAG 목록의 태그 필터로 묶어 본다.
3. `main`에 push하면 1~2분 내 UI에 반영된다. 새 DAG는 기본 pause 상태라 UI에서 토글 ON.
4. 파일을 지우면 DAG는 비활성으로 남고, 실행 이력까지 지우려면 UI에서 삭제한다.

## 실패 알림 (디스코드)

태스크가 실패하면(재시도까지 다 실패한 경우) 디스코드 채널로 DAG·태스크·에러 메시지를 보낸다.

1. 디스코드 채널 설정 → 연동 → 웹후크 → URL 복사
2. Airflow UI → Admin → Variables → `discord_webhook_url`에 URL 저장 (코드에 URL을 넣지 않는다)
3. DAG에 붙이기:

```python
from common.notify import discord_on_failure

@dag(..., default_args={"on_failure_callback": discord_on_failure})
```

Variable이 없으면 알림만 건너뛰고 태스크 결과에는 영향 없다.

## Jenkins 장시간 대기/실행 알림 (`jenkins_watch`)

5분마다 Jenkins API를 보고, 오래 멈춰 있는 빌드를 Jenkins 알림 채널로 보낸다. 모든 잡 공통이고 같은 항목은 한 번만 알린다.

- 큐 대기 10분 이상: 대기 사유 + 에이전트 파드 스케줄링 실패 사유 (`Insufficient cpu` 등)
- 실행 30분 이상

에이전트를 못 받아 멈춘 빌드는 파이프라인 코드가 실행되기 전이라 파이프라인 안에서는 알릴 수 없어서 Airflow에서 감시한다.

사전 준비 (최초 1회):

```bash
kubectl apply -f k8s/jenkins-watch-reader.yaml   # jenkins ns 파드·이벤트 조회 권한

# Jenkins UI → 사용자 → Security → API Token 발급 후
kubectl -n airflow exec -it airflow-scheduler-0 -c scheduler -- \
  airflow connections add jenkins_api --conn-type http \
  --conn-host jenkins.jenkins.svc.cluster.local --conn-port 9090 \
  --conn-login <사용자> --conn-password <API 토큰>
```

Variable `jenkins_discord_webhook_url`에 Jenkins 알림 채널 웹훅을 넣고 UI에서 토글 ON. 기준 시간은 Trigger 때 `wait_minutes`, `run_minutes`로 바꿔 볼 수 있다.

## 배치 방식 고르기

Airflow는 "언제, 어떤 순서로 실행할지"를 관리하는 도구고, 실제 작업을 **어디서** 돌릴지는 오퍼레이터로 정한다.
이 클러스터는 `LocalExecutor`라서 태스크가 scheduler 파드 안에서 프로세스로 실행된다 (동시 실행 수 = scheduler 자원 한도).

| 방식 | 작업이 도는 곳 | 장점 | 단점 | 이럴 때 |
|---|---|---|---|---|
| Python (`@task`) | scheduler 파드 안 | 제일 간단. 코드만 쓰면 끝 | Airflow 자원을 같이 씀. 무거운 작업이면 스케줄러가 느려지거나 OOM. 파이썬만 가능 | 가벼운 조회·가공, 알림, 분기 판단 |
| API (`HttpOperator`) | 이미 떠 있는 서비스 파드 | 서비스 코드·DB 연결을 그대로 재사용. 이미지 빌드 필요 없음 | 요청이 끝날 때까지 기다리므로 오래 걸리면 HTTP 타임아웃. 서비스 재배포·재시작 시 작업이 같이 끊김. 배치 부하가 실제 사용자 트래픽에 영향. 엔드포인트 인증을 따로 챙겨야 함 | 수 초~수십 초 안에 끝나는 작업, 서비스에 "이거 해줘"만 알리면 되는 작업 |
| 파드 (`KubernetesPodOperator`) | 매번 새로 뜨는 배치 전용 파드 | 서비스와 완전히 분리 (재배포·트래픽 영향 없음). 자원(cpu/memory)을 잡마다 따로 지정. Java 등 언어 무관. 끝나면 파드 삭제 | 파드 뜨는 시간(이미지 pull + Spring 기동, 수십 초)이 매번 듦. 이미지에 배치 코드가 들어가 있어야 하고 RBAC·시크릿 설정 필요 | 몇 분~몇 시간 걸리는 배치, 대량 데이터 처리 |
| 파드 + `deferrable=True` | 위와 같음 | 파드가 도는 동안 Airflow 실행 슬롯을 반납하고 triggerer가 상태만 확인 → 장시간 배치 여러 개를 동시에 걸어도 Airflow가 막히지 않음 | 로그가 실시간이 아니라 `logging_interval` 간격으로 들어옴. triggerer에도 권한 필요 | 장시간 배치의 기본값 |

판단 기준: **짧으면 API, 길거나 무거우면 파드(deferrable)**. 애매하면 파드로 시작한다.

| 예시 DAG | 방식 |
|---|---|
| `seatflow_api_batch` | API |
| `seatflow_pod_batch` | 파드 + deferrable |

## seatflow DAG 사용법

### 사전 준비 (최초 1회)

```bash
# 파드 방식: Airflow가 seatflow ns에 파드를 띄울 권한
kubectl apply -f k8s/seatflow-pod-launcher.yaml

# API 방식: seat-service management 포트 Connection
kubectl -n airflow exec airflow-scheduler-0 -c scheduler -- \
  airflow connections add seatflow_seat --conn-type http \
  --conn-host seat-service.seatflow.svc.cluster.local --conn-port 9084
```

### seatflow_api_batch

UI에서 Trigger. `seat-service /actuator/health`가 `UP`이면 성공.

### seatflow_pod_batch

UI에서 Trigger → `image_tag`에 seat-service 이미지 태그(커밋 해시)를 넣는다.

- `pod_smoke`: 자바 없이 문자열만 찍고 종료. 권한·이미지 pull 확인용
- `hello_batch`: Spring 컨텍스트를 띄워 `HelloBatchRunner` 실행 → 좌석 수 로그 → 종료
  - `HelloBatchRunner`가 포함된 이미지여야 한다. 없는 이미지면 서비스가 그냥 떠서 30분 뒤 `active_deadline_seconds`로 실패
  - 현재 배포 태그 확인: `kubectl -n seatflow get deploy seat-service -o jsonpath='{.spec.template.spec.containers[0].image}'`

로그는 태스크 로그에 그대로 나온다. 파드는 끝나면 삭제된다.

### 새 배치 잡 추가 (파드 방식)

1. 서비스에 `@ConditionalOnProperty(name = "batch.job", havingValue = "<잡이름>")` 붙인 `ApplicationRunner`를 만들고, 끝에서 `System.exit(SpringApplication.exit(context, () -> 0))`
2. DAG에서 `--batch.job=<잡이름>`과 `--spring.kafka.listener.auto-startup=false`(서비스 컨슈머 그룹 보호)를 arguments로 넘긴다
3. 서비스 시크릿은 `env_from`으로 그대로 주입 (`seat-service-secret` 참고)
