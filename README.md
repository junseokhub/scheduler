# scheduler

Airflow DAG 저장소. 클러스터의 Airflow가 gitSync로 `main` 브랜치의 `dags/` 폴더를 주기적으로 가져간다.

## 구조

```
dags/
  seatflow/                     seatflow 관련 DAG
    seatflow_api_batch.py
    seatflow_pod_batch.py
k8s/                            DAG 실행에 필요한 k8s 리소스
  seatflow-pod-launcher.yaml
```

## DAG 추가/반영

1. `dags/<프로젝트>/` 아래에 파일을 만든다. 하위 폴더도 재귀로 스캔된다.
2. UI에는 폴더 구분이 없으므로 `tags=["<프로젝트>"]`를 달아서 DAG 목록의 태그 필터로 묶어 본다.
3. `main`에 push하면 1~2분 내 UI에 반영된다. 새 DAG는 기본 pause 상태라 UI에서 토글 ON.
4. 파일을 지우면 DAG는 비활성으로 남고, 실행 이력까지 지우려면 UI에서 삭제한다.

## 배치 방식 고르기

| 방식 | 언제 | 예시 |
|---|---|---|
| API (HttpOperator) | 짧은 작업. 떠 있는 서비스에 요청만 보냄. 오래 걸리면 HTTP 타임아웃·서비스 재배포에 같이 끊김 | `seatflow_api_batch` |
| 파드 (KubernetesPodOperator) | 장시간 배치. 서비스 이미지를 배치 전용 파드로 띄우고 끝나면 파드 종료. `deferrable=True`면 대기 중 실행 슬롯을 반납 | `seatflow_pod_batch` |

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
