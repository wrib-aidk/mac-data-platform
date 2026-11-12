#!/bin/zsh
# Dagster UI 실행 (127.0.0.1:3000, 외부 주소 /dagster/)
cd "$(dirname "$0")"
export DAGSTER_HOME="$PWD/.dagster_home"   # 실행 기록·스케줄 상태를 여기에 보관 (없으면 재시작 때마다 초기화됨)
mkdir -p "$DAGSTER_HOME"
# PostgreSQL (compose.yaml) 이 꺼져 있으면 켜고 준비될 때까지 대기. Docker Desktop 이 안 켜져 있으면 경고만
if docker info >/dev/null 2>&1; then
  docker compose up -d --wait postgres >/dev/null || echo "⚠️ PostgreSQL 시작 실패 (docker compose logs postgres 확인)"
else
  echo "⚠️ Docker 가 실행 중이 아님 — DB 적재(db/*) asset 은 실패합니다. Docker Desktop 을 켜 주세요."
fi
DAGSTER_WEBSERVER_PATH_PREFIX=/dagster exec uv run dg dev --host 127.0.0.1 --port 3000
