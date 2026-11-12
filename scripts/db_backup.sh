#!/bin/zsh
# PostgreSQL 백업/복원 (다른 PC 로 옮길 때)
#   ./scripts/db_backup.sh dump            → backups/platform_YYYYmmdd_HHMM.dump
#   ./scripts/db_backup.sh restore <파일>   → 지금 DB 에 덮어써서 복원
# 새 PC: 프로젝트 복사 + .env 복사 → docker compose up -d → restore
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a
case "${1:-}" in
  dump)
    mkdir -p backups
    out="backups/${POSTGRES_DB}_$(date +%Y%m%d_%H%M).dump"
    docker compose exec -T postgres pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc > "$out"
    echo "백업 완료: $out ($(du -h "$out" | cut -f1))" ;;
  restore)
    [[ -f "${2:-}" ]] || { echo "사용법: $0 restore <dump 파일>"; exit 1; }
    docker compose exec -T postgres pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" --clean --if-exists --no-owner < "$2"
    echo "복원 완료: $2" ;;
  *) echo "사용법: $0 dump | restore <파일>"; exit 1 ;;
esac
