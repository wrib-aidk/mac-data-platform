"""Dagster 리소스. asset 함수 인자에 `postgres: PostgresResource` 를 쓰면 주입된다."""

from contextlib import contextmanager
from collections.abc import Iterator

import dagster as dg
import psycopg

from mac_data_platform import db


class PostgresResource(dg.ConfigurableResource):
    """접속 정보는 .env 의 POSTGRES_* (compose.yaml 과 공유). 처음 연결할 때 sql/*.sql 마이그레이션을 자동 적용."""

    @contextmanager
    def connect(self) -> Iterator[psycopg.Connection]:
        with db.connect() as conn:
            yield conn


defs = dg.Definitions(resources={"postgres": PostgresResource()})
