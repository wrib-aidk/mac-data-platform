import dagster as dg


class LoadConfig(dg.Config):
    force: bool = False  # true 면 바뀌지 않은 파일도 전부 다시 적재
