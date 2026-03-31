"""
공통 설정 - 실행 환경에 따라 자동으로 접속 정보를 결정합니다.

- Docker 컨테이너 안: 환경변수 사용 (broker1:29092, connect:8083, ...)
- 로컬 macOS: 기본값 localhost 사용

Monitoring Interceptor는 Docker(Linux) 환경에서만 활성화됩니다.
"""

import os
import platform

BOOTSTRAP_SERVERS = os.environ.get(
    "BOOTSTRAP_SERVERS",
    "localhost:9092,localhost:9093,localhost:9094",
)

CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083")
KSQLDB_URL = os.environ.get("KSQLDB_URL", "http://localhost:8088")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")

# Linux(Docker 컨테이너)에서만 interceptor 활성화
INTERCEPTOR_CONFIG = {}
if platform.system() == "Linux":
    INTERCEPTOR_CONFIG = {"plugin.library.paths": "monitoring-interceptor"}
