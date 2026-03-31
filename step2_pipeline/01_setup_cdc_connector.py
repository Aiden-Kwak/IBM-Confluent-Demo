"""
Step 2-1: Debezium CDC Connector 설정
=======================================
Debezium을 사용해 PostgreSQL의 변경사항(INSERT/UPDATE/DELETE)을
실시간으로 Kafka 토픽에 캡처합니다.

CDC (Change Data Capture)란?
- 데이터베이스의 변경사항을 실시간으로 감지하여 이벤트로 전달
- PostgreSQL의 WAL(Write-Ahead Log)을 읽어서 변경사항 추출
- 애플리케이션 코드 수정 없이 DB 변경을 스트리밍 가능

Kafka Connect란?
- 외부 시스템 ↔ Kafka 간 데이터를 이동시키는 프레임워크
- Source Connector: 외부 → Kafka (이번 실습)
- Sink Connector: Kafka → 외부
- 코드 없이 설정(JSON)만으로 파이프라인 구축 가능
"""

import json
import sys
import time

sys.path.insert(0, sys.path[0] + "/..")
from config import CONNECT_URL

import requests


def wait_for_connect():
    """Kafka Connect가 준비될 때까지 대기"""
    print("Kafka Connect 준비 대기 중...")
    for i in range(30):
        try:
            r = requests.get(f"{CONNECT_URL}/connectors")
            if r.status_code == 200:
                print("  → Kafka Connect 준비 완료!\n")
                return True
        except requests.ConnectionError:
            pass
        time.sleep(5)
        print(f"  대기 중... ({(i+1)*5}초)")
    print("  [ERROR] Kafka Connect 연결 실패")
    return False


def create_debezium_source():
    """Debezium PostgreSQL Source Connector 생성"""

    connector_config = {
        "name": "postgres-cdc-source",
        "config": {
            # Debezium PostgreSQL Connector
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "tasks.max": "1",
            # PostgreSQL 연결 정보
            "database.hostname": "postgres",
            "database.port": "5432",
            "database.user": "confluent",
            "database.password": "confluent",
            "database.dbname": "shop",
            # 토픽 접두사 - 토픽 이름: {prefix}.{schema}.{table}
            "topic.prefix": "cdc",
            # 캡처할 테이블 지정
            "table.include.list": "public.orders,public.products",
            # 스키마 변경도 캡처
            "include.schema.changes": "true",
            # 스냅샷 모드: 최초 시작 시 기존 데이터도 캡처
            "snapshot.mode": "initial",
            # WAL 플러그인
            "plugin.name": "pgoutput",
            # 키 변환기
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
        },
    }

    print("Debezium CDC Source Connector 생성 중...")
    print(f"  설정: {json.dumps(connector_config['config'], indent=2)}\n")

    r = requests.put(
        f"{CONNECT_URL}/connectors/postgres-cdc-source/config",
        headers={"Content-Type": "application/json"},
        data=json.dumps(connector_config["config"]),
    )

    if r.status_code in (200, 201):
        print("  → Connector 생성 성공!")
    else:
        print(f"  → 실패: {r.status_code} - {r.text}")
        return

    # 상태 확인
    time.sleep(5)
    r = requests.get(f"{CONNECT_URL}/connectors/postgres-cdc-source/status")
    status = r.json()
    print(f"\n  Connector 상태: {status['connector']['state']}")
    for task in status.get("tasks", []):
        print(f"  Task {task['id']} 상태: {task['state']}")


def list_connectors():
    """현재 등록된 커넥터 목록"""
    r = requests.get(f"{CONNECT_URL}/connectors")
    connectors = r.json()
    print(f"\n등록된 Connector 목록: {connectors}")
    return connectors


if __name__ == "__main__":
    print("=" * 60)
    print("Step 2-1: Debezium CDC Connector 설정")
    print("=" * 60)

    if not wait_for_connect():
        exit(1)

    # 사용 가능한 플러그인 확인
    r = requests.get(f"{CONNECT_URL}/connector-plugins")
    plugins = [p["class"].split(".")[-1] for p in r.json()]
    print(f"사용 가능한 플러그인: {plugins}\n")

    create_debezium_source()
    list_connectors()

    print("\n" + "=" * 60)
    print("다음 단계:")
    print("  1. Control Center에서 Connect 탭 확인: http://localhost:9021")
    print("  2. 02_trigger_cdc_events.py 실행하여 DB 변경 → Kafka 캡처 확인")
    print("=" * 60)
