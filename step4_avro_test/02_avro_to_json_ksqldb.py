"""
Step 4-2: ksqlDB로 Avro → JSON 변환
=====================================
Avro 토픽을 읽어서 JSON 포맷의 새 토픽으로 변환합니다.
QRadar 같은 텍스트 전용 시스템은 이 JSON 토픽을 구독하면 됩니다.

ksqlDB SQL로 포맷 변환을 수행합니다.
"""

import json
import sys
import time

sys.path.insert(0, sys.path[0] + "/..")
from config import KSQLDB_URL

import requests


def ksql_request(statement):
    r = requests.post(
        f"{KSQLDB_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps({"ksql": statement}),
    )
    return r.json()


def wait_for_ksqldb():
    print("ksqlDB 준비 대기 중...")
    for i in range(30):
        try:
            r = requests.get(f"{KSQLDB_URL}/info")
            if r.status_code == 200:
                print("  → ksqlDB 준비 완료!\n")
                return True
        except requests.ConnectionError:
            pass
        time.sleep(5)
    return False


if __name__ == "__main__":
    print("=" * 60)
    print("Step 4-2: Avro → JSON 변환 (ksqlDB)")
    print("=" * 60)

    if not wait_for_ksqldb():
        print("ksqlDB 연결 실패")
        exit(1)

    # ──────────────────────────────────────────────
    # 1. Avro 토픽을 Stream으로 정의 (VALUE_FORMAT='AVRO')
    # ──────────────────────────────────────────────
    print("[1] Avro Stream 생성 (shop.orders.avro 토픽)")
    result = ksql_request("""
        CREATE STREAM IF NOT EXISTS orders_avro (
            order_id INT,
            customer VARCHAR,
            product VARCHAR,
            category VARCHAR,
            quantity INT,
            unit_price DOUBLE,
            total_amount DOUBLE,
            region VARCHAR,
            timestamp VARCHAR
        ) WITH (
            KAFKA_TOPIC = 'shop.orders.avro',
            VALUE_FORMAT = 'AVRO',
            PARTITIONS = 3
        );
    """)
    print(f"  → {result}\n")

    # ──────────────────────────────────────────────
    # 2. JSON 변환 Stream 생성 (핵심! VALUE_FORMAT='JSON')
    # ──────────────────────────────────────────────
    print("[2] JSON 변환 Stream 생성 (shop.orders.json 토픽)")
    print("    Avro → JSON 포맷 변환")
    print()
    result = ksql_request("""
        CREATE STREAM IF NOT EXISTS orders_json
        WITH (
            KAFKA_TOPIC = 'shop.orders.json',
            VALUE_FORMAT = 'JSON'
        ) AS
        SELECT
            order_id,
            customer,
            product,
            category,
            quantity,
            unit_price,
            total_amount,
            region,
            timestamp
        FROM orders_avro
        EMIT CHANGES;
    """)
    print(f"  → {result}\n")

    # ──────────────────────────────────────────────
    # 확인
    # ──────────────────────────────────────────────
    print("=" * 60)
    print("변환 파이프라인 구성 완료!")
    print()
    print("  Avro 토픽 (바이너리)     →  JSON 토픽 (텍스트)")
    print("  shop.orders.avro         →  shop.orders.json")
    print()
    print("QRadar는 shop.orders.json 토픽을 구독하면 됩니다.")
    print()
    print("검증 방법:")
    print("  1. 01_avro_producer.py를 실행하여 Avro 이벤트 생성")
    print("  2. 03_verify_json.py를 실행하여 JSON 변환 결과 확인")
    print("  3. Control Center → Topics → shop.orders.json 에서 확인")
    print("=" * 60)
