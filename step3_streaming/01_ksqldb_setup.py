"""
Step 3-1: ksqlDB 스트림 프로세싱 설정
=======================================
ksqlDB를 사용해 SQL로 실시간 스트림 처리를 수행합니다.

ksqlDB란?
- Kafka 토픽 위에서 SQL로 실시간 데이터 처리
- Stream: 끊임없이 흐르는 이벤트 (INSERT only)
- Table: 최신 상태를 유지하는 뷰 (UPSERT)
- Window: 시간 기반 집계 (Tumbling, Hopping, Session)

이 스크립트는 ksqlDB REST API를 통해 스트림과 테이블을 생성합니다.
"""

import json
import sys
import time

sys.path.insert(0, sys.path[0] + "/..")
from config import KSQLDB_URL

import requests


def ksql_request(statement, stream_properties=None):
    """ksqlDB에 SQL 문 실행"""
    payload = {"ksql": statement}
    if stream_properties:
        payload["streamsProperties"] = stream_properties

    r = requests.post(
        f"{KSQLDB_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(payload),
    )
    return r.json()


def wait_for_ksqldb():
    print("ksqlDB 준비 대기 중...")
    for i in range(30):
        try:
            r = requests.get(f"{KSQLDB_URL}/info")
            if r.status_code == 200:
                info = r.json()
                print(f"  → ksqlDB 준비 완료! (version: {info['KsqlServerInfo']['version']})\n")
                return True
        except requests.ConnectionError:
            pass
        time.sleep(5)
        print(f"  대기 중... ({(i+1)*5}초)")
    return False


if __name__ == "__main__":
    print("=" * 60)
    print("Step 3-1: ksqlDB 스트림 프로세싱 설정")
    print("=" * 60)

    if not wait_for_ksqldb():
        print("ksqlDB 연결 실패")
        exit(1)

    # ──────────────────────────────────────────────
    # 1. 주문 STREAM 생성
    # ──────────────────────────────────────────────
    print("[1] 주문 Stream 생성")
    result = ksql_request("""
        CREATE STREAM IF NOT EXISTS orders_stream (
            order_id INT,
            customer VARCHAR,
            email VARCHAR,
            product VARCHAR,
            category VARCHAR,
            quantity INT,
            unit_price DOUBLE,
            total_amount DOUBLE,
            status VARCHAR,
            region VARCHAR,
            timestamp VARCHAR
        ) WITH (
            KAFKA_TOPIC = 'shop.orders',
            VALUE_FORMAT = 'JSON'
        );
    """)
    print(f"  → {result}\n")

    # ──────────────────────────────────────────────
    # 2. 카테고리별 실시간 매출 집계 (Tumbling Window)
    # ──────────────────────────────────────────────
    print("[2] 카테고리별 1분 단위 매출 집계 테이블 생성")
    result = ksql_request("""
        CREATE TABLE IF NOT EXISTS sales_by_category AS
        SELECT
            category,
            COUNT(*) AS order_count,
            SUM(total_amount) AS total_sales,
            AVG(total_amount) AS avg_order_value,
            WINDOWSTART AS window_start,
            WINDOWEND AS window_end
        FROM orders_stream
        WINDOW TUMBLING (SIZE 1 MINUTE)
        GROUP BY category
        EMIT CHANGES;
    """)
    print(f"  → {result}\n")

    # ──────────────────────────────────────────────
    # 3. 지역별 주문 현황 테이블
    # ──────────────────────────────────────────────
    print("[3] 지역별 주문 현황 테이블 생성")
    result = ksql_request("""
        CREATE TABLE IF NOT EXISTS orders_by_region AS
        SELECT
            region,
            COUNT(*) AS order_count,
            SUM(total_amount) AS total_sales
        FROM orders_stream
        GROUP BY region
        EMIT CHANGES;
    """)
    print(f"  → {result}\n")

    # ──────────────────────────────────────────────
    # 4. 고가 주문 필터링 Stream
    # ──────────────────────────────────────────────
    print("[4] 고가 주문 필터링 Stream 생성 (₩500,000 이상)")
    result = ksql_request("""
        CREATE STREAM IF NOT EXISTS high_value_orders AS
        SELECT
            order_id,
            customer,
            product,
            total_amount,
            region,
            timestamp
        FROM orders_stream
        WHERE total_amount >= 500
        EMIT CHANGES;
    """)
    print(f"  → {result}\n")

    # ──────────────────────────────────────────────
    # 5. 주문 상태별 분류 Stream
    # ──────────────────────────────────────────────
    print("[5] 취소 주문 알림 Stream 생성")
    result = ksql_request("""
        CREATE STREAM IF NOT EXISTS cancelled_orders AS
        SELECT
            order_id,
            customer,
            email,
            product,
            total_amount,
            timestamp
        FROM orders_stream
        WHERE status = 'CANCELLED'
        EMIT CHANGES;
    """)
    print(f"  → {result}\n")

    # ──────────────────────────────────────────────
    # 현재 스트림/테이블 목록 확인
    # ──────────────────────────────────────────────
    print("=" * 60)
    print("생성된 Streams:")
    streams = ksql_request("SHOW STREAMS;")
    for s in streams[0].get("streams", []):
        print(f"  - {s['name']} (topic: {s.get('topic', 'N/A')})")

    print("\n생성된 Tables:")
    tables = ksql_request("SHOW TABLES;")
    for t in tables[0].get("tables", []):
        print(f"  - {t['name']} (topic: {t.get('topic', 'N/A')})")

    print("\n" + "=" * 60)
    print("다음 단계:")
    print("  1. step1_basics/04_producer_advanced.py 실행 (이벤트 생성)")
    print("  2. python 02_query_ksqldb.py 실행 (실시간 집계 조회)")
    print("  3. ksqlDB CLI로 직접 쿼리:")
    print("     docker exec -it ksqldb-cli ksql http://ksqldb-server:8088")
    print("=" * 60)
