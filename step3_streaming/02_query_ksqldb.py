"""
Step 3-2: ksqlDB 실시간 쿼리
==============================
ksqlDB에서 생성한 스트림과 테이블을 조회합니다.

Pull Query vs Push Query:
- Pull Query: 현재 상태 조회 (SELECT ... WHERE ...) - 1회 응답
- Push Query: 실시간 구독 (SELECT ... EMIT CHANGES) - 무한 스트리밍

이 스크립트는 Pull Query로 현재 상태를 주기적으로 조회합니다.
"""

import json
import sys
import time

sys.path.insert(0, sys.path[0] + "/..")
from config import KSQLDB_URL

import requests


def ksql_query(query):
    """ksqlDB Pull Query 실행"""
    r = requests.post(
        f"{KSQLDB_URL}/query",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps({"ksql": query, "streamsProperties": {}}),
    )
    # ksqlDB는 응답을 줄 단위 JSON으로 보냄 (JSON Lines 형식)
    results = []
    for line in r.text.strip().split("\n"):
        line = line.strip().rstrip(",")
        if line and line not in ("[", "]"):
            try:
                results.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return results


def print_table(title, rows, columns):
    """간단한 테이블 출력"""
    print(f"\n  {'─' * 50}")
    print(f"  {title}")
    print(f"  {'─' * 50}")

    if not rows or len(rows) <= 1:
        print("  (데이터 없음 - 먼저 04_producer_advanced.py를 실행하세요)")
        return

    # 첫 번째 항목은 헤더
    for row in rows[1:]:
        if isinstance(row, dict) and "finalMessage" in row:
            continue
        if isinstance(row, list):
            line = " | ".join(str(v) for v in row)
            print(f"    {line}")
        elif isinstance(row, dict):
            line = " | ".join(f"{k}={v}" for k, v in row.items() if k != "header")
            print(f"    {line}")


if __name__ == "__main__":
    print("=" * 60)
    print("Step 3-2: ksqlDB 실시간 집계 대시보드")
    print("5초마다 갱신됩니다 (Ctrl+C로 종료)")
    print("=" * 60)

    try:
        while True:
            print(f"\n{'═' * 60}")
            print(f"  📊 실시간 대시보드 (갱신: {time.strftime('%H:%M:%S')})")
            print(f"{'═' * 60}")

            # 지역별 주문 현황
            try:
                result = ksql_query("SELECT * FROM ORDERS_BY_REGION;")
                print_table(
                    "지역별 주문 현황",
                    result,
                    ["region", "order_count", "total_sales"],
                )
            except Exception as e:
                print(f"  지역별 조회 오류: {e}")

            # 고가 주문 최근 5건
            try:
                result = ksql_query(
                    "SELECT order_id, customer, product, total_amount "
                    "FROM HIGH_VALUE_ORDERS LIMIT 5;"
                )
                print_table(
                    "고가 주문 (최근 5건, ₩500 이상)",
                    result,
                    ["order_id", "customer", "product", "total_amount"],
                )
            except Exception as e:
                print(f"  고가 주문 조회 오류: {e}")

            time.sleep(5)

    except KeyboardInterrupt:
        print("\n\n대시보드 종료")
