"""
Step 2-2: CDC 이벤트 트리거
============================
PostgreSQL에 직접 INSERT/UPDATE/DELETE를 수행하고,
Debezium이 이를 Kafka 토픽에 캡처하는지 확인합니다.

이 스크립트를 실행한 후 02_consumer.py로 CDC 토픽을 구독하면
DB 변경사항이 실시간으로 Kafka에 도착하는 것을 볼 수 있습니다.
"""

import json
import sys
import time

sys.path.insert(0, sys.path[0] + "/..")
from config import POSTGRES_HOST

import psycopg2

# ──────────────────────────────────────────────
# PostgreSQL 연결
# ──────────────────────────────────────────────
conn = psycopg2.connect(
    host=POSTGRES_HOST,
    port=5432,
    dbname="shop",
    user="confluent",
    password="confluent",
)
conn.autocommit = True
cur = conn.cursor()

print("=" * 60)
print("Step 2-2: CDC 이벤트 트리거")
print("PostgreSQL에 변경을 가하면 Debezium이 Kafka로 캡처합니다")
print("=" * 60)

# ──────────────────────────────────────────────
# 1. INSERT - 새 주문 추가
# ──────────────────────────────────────────────
print("\n[1] INSERT - 새 주문 추가")
new_orders = [
    ("홍길동", "MacBook Pro 14\"", 1, 2499.99, "PENDING"),
    ("성춘향", "AirPods Pro", 3, 749.97, "CONFIRMED"),
    ("이몽룡", "Galaxy S24", 2, 1999.98, "PENDING"),
]
for name, product, qty, price, status in new_orders:
    cur.execute(
        "INSERT INTO orders (customer_name, product, quantity, price, status) "
        "VALUES (%s, %s, %s, %s, %s) RETURNING id",
        (name, product, qty, price, status),
    )
    order_id = cur.fetchone()[0]
    print(f"  → 주문 #{order_id}: {name} - {product} (₩{price:,.0f})")
    time.sleep(1)  # CDC 캡처 관찰을 위해 천천히

# ──────────────────────────────────────────────
# 2. UPDATE - 주문 상태 변경
# ──────────────────────────────────────────────
print("\n[2] UPDATE - 주문 상태 변경")
cur.execute(
    "UPDATE orders SET status = 'SHIPPED' WHERE customer_name = '홍길동' AND status = 'PENDING'"
)
print("  → 홍길동 주문: PENDING → SHIPPED")
time.sleep(1)

cur.execute(
    "UPDATE orders SET status = 'DELIVERED' WHERE customer_name = '성춘향'"
)
print("  → 성춘향 주문: CONFIRMED → DELIVERED")
time.sleep(1)

# ──────────────────────────────────────────────
# 3. DELETE - 주문 취소
# ──────────────────────────────────────────────
print("\n[3] DELETE - 주문 삭제 (취소)")
cur.execute("DELETE FROM orders WHERE customer_name = '이몽룡'")
print("  → 이몽룡 주문 삭제됨")
time.sleep(1)

# ──────────────────────────────────────────────
# 4. 상품 테이블 변경
# ──────────────────────────────────────────────
print("\n[4] UPDATE - 상품 가격 변경")
cur.execute("UPDATE products SET price = 2299.99 WHERE name = 'MacBook Pro 14\"'")
print("  → MacBook Pro 14\" 가격: 2499.99 → 2299.99 (할인!)")

cur.execute("UPDATE products SET stock = stock - 5 WHERE name = 'AirPods Pro'")
print("  → AirPods Pro 재고 5개 감소")

cur.close()
conn.close()

print("\n" + "=" * 60)
print("DB 변경 완료! Kafka 토픽에서 CDC 이벤트를 확인하세요:")
print("  토픽: cdc.public.orders (주문 변경)")
print("  토픽: cdc.public.products (상품 변경)")
print()
print("확인 방법:")
print("  1. Control Center: http://localhost:9021 → Topics 탭")
print("  2. python 03_cdc_consumer.py 실행")
print("=" * 60)
