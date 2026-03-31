"""
Step 1-1: Kafka Producer 기초
================================
Producer는 Kafka 토픽에 메시지를 보내는 역할입니다.

핵심 개념:
- Topic: 메시지가 저장되는 카테고리 (DB의 테이블과 유사)
- Partition: 토픽을 분할한 단위. 병렬 처리의 핵심
- Key: 같은 키를 가진 메시지는 항상 같은 파티션에 저장 → 순서 보장
- Acks: 메시지 전송 신뢰성 레벨 (0, 1, all)
"""

import json
import sys
import time

sys.path.insert(0, sys.path[0] + "/..")
from config import BOOTSTRAP_SERVERS, INTERCEPTOR_CONFIG

from confluent_kafka import Producer

# ──────────────────────────────────────────────
# 1. Producer 설정
# ──────────────────────────────────────────────
config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    # acks=all → 모든 replica에 저장된 후 확인 (가장 안전)
    "acks": "all",
    # 메시지가 실패하면 최대 3번 재시도
    "retries": 3,
    **INTERCEPTOR_CONFIG,
}

producer = Producer(config)

TOPIC = "shop.orders"


# ──────────────────────────────────────────────
# 2. 전송 콜백 (성공/실패 확인)
# ──────────────────────────────────────────────
def delivery_callback(err, msg):
    if err:
        print(f"[FAIL] 전송 실패: {err}")
    else:
        print(
            f"[OK] Topic={msg.topic()} "
            f"Partition={msg.partition()} "
            f"Offset={msg.offset()} "
            f"Key={msg.key().decode('utf-8') if msg.key() else None}"
        )


# ──────────────────────────────────────────────
# 3. 주문 이벤트 생성 & 전송
# ──────────────────────────────────────────────
orders = [
    {"order_id": 1001, "customer": "김철수", "product": "MacBook Pro", "amount": 2499.99},
    {"order_id": 1002, "customer": "이영희", "product": "AirPods Pro", "amount": 249.99},
    {"order_id": 1003, "customer": "박민수", "product": "iPhone 15", "amount": 1199.99},
    {"order_id": 1004, "customer": "김철수", "product": "Magic Mouse", "amount": 99.99},
    {"order_id": 1005, "customer": "정지은", "product": "Galaxy S24", "amount": 999.99},
    {"order_id": 1006, "customer": "이영희", "product": "AirPods Max", "amount": 549.99},
]

print("=" * 60)
print("Kafka Producer - 주문 이벤트 전송")
print("=" * 60)

for order in orders:
    # Key = customer → 같은 고객의 주문은 같은 파티션으로 → 순서 보장
    key = order["customer"]
    value = json.dumps(order, ensure_ascii=False)

    producer.produce(
        topic=TOPIC,
        key=key,
        value=value,
        callback=delivery_callback,
    )

    # produce()는 비동기. poll()로 콜백 처리
    producer.poll(0)
    time.sleep(0.5)  # 천천히 보내서 관찰하기

# 모든 메시지가 전송 완료될 때까지 대기
producer.flush()

print("\n모든 주문 이벤트 전송 완료!")
print(f"총 {len(orders)}건 전송")
print("\n[TIP] '김철수'의 주문 2건은 같은 파티션에 저장됩니다 (Key 기반 파티셔닝)")
print("[TIP] Control Center에서 확인: http://localhost:9021")
