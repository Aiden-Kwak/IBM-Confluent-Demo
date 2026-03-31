"""
Step 1-4: 연속 이벤트 생성기 (고급 Producer)
================================
Faker를 사용해 실시간으로 주문 이벤트를 계속 생성합니다.
Step 2, 3 실습에서 실시간 데이터 소스로 활용합니다.

실행: python 04_producer_advanced.py
"""

import json
import random
import sys
import time
from datetime import datetime

sys.path.insert(0, sys.path[0] + "/..")
from config import BOOTSTRAP_SERVERS, INTERCEPTOR_CONFIG

from confluent_kafka import Producer
from faker import Faker

fake = Faker("ko_KR")

config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "acks": "all",
    "linger.ms": 10,  # 10ms 동안 메시지를 모아서 배치 전송 (처리량 향상)
    "batch.size": 16384,  # 배치 크기 16KB
    **INTERCEPTOR_CONFIG,
}

producer = Producer(config)

PRODUCTS = [
    {"name": "MacBook Pro 14\"", "category": "electronics", "price": 2499.99},
    {"name": "iPhone 15 Pro", "category": "electronics", "price": 1199.99},
    {"name": "AirPods Pro", "category": "electronics", "price": 249.99},
    {"name": "Galaxy S24", "category": "electronics", "price": 999.99},
    {"name": "Sony WH-1000XM5", "category": "electronics", "price": 349.99},
    {"name": "Nike Air Max", "category": "shoes", "price": 179.99},
    {"name": "Adidas Ultraboost", "category": "shoes", "price": 189.99},
    {"name": "Levi's 501", "category": "clothing", "price": 89.99},
]

STATUSES = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]


def generate_order():
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)
    return {
        "order_id": random.randint(10000, 99999),
        "customer": fake.name(),
        "email": fake.email(),
        "product": product["name"],
        "category": product["category"],
        "quantity": quantity,
        "unit_price": product["price"],
        "total_amount": round(product["price"] * quantity, 2),
        "status": random.choice(STATUSES),
        "region": random.choice(["서울", "부산", "대구", "인천", "광주", "대전", "울산", "제주"]),
        "timestamp": datetime.now().isoformat(),
    }


def delivery_callback(err, msg):
    if err:
        print(f"  [FAIL] {err}")


print("=" * 60)
print("실시간 주문 이벤트 생성기")
print("2초마다 1~3건의 주문 이벤트를 생성합니다")
print("=" * 60)
print("Ctrl+C로 종료\n")

total = 0
try:
    while True:
        batch_size = random.randint(1, 3)
        for _ in range(batch_size):
            order = generate_order()
            producer.produce(
                topic="shop.orders",
                key=order["customer"],
                value=json.dumps(order, ensure_ascii=False),
                callback=delivery_callback,
            )
            total += 1
            print(
                f"  [{total:04d}] {order['customer']} | "
                f"{order['product']} x{order['quantity']} | "
                f"₩{order['total_amount']:,.0f} | "
                f"{order['status']} | {order['region']}"
            )

        producer.poll(0)
        time.sleep(2)

except KeyboardInterrupt:
    producer.flush()
    print(f"\n\n총 {total}건 이벤트 생성 완료")
