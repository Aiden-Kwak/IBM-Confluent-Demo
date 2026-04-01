"""
Step 4-1: Avro Producer
========================
Schema Registry에 스키마를 등록하고, Avro 바이너리로 직렬화하여 Kafka에 전송합니다.
QRadar 같은 텍스트 전용 시스템은 이 토픽을 직접 읽을 수 없습니다.
"""

import json
import random
import sys
import time
from datetime import datetime

sys.path.insert(0, sys.path[0] + "/..")
from config import BOOTSTRAP_SERVERS, INTERCEPTOR_CONFIG

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Schema Registry 설정
SCHEMA_REGISTRY_URL = "http://localhost:8081"
if "29092" in BOOTSTRAP_SERVERS:
    SCHEMA_REGISTRY_URL = "http://schema-registry:8081"

schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

# Avro 스키마 정의
ORDER_SCHEMA = """{
    "type": "record",
    "name": "OrderEvent",
    "namespace": "com.shop.events",
    "doc": "쇼핑몰 주문 이벤트",
    "fields": [
        {"name": "order_id", "type": "int", "doc": "주문 ID"},
        {"name": "customer", "type": "string", "doc": "고객명"},
        {"name": "product", "type": "string", "doc": "상품명"},
        {"name": "category", "type": "string", "doc": "카테고리"},
        {"name": "quantity", "type": "int", "doc": "수량"},
        {"name": "unit_price", "type": "double", "doc": "단가"},
        {"name": "total_amount", "type": "double", "doc": "총액"},
        {"name": "region", "type": "string", "doc": "지역"},
        {"name": "timestamp", "type": "string", "doc": "주문 시각"}
    ]
}"""

avro_serializer = AvroSerializer(schema_registry_client, ORDER_SCHEMA)

producer_config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "value.serializer": avro_serializer,
    **INTERCEPTOR_CONFIG,
}

producer = SerializingProducer(producer_config)

TOPIC = "shop.orders.avro"

PRODUCTS = [
    {"name": "MacBook Pro 14\"", "category": "electronics", "price": 2499.99},
    {"name": "iPhone 15 Pro", "category": "electronics", "price": 1199.99},
    {"name": "AirPods Pro", "category": "electronics", "price": 249.99},
    {"name": "Galaxy S24", "category": "electronics", "price": 999.99},
    {"name": "Nike Air Max", "category": "shoes", "price": 179.99},
]

CUSTOMERS = ["김철수", "이영희", "박민수", "정지은", "홍길동"]
REGIONS = ["서울", "부산", "대구", "인천", "광주", "대전"]


def delivery_callback(err, msg):
    if err:
        print(f"  [FAIL] {err}")
    else:
        print(
            f"  [OK] Topic={msg.topic()} Partition={msg.partition()} "
            f"Offset={msg.offset()} (Avro 바이너리)"
        )


print("=" * 60)
print("Step 4-1: Avro Producer")
print("Schema Registry에 스키마 등록 후 Avro 바이너리로 전송")
print("=" * 60)
print(f"Schema Registry: {SCHEMA_REGISTRY_URL}")
print(f"Topic: {TOPIC}")
print()

total = 0
try:
    while True:
        product = random.choice(PRODUCTS)
        quantity = random.randint(1, 5)
        order = {
            "order_id": random.randint(10000, 99999),
            "customer": random.choice(CUSTOMERS),
            "product": product["name"],
            "category": product["category"],
            "quantity": quantity,
            "unit_price": product["price"],
            "total_amount": round(product["price"] * quantity, 2),
            "region": random.choice(REGIONS),
            "timestamp": datetime.now().isoformat(),
        }

        producer.produce(
            topic=TOPIC,
            value=order,
            on_delivery=delivery_callback,
        )
        producer.poll(0)
        total += 1

        print(
            f"  [{total:03d}] {order['customer']} | "
            f"{order['product']} x{order['quantity']} | "
            f"₩{order['total_amount']:,.0f} | {order['region']}"
        )

        time.sleep(2)

except KeyboardInterrupt:
    producer.flush()
    print(f"\n총 {total}건 Avro 이벤트 전송 완료")
