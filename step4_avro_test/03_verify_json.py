"""
Step 4-3: JSON 변환 결과 검증
==============================
Avro 토픽과 JSON 토픽을 동시에 소비하여 비교합니다.
- shop.orders.avro: 바이너리 (사람이 읽을 수 없음)
- shop.orders.json: 텍스트 (QRadar가 읽을 수 있음)
"""

import json
import signal
import sys
import time

sys.path.insert(0, sys.path[0] + "/..")
from config import BOOTSTRAP_SERVERS, INTERCEPTOR_CONFIG

from confluent_kafka import Consumer, KafkaError


def create_consumer(group_id):
    return Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        **INTERCEPTOR_CONFIG,
    })


def shutdown(sig, frame):
    sys.exit(0)


signal.signal(signal.SIGINT, shutdown)

print("=" * 60)
print("Step 4-3: Avro vs JSON 비교 검증")
print("=" * 60)

# ──────────────────────────────────────────────
# 1. Avro 토픽 읽기 (바이너리)
# ──────────────────────────────────────────────
print("\n[1] shop.orders.avro (Avro 바이너리) 샘플:")
print("-" * 50)

avro_consumer = create_consumer("verify-avro")
avro_consumer.subscribe(["shop.orders.avro"])

avro_count = 0
for _ in range(50):  # 5초 대기
    msg = avro_consumer.poll(timeout=0.1)
    if msg and not msg.error():
        raw = msg.value()
        avro_count += 1
        if avro_count <= 2:
            print(f"  [{avro_count}] 바이너리 데이터 (길이: {len(raw)} bytes)")
            print(f"      Raw: {raw[:80]}...")
            print(f"      → 사람이 읽을 수 없음. QRadar도 파싱 불가.")
            print()
        if avro_count >= 2:
            break

avro_consumer.close()

if avro_count == 0:
    print("  (데이터 없음 - 먼저 01_avro_producer.py를 실행하세요)")

# ──────────────────────────────────────────────
# 2. JSON 토픽 읽기 (텍스트)
# ──────────────────────────────────────────────
print("\n[2] shop.orders.json (JSON 텍스트) 샘플:")
print("-" * 50)

json_consumer = create_consumer("verify-json")
json_consumer.subscribe(["shop.orders.json"])

json_count = 0
for _ in range(50):
    msg = json_consumer.poll(timeout=0.1)
    if msg and not msg.error():
        raw = msg.value()
        json_count += 1
        if json_count <= 2:
            text = raw.decode("utf-8")
            parsed = json.loads(text)
            print(f"  [{json_count}] JSON 텍스트:")
            print(f"      {json.dumps(parsed, ensure_ascii=False, indent=6)}")
            print(f"      → 사람이 읽을 수 있음. QRadar 바로 수집 가능!")
            print()
        if json_count >= 2:
            break

json_consumer.close()

if json_count == 0:
    print("  (데이터 없음 - 02_avro_to_json_ksqldb.py를 먼저 실행하세요)")

# ──────────────────────────────────────────────
# 결론
# ──────────────────────────────────────────────
print()
print("=" * 60)
print("검증 결과")
print("=" * 60)
print(f"  Avro 토픽 (shop.orders.avro): {avro_count}건 확인 - 바이너리")
print(f"  JSON 토픽 (shop.orders.json): {json_count}건 확인 - 텍스트")
print()
if avro_count > 0 and json_count > 0:
    print("  ✓ Avro → JSON 변환 성공!")
    print("  ✓ QRadar는 shop.orders.json 토픽을 구독하면 됩니다.")
    print()
    print("  QRadar 설정 예시:")
    print("    Bootstrap Server List: <confluent-broker>:9092")
    print("    Consumer Group: qradar-siem-group")
    print("    Topic List: shop.orders.json")
print("=" * 60)
