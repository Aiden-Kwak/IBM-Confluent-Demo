"""
Step 4-4: QRadar 모의 테스트 (텍스트 전용 외부 시스템)
=====================================================
Schema Registry 연동 없이 순수 Kafka Consumer로 토픽을 읽습니다.
QRadar처럼 텍스트(JSON)만 파싱할 수 있는 외부 시스템을 시뮬레이션합니다.

테스트 항목:
  1. shop.orders.avro → 바이너리라 JSON 파싱 실패
  2. shop.orders.json → 텍스트라 JSON 파싱 성공
"""

import json
import signal
import sys

sys.path.insert(0, sys.path[0] + "/..")
from config import BOOTSTRAP_SERVERS

from confluent_kafka import Consumer, KafkaError

# Schema Registry 연동 없이 순수 Consumer (QRadar와 동일한 조건)
CONSUMER_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "mock-qradar-siem",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
    # Schema Registry 설정 없음 = QRadar와 동일 조건
}


def test_topic(topic_name, expected_success):
    """토픽에서 메시지를 읽고 JSON 파싱을 시도"""
    consumer = Consumer(CONSUMER_CONFIG)
    consumer.subscribe([topic_name])

    success = 0
    fail = 0
    samples = []

    for _ in range(100):  # 10초 대기
        msg = consumer.poll(timeout=0.1)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"    [ERROR] {msg.error()}")
            continue

        raw = msg.value()

        # QRadar가 하는 것: 바이트를 텍스트로 디코딩 → JSON 파싱
        try:
            text = raw.decode("utf-8")
            parsed = json.loads(text)
            success += 1
            if len(samples) < 2:
                samples.append(("OK", parsed))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            fail += 1
            if len(samples) < 2:
                samples.append(("FAIL", str(e), raw[:60]))

        if success + fail >= 5:
            break

    consumer.close()
    return success, fail, samples


def shutdown(sig, frame):
    sys.exit(0)


signal.signal(signal.SIGINT, shutdown)


print("=" * 70)
print("  QRadar 모의 테스트 - 텍스트 전용 외부 시스템 시뮬레이션")
print("  (Schema Registry 연동 없음, 순수 Kafka Consumer)")
print("=" * 70)

# ──────────────────────────────────────────────
# 테스트 1: Avro 토픽 읽기
# ──────────────────────────────────────────────
print("\n" + "─" * 70)
print("  TEST 1: shop.orders.avro (Avro 바이너리 토픽)")
print("─" * 70)

s, f, samples = test_topic("shop.orders.avro", expected_success=False)

if f > 0:
    print(f"  결과: FAIL - JSON 파싱 실패 {f}건")
    print(f"  → QRadar가 이 토픽을 구독하면 데이터를 읽을 수 없습니다.")
    for item in samples:
        if item[0] == "FAIL":
            print(f"\n  [파싱 에러] {item[1]}")
            print(f"  [Raw 바이트] {item[2]}")
elif s > 0:
    print(f"  결과: 파싱 성공 {s}건 (예상과 다름)")
    for item in samples:
        if item[0] == "OK":
            print(f"  [데이터] {item[1]}")
else:
    print("  (데이터 없음 - 01_avro_producer.py를 먼저 실행하세요)")

# ──────────────────────────────────────────────
# 테스트 2: JSON 토픽 읽기
# ──────────────────────────────────────────────
print("\n" + "─" * 70)
print("  TEST 2: shop.orders.json (JSON 텍스트 토픽)")
print("─" * 70)

s, f, samples = test_topic("shop.orders.json", expected_success=True)

if s > 0:
    print(f"  결과: OK - JSON 파싱 성공 {s}건")
    print(f"  → QRadar가 이 토픽을 구독하면 정상적으로 데이터를 수집합니다.")
    for item in samples:
        if item[0] == "OK":
            print(f"\n  [수집된 이벤트]")
            print(f"  {json.dumps(item[1], ensure_ascii=False, indent=4)}")
elif f > 0:
    print(f"  결과: FAIL - JSON 파싱 실패 {f}건 (예상과 다름)")
else:
    print("  (데이터 없음 - 02_avro_to_json_ksqldb.py를 먼저 실행하세요)")

# ──────────────────────────────────────────────
# 최종 결론
# ──────────────────────────────────────────────
print("\n" + "=" * 70)
print("  결론")
print("=" * 70)
print("""
  Confluent 고객이 Avro로 데이터를 운영하는 환경에서
  QRadar 같은 텍스트 전용 외부 시스템과 연동하려면:

  1. ksqlDB에서 Avro → JSON 변환 Stream을 생성 (SQL 한 줄)
  2. QRadar는 변환된 JSON 토픽(shop.orders.json)을 구독

  기존 Avro 파이프라인에 영향 없이 연동 가능합니다.

  QRadar 설정:
    Bootstrap Server List: <confluent-broker>:9092
    Consumer Group:        qradar-siem-group
    Topic List:            shop.orders.json
""")
print("=" * 70)
