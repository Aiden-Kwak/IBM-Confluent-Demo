"""
Step 1-2: Kafka Consumer 기초
================================
Consumer는 Kafka 토픽에서 메시지를 읽는 역할입니다.

핵심 개념:
- Consumer Group: 같은 그룹의 컨슈머끼리 파티션을 나눠서 처리 (병렬 처리)
- Offset: 각 파티션에서 현재 읽은 위치. 컨슈머가 어디까지 읽었는지 추적
- auto.offset.reset: 처음 시작할 때 어디서부터 읽을지
  - "earliest": 처음부터 (놓친 메시지 없음)
  - "latest": 지금부터 (과거 메시지 무시)
"""

import json
import signal
import sys

sys.path.insert(0, sys.path[0] + "/..")
from config import BOOTSTRAP_SERVERS, INTERCEPTOR_CONFIG

from confluent_kafka import Consumer, KafkaError

# ──────────────────────────────────────────────
# 1. Consumer 설정
# ──────────────────────────────────────────────
config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    # Consumer Group ID - 같은 그룹은 파티션을 나눠서 소비
    "group.id": "order-processing-group",
    # 처음 시작할 때 가장 오래된 메시지부터 읽기
    "auto.offset.reset": "earliest",
    # 자동 커밋 비활성화 → 수동 커밋으로 정확한 처리 보장
    "enable.auto.commit": False,
    **INTERCEPTOR_CONFIG,
}

consumer = Consumer(config)

TOPIC = "shop.orders"

# ──────────────────────────────────────────────
# 2. 토픽 구독
# ──────────────────────────────────────────────
consumer.subscribe([TOPIC])

print("=" * 60)
print(f"Kafka Consumer 시작 (Group: {config['group.id']})")
print(f"토픽: {TOPIC}")
print("=" * 60)
print("메시지 대기 중... (Ctrl+C로 종료)\n")


# ──────────────────────────────────────────────
# 3. 메시지 소비 루프
# ──────────────────────────────────────────────
def shutdown(sig, frame):
    print("\n\nConsumer 종료 중...")
    consumer.close()
    sys.exit(0)


signal.signal(signal.SIGINT, shutdown)

message_count = 0

try:
    while True:
        # 1초 대기하면서 메시지 폴링
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"  [INFO] 파티션 {msg.partition()} 끝에 도달")
            else:
                print(f"  [ERROR] {msg.error()}")
            continue

        # 메시지 파싱
        key = msg.key().decode("utf-8") if msg.key() else None
        value = json.loads(msg.value().decode("utf-8"))
        message_count += 1

        print(f"  [{message_count}] ────────────────────────────")
        print(f"  Topic     : {msg.topic()}")
        print(f"  Partition : {msg.partition()}")
        print(f"  Offset    : {msg.offset()}")
        print(f"  Key       : {key}")
        print(f"  Value     : {json.dumps(value, ensure_ascii=False, indent=2)}")

        # ──────────────────────────────────────
        # 비즈니스 로직 (여기서 주문 처리)
        # ──────────────────────────────────────
        print(f"  → 주문 처리: {value.get('customer')}님의 {value.get('product')} 주문")

        # 수동 커밋 → "이 메시지까지 처리 완료"를 Kafka에 알림
        consumer.commit(asynchronous=False)
        print(f"  → Offset 커밋 완료\n")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    print(f"\n총 {message_count}건 처리 완료")
