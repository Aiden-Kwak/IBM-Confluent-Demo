"""
Step 2-3: CDC 이벤트 Consumer
===============================
Debezium이 캡처한 CDC 이벤트를 소비하며,
INSERT/UPDATE/DELETE 이벤트의 구조를 분석합니다.

Debezium CDC 이벤트 구조:
  {
    "before": { ... },  ← 변경 전 레코드 (INSERT 시 null)
    "after": { ... },   ← 변경 후 레코드 (DELETE 시 null)
    "op": "c/u/d/r",    ← c:create, u:update, d:delete, r:read(snapshot)
    "source": { ... }   ← 소스 DB 메타데이터
  }
"""

import json
import signal
import sys

sys.path.insert(0, sys.path[0] + "/..")
from config import BOOTSTRAP_SERVERS, INTERCEPTOR_CONFIG

from confluent_kafka import Consumer, KafkaError

config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "cdc-monitor",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
    **INTERCEPTOR_CONFIG,
}

consumer = Consumer(config)

# CDC 토픽 구독 (Debezium이 만든 토픽)
CDC_TOPICS = ["cdc.public.orders", "cdc.public.products"]
consumer.subscribe(CDC_TOPICS)

OP_LABELS = {
    "c": "INSERT",
    "u": "UPDATE",
    "d": "DELETE",
    "r": "SNAPSHOT",  # 초기 스냅샷
}

print("=" * 60)
print("CDC 이벤트 모니터")
print(f"토픽: {CDC_TOPICS}")
print("=" * 60)
print("이벤트 대기 중... (Ctrl+C로 종료)\n")


def shutdown(sig, frame):
    consumer.close()
    sys.exit(0)


signal.signal(signal.SIGINT, shutdown)

count = 0
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"  [ERROR] {msg.error()}")
            continue

        value = json.loads(msg.value().decode("utf-8"))
        count += 1

        op = value.get("op", "?")
        op_label = OP_LABELS.get(op, op)
        table = msg.topic().split(".")[-1]

        print(f"  [{count:03d}] ═══ {op_label} on {table} ═══")
        print(f"  Topic: {msg.topic()} | Partition: {msg.partition()} | Offset: {msg.offset()}")

        if op in ("r", "c"):
            # INSERT 또는 스냅샷: after 데이터만 존재
            after = value.get("after", {})
            print(f"  [NEW] {json.dumps(after, ensure_ascii=False, indent=4)}")

        elif op == "u":
            # UPDATE: before와 after 비교
            before = value.get("before", {})
            after = value.get("after", {})
            print(f"  [BEFORE] {json.dumps(before, ensure_ascii=False)}")
            print(f"  [AFTER]  {json.dumps(after, ensure_ascii=False)}")
            # 변경된 필드 하이라이트
            if before and after:
                changes = {
                    k: {"from": before.get(k), "to": v}
                    for k, v in after.items()
                    if before.get(k) != v
                }
                if changes:
                    print(f"  [DIFF]   {json.dumps(changes, ensure_ascii=False)}")

        elif op == "d":
            # DELETE: before 데이터만 존재
            before = value.get("before", {})
            print(f"  [DELETED] {json.dumps(before, ensure_ascii=False)}")

        print()

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    print(f"\n총 {count}건 CDC 이벤트 처리")
