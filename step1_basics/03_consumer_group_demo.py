"""
Step 1-3: Consumer Group 데모
================================
같은 Consumer Group의 컨슈머들이 파티션을 자동으로 분배하는 것을 확인합니다.

실험 방법:
  터미널 1: python 03_consumer_group_demo.py --id worker-1
  터미널 2: python 03_consumer_group_demo.py --id worker-2
  터미널 3: python 03_consumer_group_demo.py --id worker-3

  → 3개 파티션이 3개 워커에 1:1로 분배됩니다
  → 워커 하나를 종료하면? → 리밸런싱이 발생하여 남은 워커가 더 많은 파티션을 맡음

핵심 관찰 포인트:
  1. 각 워커가 담당하는 파티션 번호가 다름
  2. 워커 추가/제거 시 파티션 재분배 (리밸런싱)
  3. 같은 그룹 내에서는 각 메시지가 정확히 한 번만 처리됨
"""

import argparse
import json
import signal
import sys

sys.path.insert(0, sys.path[0] + "/..")
from config import BOOTSTRAP_SERVERS, INTERCEPTOR_CONFIG

from confluent_kafka import Consumer, KafkaError

parser = argparse.ArgumentParser()
parser.add_argument("--id", default="worker-1", help="워커 ID (예: worker-1)")
args = parser.parse_args()

WORKER_ID = args.id

config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "order-workers",  # 모든 워커가 같은 그룹
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
    "auto.commit.interval.ms": 5000,
    **INTERCEPTOR_CONFIG,
}

consumer = Consumer(config)


def on_assign(consumer, partitions):
    """파티션 할당 시 호출"""
    assigned = [p.partition for p in partitions]
    print(f"\n  ★ [{WORKER_ID}] 파티션 할당됨: {assigned}")
    print(f"    → 이 워커가 {len(assigned)}개 파티션을 담당합니다\n")


def on_revoke(consumer, partitions):
    """파티션 해제 시 호출 (리밸런싱)"""
    revoked = [p.partition for p in partitions]
    print(f"\n  ★ [{WORKER_ID}] 파티션 해제됨: {revoked} (리밸런싱 중...)\n")


consumer.subscribe(["shop.orders"], on_assign=on_assign, on_revoke=on_revoke)

print("=" * 60)
print(f"Consumer Group 데모 - {WORKER_ID}")
print(f"Group: order-workers")
print("=" * 60)
print("메시지 대기 중... (Ctrl+C로 종료)\n")


def shutdown(sig, frame):
    consumer.close()
    sys.exit(0)


signal.signal(signal.SIGINT, shutdown)

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
        print(
            f"  [{WORKER_ID}] P{msg.partition()} | "
            f"offset={msg.offset()} | "
            f"{value.get('customer')} → {value.get('product')}"
        )
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    print(f"\n[{WORKER_ID}] 종료됨")
