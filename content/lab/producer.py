#!/usr/bin/env python3
"""KEDA lab producer: burst N messages at a configurable rate."""
import json
import logging
import os
import sys
import time
import uuid

from confluent_kafka import Producer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "orders")
COUNT = int(os.environ.get("MESSAGE_COUNT", "5000"))
RATE = float(os.environ.get("MESSAGES_PER_SEC", "0"))  # 0 = as fast as possible
KEYS = int(os.environ.get("KEY_CARDINALITY", "1000"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
log = logging.getLogger("producer")

p = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "linger.ms": 20,
    "compression.type": "lz4",
    "acks": "1",
})

delivered = 0
failed = 0


def on_delivery(err, msg):
    global delivered, failed
    if err:
        failed += 1
        if failed < 5:
            log.error("delivery failed: %s", err)
    else:
        delivered += 1


log.info("producing count=%d rate=%s/s topic=%s bootstrap=%s", COUNT, RATE or "unlimited", TOPIC, BOOTSTRAP)
start = time.monotonic()
interval = 1.0 / RATE if RATE > 0 else 0.0

for i in range(COUNT):
    p.produce(
        TOPIC,
        key=f"key-{i % KEYS}".encode(),
        value=json.dumps({"id": str(uuid.uuid4()), "seq": i, "ts": time.time()}).encode(),
        on_delivery=on_delivery,
    )
    p.poll(0)
    if interval:
        target = start + (i + 1) * interval
        now = time.monotonic()
        if target > now:
            time.sleep(target - now)

p.flush(30)
elapsed = time.monotonic() - start
log.info("done delivered=%d failed=%d in %.1fs (%.0f msg/s)", delivered, failed, elapsed, delivered / max(elapsed, 1e-9))
