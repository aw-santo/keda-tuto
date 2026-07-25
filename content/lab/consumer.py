#!/usr/bin/env python3
"""KEDA lab consumer: joins a group, logs partition assignment, sleeps per message."""
import logging
import os
import signal
import socket
import sys
import time

from confluent_kafka import Consumer, KafkaError, KafkaException

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "orders")
GROUP = os.environ.get("KAFKA_GROUP", "lab-consumers")
DELAY_MS = int(os.environ.get("PROCESS_DELAY_MS", "250"))
POD = os.environ.get("POD_NAME", socket.gethostname())

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [{POD}] %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("consumer")

running = True


def _shutdown(signum, _frame):
    global running
    log.warning("signal %s received -> draining and leaving group", signal.Signals(signum).name)
    running = False


signal.signal(signal.SIGTERM, _shutdown)
signal.signal(signal.SIGINT, _shutdown)


def fmt(parts):
    return sorted(p.partition for p in parts)


# With `cooperative-sticky` the callbacks receive the INCREMENTAL delta, not the
# full set, so we track the running total ourselves. This is what makes the
# rebalance legible in `kubectl logs`.
owned = set()


def safe_commit(consumer, where):
    """Sync-commit stored offsets, tolerating 'nothing stored' which is normal."""
    try:
        consumer.commit(asynchronous=False)
    except KafkaException as exc:
        if exc.args[0].code() != KafkaError._NO_OFFSET:
            log.warning("commit failed at %s: %s", where, exc)


def on_assign(consumer, partitions):
    owned.update(p.partition for p in partitions)
    log.info("REBALANCE +added=%s -> owns %d partitions %s",
             fmt(partitions), len(owned), sorted(owned))


def on_revoke(consumer, partitions):
    owned.difference_update(p.partition for p in partitions)
    log.info("REBALANCE -revoked=%s -> owns %d partitions %s",
             fmt(partitions), len(owned), sorted(owned))
    safe_commit(consumer, "revoke")


def on_lost(consumer, partitions):
    owned.difference_update(p.partition for p in partitions)
    log.warning("REBALANCE !lost=%s -> owns %d partitions %s",
                fmt(partitions), len(owned), sorted(owned))


conf = {
    "bootstrap.servers": BOOTSTRAP,
    "group.id": GROUP,
    "client.id": POD,
    "auto.offset.reset": "earliest",
    # Commit often so KEDA sees lag fall in near-real-time, but only for
    # messages we actually finished (store_offsets after processing).
    "enable.auto.commit": True,
    "auto.commit.interval.ms": 1000,
    "enable.auto.offset.store": False,
    "partition.assignment.strategy": "cooperative-sticky",
    "session.timeout.ms": 10000,
    "heartbeat.interval.ms": 3000,
    "max.poll.interval.ms": 300000,
    "fetch.min.bytes": 1,
}

consumer = Consumer(conf)
consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke, on_lost=on_lost)
log.info("started bootstrap=%s topic=%s group=%s delay=%dms", BOOTSTRAP, TOPIC, GROUP, DELAY_MS)

processed = 0
last_report = time.monotonic()
try:
    while running:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            log.error("consume error: %s", msg.error())
            continue

        time.sleep(DELAY_MS / 1000.0)
        processed += 1
        consumer.store_offsets(msg)

        if processed % 20 == 0 or time.monotonic() - last_report > 10:
            log.info(
                "processed=%d last=(p%d o%d) assignment=%s",
                processed, msg.partition(), msg.offset(), sorted(owned),
            )
            last_report = time.monotonic()
finally:
    log.info("committing final offsets (processed=%d)", processed)
    safe_commit(consumer, "shutdown")
    consumer.close()
    log.info("closed cleanly")
