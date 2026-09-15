"""Чтение оригинального JSON-конверта Debezium, включая tombstone."""

import json
import time
from uuid import uuid4

from confluent_kafka import Consumer, KafkaException, TopicPartition

from kafka_practice.common import BOOTSTRAP, TOPICS


def new_consumer():
    return Consumer({
        "bootstrap.servers": BOOTSTRAP,
        # Compose публикует порт только на IPv4 loopback.
        "broker.address.family": "v4",
        "group.id": f"practice-view-{uuid4().hex}",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "allow.auto.create.topics": False,
    })


def assign_topics(consumer, *, from_end=False):
    """Явное назначение исключает гонку между rebalance и тестовым INSERT."""
    metadata = consumer.list_topics(timeout=10)
    partitions = []
    for topic in TOPICS:
        info = metadata.topics.get(topic)
        if info is None or info.error:
            raise RuntimeError(f"Топик {topic} пока не готов; выполните make up")
        for partition in sorted(info.partitions):
            tp = TopicPartition(topic, partition)
            low, high = consumer.get_watermark_offsets(tp, timeout=10)
            partitions.append(TopicPartition(topic, partition, high if from_end else low))
    consumer.assign(partitions)
    return partitions


def read_event(consumer, timeout=1.0):
    message = consumer.poll(timeout)
    if message is None:
        return None
    if message.error():
        raise KafkaException(message.error())
    return {
        "topic": message.topic(),
        "partition": message.partition(),
        "offset": message.offset(),
        "key": json.loads(message.key()) if message.key() is not None else None,
        "value": json.loads(message.value()) if message.value() is not None else None,
    }


def consume(limit=0, timeout=0):
    consumer = new_consumer()
    count = 0
    deadline = time.monotonic() + timeout if timeout else float("inf")
    try:
        assign_topics(consumer)
        while time.monotonic() < deadline and (not limit or count < limit):
            event = read_event(consumer)
            if event is not None:
                print(json.dumps(event, ensure_ascii=False), flush=True)
                count += 1
        if limit and count < limit:
            raise TimeoutError(f"Получено {count} из {limit} ожидаемых записей")
    finally:
        consumer.close()
