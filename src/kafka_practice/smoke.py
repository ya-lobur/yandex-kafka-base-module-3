"""Сквозная проверка живого стенда. Повторы доставки допустимы."""

import json
import time
from uuid import uuid4

import psycopg

from kafka_practice.common import (
    CONNECT, CONNECTOR, DSN, GRAFANA, TOPICS, connector_status,
    get_json, query, wait_for, wait_running,
)
from kafka_practice.consumer import assign_topics, new_consumer, read_event

WRITE_TOTAL = 'kafka_connect_source_record_write_total{connector="postgres-cdc"}'
STREAMING = 'debezium_connected{server="shop"}'


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def wait_events(consumer, expected, timeout):
    """expected: (topic, id, op) → ожидаемые части before/after."""
    pending = dict(expected)
    deadline = time.monotonic() + timeout
    while pending and time.monotonic() < deadline:
        event = read_event(consumer)
        if event is None:
            continue
        key, value = event["key"], event["value"]
        if not isinstance(key, dict):
            continue
        op = value["op"] if value is not None else "tombstone"
        identity = (event["topic"], key.get("id"), op)
        if identity not in pending:
            continue
        fields = pending[identity]
        if value is not None:
            require(value["source"]["table"] == event["topic"].rsplit(".", 1)[1], "Неверная source.table")
            for side, expected_fields in fields.items():
                actual = value[side]
                if expected_fields is None:
                    require(actual is None, f"{identity}: {side} должен быть null")
                else:
                    require(
                        isinstance(actual, dict) and all(actual.get(k) == v for k, v in expected_fields.items()),
                        f"{identity}: неверный {side}: {actual}",
                    )
        del pending[identity]
        print(f"  OK {event['topic']} id={key['id']} op={op}", flush=True)
    require(not pending, f"Не получены события: {list(pending)}")


def check_snapshot(timeout):
    consumer = new_consumer()
    try:
        assign_topics(consumer)
        users = [
            ("John Doe", "john@example.com"), ("Jane Smith", "jane@example.com"),
            ("Alice Johnson", "alice@example.com"), ("Bob Brown", "bob@example.com"),
        ]
        orders = [(1, "Product A", 2), (1, "Product B", 1), (2, "Product C", 5),
                  (3, "Product D", 3), (4, "Product E", 4)]
        expected = {
            (TOPICS[0], i, "r"): {"before": None, "after": {"id": i, "name": name, "email": email}}
            for i, (name, email) in enumerate(users, 1)
        }
        expected.update({
            (TOPICS[1], i, "r"): {"before": None, "after": {
                "id": i, "user_id": user_id, "product_name": product, "quantity": quantity,
            }}
            for i, (user_id, product, quantity) in enumerate(orders, 1)
        })
        wait_events(consumer, expected, timeout)
    finally:
        consumer.close()


def run_smoke(timeout=120, snapshot=False):
    stage = "готовность"
    consumer = None
    user_id = order_id = None
    try:
        wait_running(timeout)
        wait_for("streaming Debezium", lambda: query(STREAMING) == [1.0], timeout)
        config = get_json(f"{CONNECT}/connectors/{CONNECTOR}/config")
        require(
            set(config["table.include.list"].split(",")) == {r"public\.users", r"public\.orders"},
            "Коннектор должен захватывать только users и orders",
        )
        with psycopg.connect(DSN, autocommit=True) as db:
            tables = db.execute(
                "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = %s",
                ("practice5_publication",),
            ).fetchall()
            require(set(tables) == {("public", "users"), ("public", "orders")}, f"Неверная publication: {tables}")
            if snapshot:
                stage = "начальный snapshot"
                check_snapshot(timeout)

            stage = "подготовка CDC"
            baseline = wait_for("счётчик записи", lambda: query(WRITE_TOTAL), timeout)[0]
            consumer = new_consumer()
            assign_topics(consumer, from_end=True)
            marker = f"smoke-{uuid4().hex}"
            email = f"{marker}@example.test"

            stage = "INSERT"
            with db.transaction():
                user_id = db.execute(
                    "INSERT INTO users (name, email) VALUES (%s, %s) RETURNING id", (marker, email),
                ).fetchone()[0]
                order_id = db.execute(
                    "INSERT INTO orders (user_id, product_name, quantity) VALUES (%s, %s, 2) RETURNING id",
                    (user_id, marker),
                ).fetchone()[0]
            user_before = {"id": user_id, "name": marker, "email": email}
            order_before = {"id": order_id, "user_id": user_id, "product_name": marker, "quantity": 2}
            wait_events(consumer, {
                (TOPICS[0], user_id, "c"): {"before": None, "after": user_before},
                (TOPICS[1], order_id, "c"): {"before": None, "after": order_before},
            }, timeout)

            stage = "UPDATE"
            updated_name = marker + "-updated"
            with db.transaction():
                db.execute("UPDATE users SET name = %s WHERE id = %s", (updated_name, user_id))
                db.execute("UPDATE orders SET quantity = 3 WHERE id = %s", (order_id,))
            user_after = {**user_before, "name": updated_name}
            order_after = {**order_before, "quantity": 3}
            wait_events(consumer, {
                (TOPICS[0], user_id, "u"): {"before": user_before, "after": user_after},
                (TOPICS[1], order_id, "u"): {"before": order_before, "after": order_after},
            }, timeout)

            stage = "DELETE и tombstone"
            with db.transaction():
                db.execute("DELETE FROM orders WHERE id = %s", (order_id,))
                db.execute("DELETE FROM users WHERE id = %s", (user_id,))
            wait_events(consumer, {
                (TOPICS[0], user_id, "d"): {"before": user_after, "after": None},
                (TOPICS[1], order_id, "d"): {"before": order_after, "after": None},
                (TOPICS[0], user_id, "tombstone"): {},
                (TOPICS[1], order_id, "tombstone"): {},
            }, timeout)

            stage = "Prometheus"
            checks = {
                'up{job="kafka-connect"}': lambda v: v == [1.0],
                STREAMING: lambda v: v == [1.0],
                'kafka_connect_connector_running_task_count{connector="postgres-cdc"}': lambda v: v == [1.0],
                'kafka_connect_connector_failed_task_count{connector="postgres-cdc"}': lambda v: v == [0.0],
                WRITE_TOTAL: lambda v: len(v) == 1 and v[0] >= baseline + 8,
                'kafka_connect_source_record_poll_total{connector="postgres-cdc"}': lambda v: len(v) == 1,
                'kafka_connect_source_record_active_count{connector="postgres-cdc"}': lambda v: len(v) == 1,
                'debezium_milliseconds_behind_source{server="shop"}': lambda v: len(v) == 1,
            }
            for expression, check in checks.items():
                wait_for(expression, lambda e=expression, c=check: c(query(e)), timeout)

            stage = "Grafana"
            dashboard = get_json(f"{GRAFANA}/api/dashboards/uid/postgres-cdc", auth=("admin", "admin"))
            require(len(dashboard["dashboard"]["panels"]) == 8, "Dashboard должен содержать 8 панелей")
            datasource = get_json(f"{GRAFANA}/api/datasources/uid/prometheus/health", auth=("admin", "admin"))
            require(datasource["status"] == "OK", f"Datasource недоступен: {datasource}")
        print("PASS: CDC обеих таблиц, Prometheus и Grafana", flush=True)
    except Exception:
        print(f"FAIL: этап «{stage}»", flush=True)
        try:
            print(json.dumps(connector_status(), ensure_ascii=False, indent=2), flush=True)
        except Exception as diagnostic_error:
            print(f"Статус Connect недоступен: {diagnostic_error}", flush=True)
        raise
    finally:
        if consumer is not None:
            consumer.close()
        # Даже при ошибке удаляем только строки этого прогона, в порядке FK.
        if user_id is not None:
            try:
                with psycopg.connect(DSN) as cleanup:
                    if order_id is not None:
                        cleanup.execute("DELETE FROM orders WHERE id = %s", (order_id,))
                    cleanup.execute("DELETE FROM users WHERE id = %s", (user_id,))
            except psycopg.Error as cleanup_error:
                print(f"Не удалось удалить тестовые строки: {cleanup_error}", flush=True)
