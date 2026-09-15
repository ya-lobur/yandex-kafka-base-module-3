"""Общие адреса стенда и ожидание готовности внешних сервисов."""

import json
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[2]
CONNECT = "http://localhost:8083"
PROMETHEUS = "http://localhost:9090"
GRAFANA = "http://localhost:3000"
BOOTSTRAP = "localhost:19092"
DSN = "host=localhost port=15432 dbname=shop user=postgres-user password=postgres-pw connect_timeout=5"
CONNECTOR = "postgres-cdc"
TOPICS = ("shop.public.users", "shop.public.orders")
HTTP_TIMEOUT = 5.0


def get_json(url, **kwargs):
    response = httpx.get(url, timeout=HTTP_TIMEOUT, **kwargs)
    response.raise_for_status()
    return response.json()


def wait_for(description, check, timeout=120):
    """Повторяем только временные сетевые ошибки; все ожидания ограничены."""
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except httpx.HTTPError as exc:
            last_error = str(exc)
        time.sleep(1)
    detail = f": {last_error}" if last_error else ""
    raise TimeoutError(f"Не дождались: {description} ({timeout} с){detail}")


def connector_status():
    return get_json(f"{CONNECT}/connectors/{CONNECTOR}/status")


def wait_running(timeout=120):
    def check():
        status = connector_status()
        states = [status["connector"], *status.get("tasks", [])]
        failed = [item for item in states if item["state"] == "FAILED"]
        if failed:
            raise RuntimeError(json.dumps(status, ensure_ascii=False, indent=2))
        return status if (
            status["connector"]["state"] == "RUNNING"
            and len(status.get("tasks", [])) == 1
            and status["tasks"][0]["state"] == "RUNNING"
        ) else None

    return wait_for("RUNNING коннектора и единственной задачи", check, timeout)


def query(expression):
    body = get_json(f"{PROMETHEUS}/api/v1/query", params={"query": expression})
    if body["status"] != "success":
        raise RuntimeError(f"Ошибка Prometheus: {body}")
    return [float(item["value"][1]) for item in body["data"]["result"]]
