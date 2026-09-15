"""Команды учебного стенда: uv run kafka-practice --help."""

import json

import httpx
import psycopg
import typer
from confluent_kafka import KafkaException

from kafka_practice.common import CONNECT, CONNECTOR, ROOT, connector_status, wait_running
from kafka_practice.consumer import consume as consume_events
from kafka_practice.smoke import run_smoke

app = typer.Typer(no_args_is_help=True, help="PostgreSQL CDC: Debezium → Kafka.")


@app.command()
def register():
    """Создать/обновить коннектор и дождаться его задачи."""
    config = json.loads((ROOT / "connectors/postgres.json").read_text())
    response = httpx.put(
        f"{CONNECT}/connectors/{CONNECTOR}/config", json=config, timeout=15,
    )
    response.raise_for_status()
    wait_running()
    typer.echo(f"{CONNECTOR}: connector=RUNNING, task 0=RUNNING")


@app.command()
def status():
    """Вывести статус; при неработающей задаче вернуть ошибку."""
    result = connector_status()
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))
    if (
        result["connector"]["state"] != "RUNNING"
        or len(result.get("tasks", [])) != 1
        or result["tasks"][0]["state"] != "RUNNING"
    ):
        raise typer.Exit(1)


@app.command()
def consume(
    limit: int = typer.Option(0, min=0, help="Число записей; 0 — без ограничения."),
    timeout: int = typer.Option(0, min=0, help="Общий таймаут, секунды; 0 — без ограничения."),
):
    """Читать обе таблицы с начала доступной истории; Ctrl+C — выход."""
    consume_events(limit=limit, timeout=timeout)


@app.command()
def smoke(
    timeout: int = typer.Option(120, min=10, help="Таймаут отдельного этапа, секунды."),
    snapshot: bool = typer.Option(False, help="Также проверить 9 исходных snapshot-записей."),
):
    """Проверить реальные CDC-события, Prometheus и Grafana."""
    run_smoke(timeout=timeout, snapshot=snapshot)


def main():
    try:
        app()
    except (httpx.HTTPError, psycopg.Error, KafkaException, RuntimeError, TimeoutError, ValueError) as exc:
        typer.echo(f"Ошибка: {exc}", err=True)
        raise SystemExit(1) from None
