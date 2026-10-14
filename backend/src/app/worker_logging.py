"""Общая настройка логирования worker-процессов."""

import logging


def configure_worker_logging() -> None:
    """Настроить единый формат логов worker-процесса."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        force=True,
    )
