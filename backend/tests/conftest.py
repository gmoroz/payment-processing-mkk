"""Подключение фикстур."""

pytest_plugins = [
    "tests.fixtures.database",
    "tests.fixtures.messaging",
]
