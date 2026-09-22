"""MotherDuck connectivity diagnostics for the configured token.

``poetry run check-motherduck`` (or ``python -m climate_trace_etl.diagnostics``) connects to
``md:`` in workspace mode and prints every database and share the current ``MOTHERDUCK_TOKEN``
can see. It is the quickest way to find out whether ``MOTHERDUCK_DATABASE`` points at a database
the token actually owns, which is exactly what MotherDuck reports as
``no database/share named '<name>' found``.

The module only reads metadata - it never writes to MotherDuck.
"""

from __future__ import annotations

import sys
from collections.abc import Callable

import duckdb
from loguru import logger

from climate_trace_etl.config import Settings, get_settings
from climate_trace_etl.loader import mask_dsn

#: Exit code of a successful check.
EXIT_OK = 0

#: Exit code used when the token is missing, unusable or cannot see ``MOTHERDUCK_DATABASE``.
EXIT_FAILURE = 1


def list_motherduck_databases(settings: Settings | None = None) -> list[str]:
    """Return every MotherDuck database and share visible to the configured token.

    The check runs over ``md:`` (workspace mode), which attaches the databases saved in the
    MotherDuck workspace, so the result reflects the whole account behind the token.

    Args:
        settings: Configuration to use; defaults to
            :func:`climate_trace_etl.config.get_settings`.

    Returns:
        The sorted database names, for example ``["emissions_db", "my_db"]``.

    Raises:
        ConfigurationError: if no MotherDuck token is configured.
        duckdb.Error: if the connection itself fails (bad token, no network, wrong region).
    """
    current = settings or get_settings()
    dsn = current.motherduck_workspace_dsn
    logger.debug("listing MotherDuck databases via {}", mask_dsn(dsn))

    with duckdb.connect(dsn) as connection:
        rows = connection.execute("show databases").fetchall()
    return sorted(str(row[0]) for row in rows)


def check(settings: Settings | None = None, *, echo: Callable[[str], None] = print) -> int:
    """Print the visible databases and report whether ``MOTHERDUCK_DATABASE`` is among them.

    Args:
        settings: Configuration to use; defaults to
            :func:`climate_trace_etl.config.get_settings`.
        echo: Sink for the report; defaults to :func:`print`, tests inject a list ``append``.

    Returns:
        :data:`EXIT_OK` when ``MOTHERDUCK_DATABASE`` is visible, :data:`EXIT_FAILURE` otherwise.
    """
    current = settings or get_settings()
    if not current.motherduck_configured:
        echo("MOTHERDUCK_TOKEN is not set: export it (or add it to .env) and run the check again.")
        return EXIT_FAILURE

    try:
        databases = list_motherduck_databases(current)
    except duckdb.Error as error:
        echo(f"could not connect to MotherDuck: {error}")
        return EXIT_FAILURE

    echo(f"MotherDuck databases visible to MOTHERDUCK_TOKEN ({len(databases)}):")
    for name in databases or ["(none)"]:
        echo(f"  - {name}")

    if current.motherduck_database in databases:
        echo(f"MOTHERDUCK_DATABASE={current.motherduck_database} is visible: nothing to do.")
        return EXIT_OK

    echo(
        f"MOTHERDUCK_DATABASE={current.motherduck_database} is NOT visible to this token: point "
        "MOTHERDUCK_DATABASE at one of the names above, create the database, or make sure the "
        "token belongs to the account that hosts it."
    )
    return EXIT_FAILURE


def main() -> int:
    """Entrypoint of ``poetry run check-motherduck``; returns the process exit code."""
    return check(get_settings())


if __name__ == "__main__":  # pragma: no cover - manual invocation
    sys.exit(main())
