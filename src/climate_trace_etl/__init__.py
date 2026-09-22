"""Climate TRACE -> MotherDuck emissions ELT pipeline.

The package is split along the classic ELT boundaries:

``config``          configuration and settings singleton.
``logging_config``  structured logging bootstrap.
``client``          resilient Climate TRACE API v7 HTTP client.
``transformer``     JSON -> flat pandas DataFrames.
``loader``          DuckDB / MotherDuck loading and data marts.
``main``            CLI entrypoint tying everything together.
"""

from climate_trace_etl.config import (
    PROJECT_ROOT,
    ConfigurationError,
    Settings,
    get_settings,
)

__all__ = [
    "PROJECT_ROOT",
    "ConfigurationError",
    "Settings",
    "__version__",
    "get_settings",
]

#: Keep in sync with ``[project].version`` in ``pyproject.toml``.
__version__ = "0.1.0"
