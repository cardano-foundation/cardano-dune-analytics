"""External data exporters that fetch from HTTP APIs."""

from typing import Dict, Type

EXTERNAL_EXPORTERS: Dict[str, Type] = {}


def register(name: str):
    """Decorator to register an external exporter class."""
    def decorator(cls):
        EXTERNAL_EXPORTERS[name] = cls
        return cls
    return decorator


# Import submodules to trigger registration
from . import asset_data  # noqa: F401, E402
from . import contract_registry  # noqa: F401, E402
from . import off_chain_pool_data  # noqa: F401, E402
