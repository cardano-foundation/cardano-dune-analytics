"""Hybrid exporters that join local data sources."""

from typing import Dict, Type

HYBRID_EXPORTERS: Dict[str, Type] = {}


def register(name: str):
    """Decorator to register a hybrid exporter class."""
    def decorator(cls):
        HYBRID_EXPORTERS[name] = cls
        return cls
    return decorator


# Import submodules to trigger registration
from . import drep_dist_enriched  # noqa: F401, E402
