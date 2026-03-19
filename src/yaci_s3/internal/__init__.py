"""Internal jobs (profile builders, maintenance tasks)."""

from typing import Dict, Type

INTERNAL_JOBS: Dict[str, Type] = {}


def register(name: str):
    """Decorator to register an internal job class."""
    def decorator(cls):
        INTERNAL_JOBS[name] = cls
        return cls
    return decorator


# Import submodules to trigger registration
from . import drep_profile  # noqa: F401, E402
from . import pool_profile  # noqa: F401, E402
