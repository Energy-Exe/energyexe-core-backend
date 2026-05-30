"""Opportunity-detection schema package.

Houses the per-windfarm ``DetectionContext`` (prefetch + memoize), the
``DetectorResult`` return dataclass, the ``SCHEMA_NAMES`` human-name map, and
(in later issues) the registry/orchestrator and one module per detector.
"""

from app.services.opportunity_schemas.context import DetectionContext, DetectorResult
from app.services.opportunity_schemas.registry import (
    SCHEMA_DEPENDENCIES,
    SCHEMA_REGISTRY,
    SCHEMA_STATUS,
    run_for_windfarm,
)
from app.services.opportunity_schemas.schema_names import SCHEMA_NAMES

__all__ = [
    "DetectionContext",
    "DetectorResult",
    "SCHEMA_NAMES",
    "SCHEMA_REGISTRY",
    "SCHEMA_DEPENDENCIES",
    "SCHEMA_STATUS",
    "run_for_windfarm",
]
