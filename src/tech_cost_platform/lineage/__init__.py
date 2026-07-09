"""Public lineage surface."""

from .build import LINEAGE_TABLE, LineageBuildResult, build_lineage_outputs, main
from .trace import (
    LineageRoundTripResult,
    LineageValidationError,
    MAX_PROPORTION_DRIFT_EUR,
    build_worked_example_payload,
    trace_backward,
    trace_forward,
    validate_lineage_per_line,
    validate_lineage_round_trip,
    validate_proportion_consistency,
)

__all__ = [
    "LINEAGE_TABLE",
    "LineageBuildResult",
    "LineageRoundTripResult",
    "LineageValidationError",
    "MAX_PROPORTION_DRIFT_EUR",
    "build_lineage_outputs",
    "build_worked_example_payload",
    "main",
    "trace_backward",
    "trace_forward",
    "validate_lineage_per_line",
    "validate_lineage_round_trip",
    "validate_proportion_consistency",
]
