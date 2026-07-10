"""Public surface for the allocation engine core.

Keep this package import-light so callers can import
`tech_cost_platform.engine.strategies` without pulling in the DuckDB-backed
runtime adapter from `cascade.py`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .cascade import AllocationResult, AllocationValidationError, run_allocation

__all__ = ["AllocationResult", "AllocationValidationError", "run_allocation"]


def __getattr__(name: str):
    """Lazily expose cascade exports without eager runtime imports."""
    if name in __all__:
        from .cascade import AllocationResult, AllocationValidationError, run_allocation

        exports = {
            "AllocationResult": AllocationResult,
            "AllocationValidationError": AllocationValidationError,
            "run_allocation": run_allocation,
        }
        return exports[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
