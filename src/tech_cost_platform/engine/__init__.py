"""Public surface for the allocation engine core."""

from .cascade import AllocationResult, AllocationValidationError, run_allocation

__all__ = ["AllocationResult", "AllocationValidationError", "run_allocation"]
