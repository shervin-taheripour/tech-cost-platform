"""Gold report view surface."""

from .build import GoldReportsResult, build_gold_reports, main
from .views import (
    REPORT_APP_TCO_TABLE,
    REPORT_BU_SHOWBACK_TABLE,
    REPORT_DRIVER_COMPARISON_BY_APP_TABLE,
    REPORT_DRIVER_COMPARISON_TABLE,
    REPORT_LINEAGE_TABLE,
    REPORT_RESIDUAL_TABLE,
)

__all__ = [
    "GoldReportsResult",
    "REPORT_APP_TCO_TABLE",
    "REPORT_BU_SHOWBACK_TABLE",
    "REPORT_DRIVER_COMPARISON_BY_APP_TABLE",
    "REPORT_DRIVER_COMPARISON_TABLE",
    "REPORT_LINEAGE_TABLE",
    "REPORT_RESIDUAL_TABLE",
    "build_gold_reports",
    "main",
]
