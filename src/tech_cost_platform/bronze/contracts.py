"""Pydantic ingestion-boundary contracts for bronze source tables."""

from __future__ import annotations

from decimal import Decimal
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
PeriodStr = Annotated[str, StringConstraints(pattern=r"^\d{4}-\d{2}$")]
GLAccountStr = Annotated[str, StringConstraints(pattern=r"^\d{4}$")]
OptionalNonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class BronzeContract(BaseModel):
    """Shared base model for bronze ingestion validation."""

    model_config = ConfigDict(frozen=True, extra="forbid")


class GLCostContract(BronzeContract):
    gl_line_id: NonEmptyStr
    period: PeriodStr
    gl_account: GLAccountStr
    cost_center_id: NonEmptyStr
    amount_eur: Decimal = Field(gt=Decimal("0.00"))
    description: NonEmptyStr


class CostCenterContract(BronzeContract):
    cost_center_id: NonEmptyStr
    cost_center_name: NonEmptyStr
    tower_id: OptionalNonEmptyStr | None = None


class ResourceTowerContract(BronzeContract):
    tower_id: NonEmptyStr
    tower_name: NonEmptyStr
    tower_type: NonEmptyStr


class ApplicationContract(BronzeContract):
    app_id: NonEmptyStr
    app_name: NonEmptyStr
    business_criticality: Literal["low", "med", "high"]


class BusinessUnitContract(BronzeContract):
    bu_id: NonEmptyStr
    bu_name: NonEmptyStr


class UsageMetricContract(BronzeContract):
    metric_id: NonEmptyStr
    period: PeriodStr
    step: Literal["tower_to_app", "app_to_bu"]
    from_id: NonEmptyStr
    to_id: NonEmptyStr
    metric_name: NonEmptyStr
    value: Decimal = Field(ge=Decimal("0.00"))
