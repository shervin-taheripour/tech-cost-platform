"""Output-shape models for the synthetic source exports."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict


class SynthConfig(BaseModel):
    """Runtime config for the synthetic source generator."""

    model_config = ConfigDict(frozen=True)

    seed: int
    period: str
    output_dir: str


class CSVRowModel(BaseModel):
    """Shared base model for deterministic CSV rows."""

    model_config = ConfigDict(frozen=True)


class GLCost(CSVRowModel):
    gl_line_id: str
    period: str
    gl_account: str
    cost_center_id: str
    amount_eur: Decimal
    description: str


class CostCenter(CSVRowModel):
    cost_center_id: str
    cost_center_name: str
    tower_id: str | None


class ResourceTower(CSVRowModel):
    tower_id: str
    tower_name: str
    tower_type: str


class Application(CSVRowModel):
    app_id: str
    app_name: str
    business_criticality: str


class BusinessUnit(CSVRowModel):
    bu_id: str
    bu_name: str


class UsageMetric(CSVRowModel):
    metric_id: str
    period: str
    step: str
    from_id: str
    to_id: str
    metric_name: str
    value: Decimal


GL_COST_COLUMNS = list(GLCost.model_fields)
COST_CENTER_COLUMNS = list(CostCenter.model_fields)
RESOURCE_TOWER_COLUMNS = list(ResourceTower.model_fields)
APPLICATION_COLUMNS = list(Application.model_fields)
BUSINESS_UNIT_COLUMNS = list(BusinessUnit.model_fields)
USAGE_METRIC_COLUMNS = list(UsageMetric.model_fields)
