"""Schemas for the SCADA portal endpoints.

Only the farms meta endpoint is typed; chart endpoints return shaped dicts that
mirror the validated dashboard prototype's data snapshot
(energyexe-scada-pipeline/docs/ui/dashdata.js), matching the analytics-endpoint
precedent in this repo (no response_model on generation/price analytics).
"""

from typing import List, Optional

from pydantic import BaseModel, ConfigDict


class ScadaFarm(BaseModel):
    """One row of scada.dim_farm plus derived meta; extra dim columns pass through."""

    model_config = ConfigDict(extra="allow")

    farm: str
    windfarm_id: Optional[int] = None
    n_turbines: int = 0
    data_through: Optional[str] = None
    days: Optional[int] = None


class ScadaFarmsResponse(BaseModel):
    farms: List[ScadaFarm]
