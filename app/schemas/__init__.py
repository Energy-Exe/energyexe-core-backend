"""Pydantic schemas package."""

from .control_area import ControlArea, ControlAreaCreate, ControlAreaUpdate
from .country import Country, CountryCreate, CountryInDB, CountryUpdate
from .data_anomaly import (
    DataAnomalyBase,
    DataAnomalyCreate,
    DataAnomalyUpdate,
    DataAnomalyStatusUpdate,
    DataAnomalyResponse,
    DataAnomalyDetectionResult,
    AnomalyDetectionRequest,
    AnomalyDetectionResponse,
    ReaggregationRequest,
    ReaggregationResponse,
    AnomalyListFilters,
    AnomalyListResponse,
)
from .generation_unit import (
    GenerationUnitCreate,
    GenerationUnitResponse,
    GenerationUnitSearchParams,
    GenerationUnitUpdate,
)
from .market_balance_area import MarketBalanceArea, MarketBalanceAreaCreate, MarketBalanceAreaUpdate
from .state import State, StateCreate, StateInDB, StateUpdate, StateWithCountry
from .user import Token, TokenData, UserCreate, UserLogin, UserResponse, UserUpdate

__all__ = [
    "UserResponse",
    "UserCreate",
    "UserUpdate",
    "UserLogin",
    "Token",
    "TokenData",
    "Country",
    "CountryCreate",
    "CountryUpdate",
    "CountryInDB",
    "State",
    "StateCreate",
    "StateUpdate",
    "StateInDB",
    "StateWithCountry",
    "ControlArea",
    "ControlAreaCreate",
    "ControlAreaUpdate",
    "MarketBalanceArea",
    "MarketBalanceAreaCreate",
    "MarketBalanceAreaUpdate",
    "GenerationUnitCreate",
    "GenerationUnitUpdate",
    "GenerationUnitResponse",
    "GenerationUnitSearchParams",
    "DataAnomalyBase",
    "DataAnomalyCreate",
    "DataAnomalyUpdate",
    "DataAnomalyStatusUpdate",
    "DataAnomalyResponse",
    "DataAnomalyDetectionResult",
    "AnomalyDetectionRequest",
    "AnomalyDetectionResponse",
    "ReaggregationRequest",
    "ReaggregationResponse",
    "AnomalyListFilters",
    "AnomalyListResponse",
]

# Resolve forward references
ControlArea.model_rebuild()
MarketBalanceArea.model_rebuild()
