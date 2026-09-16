"""Shared register reads preserve vendor metadata without reading private terms."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from app.services.scada_opportunity_service import ScadaOpportunityService


@pytest.mark.parametrize("detail", [False, True])
@pytest.mark.parametrize(
    "metadata",
    [
        {"price_basis": None, "offtake_regime": None},
        {"price_basis": "SPOT", "offtake_regime": "2016-2026 UNKNOWN"},
    ],
)
async def test_register_queries_return_analysis_metadata(detail, metadata):
    row = {"farm": "hill_of_towie", "id": 0, "gbp_year": 378623.0, **metadata}
    result = Mock()
    result.fetchone.return_value = SimpleNamespace(_mapping=row)
    result.fetchall.return_value = [SimpleNamespace(_mapping=row)]
    result.scalar.return_value = 1
    db = SimpleNamespace(execute=AsyncMock(return_value=result))
    service = ScadaOpportunityService(db)
    service.summary = AsyncMock(return_value=None)

    if detail:
        actual = await service.get("hill_of_towie", 0)
    else:
        actual = (await service.list_opportunities("hill_of_towie"))["items"][0]

    assert actual == row
    query = str(db.execute.call_args_list[0].args[0])
    assert "r.price_basis" in query and "r.offtake_regime" in query
    assert "scada.opportunity_register" in query
    assert "scada_ppa" not in query and "contract_price" not in query
