"""EPR-123/122 — the ratios response says whose accounts the figures are.

``FinancialRatiosResponse`` carries the windfarm's relationship to its
financial entity (``relationship_type``) and the names of the platform-visible
windfarms that entity's accounts cover (``linked_windfarms``), so the client can
render "Reported by <entity>" and, for consolidated filers, "also covers: …".

DB-free: a queue-based fake session replays, in order, the executes
``calculate_financial_ratios`` issues for an entity with no filings:

  1. windfarm_financial_entities links for the windfarm   → .scalars().all()
  2. the windfarm itself                                  → .scalar_one_or_none()
  3. the entity                                           → .scalar_one_or_none()
  4. all windfarm_ids linked to the entity                → .all()  (1-tuples)
  5. (id, name, is_deleted, cod) for those windfarms       → .all()
  6. the entity's financial_data rows                     → .scalars().all()
"""

from datetime import date
from types import SimpleNamespace

import pytest

from app.schemas.financial_data import LinkedWindfarmRef
from app.services.financial_data_service import FinancialDataService


class _Result:
    def __init__(self, rows=None, scalar=None):
        self._rows = list(rows or [])
        self._scalar = scalar

    def scalars(self):
        return self

    def all(self):
        return list(self._rows)

    def scalar_one_or_none(self):
        return self._scalar


class _QueueSession:
    """Hands back the scripted results one execute at a time."""

    def __init__(self, results):
        self._results = list(results)
        self.calls = 0

    async def execute(self, *a, **k):
        self.calls += 1
        if not self._results:
            raise AssertionError("calculate_financial_ratios issued an unexpected query")
        return self._results.pop(0)


def _script(*, link, windfarm, entity, all_link_ids, linked_rows):
    return [
        _Result(rows=[link]),
        _Result(scalar=windfarm),
        _Result(scalar=entity),
        _Result(rows=[(wf_id,) for wf_id in all_link_ids]),
        _Result(rows=linked_rows),
        _Result(rows=[]),  # no financial_data rows → no per-period generation queries
    ]


@pytest.mark.asyncio
async def test_consolidated_entity_names_visible_linked_windfarms_only():
    session = _QueueSession(
        _script(
            link=SimpleNamespace(financial_entity_id=31, relationship_type="consolidated"),
            windfarm=SimpleNamespace(id=7187, name="Hennøy"),
            entity=SimpleNamespace(id=31, name="Renantis Norway AS", entity_type="holdco"),
            all_link_ids=[7204, 7187, 9999],
            linked_rows=[
                (7204, "Okla", False, date(2021, 1, 1)),
                (7187, "Hennøy", False, date(2019, 6, 1)),
                (9999, "Hidden farm", True, date(2023, 1, 1)),  # is_deleted
            ],
        )
    )

    responses = await FinancialDataService(session).calculate_financial_ratios(7187)

    assert len(responses) == 1
    resp = responses[0]
    assert resp.financial_entity_name == "Renantis Norway AS"
    assert resp.relationship_type == "consolidated"
    # Denominator set is untouched: every link, deleted farms included.
    assert resp.linked_windfarm_ids == [7204, 7187, 9999]
    # Names: only platform-visible farms, sorted, the requested farm included.
    assert resp.linked_windfarms == [
        LinkedWindfarmRef(id=7187, name="Hennøy"),
        LinkedWindfarmRef(id=7204, name="Okla"),
    ]
    # Effective COD semantics unchanged (max over all linked farms).
    assert resp.cod == date(2023, 1, 1)
    assert resp.periods == []
    # The names ride along on the existing COD query — no extra round-trip.
    assert session.calls == 6


@pytest.mark.asyncio
async def test_primary_asset_entity_lists_only_itself():
    session = _QueueSession(
        _script(
            link=SimpleNamespace(financial_entity_id=34, relationship_type="primary_asset"),
            windfarm=SimpleNamespace(id=7213, name="Smøla"),
            entity=SimpleNamespace(id=34, name="Smøla Vind 2 AS", entity_type="spv"),
            all_link_ids=[7213],
            linked_rows=[(7213, "Smøla", False, date(2005, 9, 1))],
        )
    )

    (resp,) = await FinancialDataService(session).calculate_financial_ratios(7213)

    assert resp.relationship_type == "primary_asset"
    assert resp.linked_windfarms == [LinkedWindfarmRef(id=7213, name="Smøla")]
    assert resp.cod == date(2005, 9, 1)


@pytest.mark.asyncio
async def test_windfarm_without_entity_returns_empty():
    session = _QueueSession([_Result(rows=[])])

    assert await FinancialDataService(session).calculate_financial_ratios(1) == []
    assert session.calls == 1
