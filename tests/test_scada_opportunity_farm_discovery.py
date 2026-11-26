"""D-093: the opportunity feature discovers its farms from the run table, so an asset with no register row (every line
declared not assessed, Lutelandet) is still served; the internal-farm filter is unchanged."""
import asyncio

from app.services.scada_opportunity_service import ScadaOpportunityService


class _Rows:
    def __init__(self, farms):
        self._farms = farms

    def fetchall(self):
        return [type("Row", (), {"farm": f})() for f in self._farms]


class _FakeDb:
    def __init__(self, farms):
        self.farms, self.sql, self.params = farms, None, None

    async def execute(self, statement, params=None):
        self.sql, self.params = str(statement), params
        return _Rows(self.farms)


def test_farms_come_from_the_run_table_not_the_register_rows():
    db = _FakeDb(["hill_of_towie", "lutelandet", "raggovidda"])
    farms = asyncio.run(ScadaOpportunityService(db, include_internal=True).farm_slugs())
    assert farms == ["hill_of_towie", "lutelandet", "raggovidda"]
    assert "FROM scada.opportunity_run r" in db.sql and "opportunity_register" not in db.sql
    assert "COALESCE(d.internal, false)" in db.sql and db.params == {"every": True}


def test_non_staff_still_get_the_internal_filter():
    db = _FakeDb([])
    asyncio.run(ScadaOpportunityService(db, include_internal=False).farm_slugs())
    assert db.params == {"every": False}
