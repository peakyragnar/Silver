from __future__ import annotations

from silver.reference import universe


def test_membership_query_casts_nullable_date_bounds_for_postgres() -> None:
    sql = universe._SELECT_MEMBERS_SQL

    assert "%(end_date)s::date IS NULL" in sql
    assert "membership.valid_from <= %(end_date)s::date" in sql
    assert "%(start_date)s::date IS NULL" in sql
    assert "membership.valid_to >= %(start_date)s::date" in sql
