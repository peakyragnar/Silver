from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any

import pytest

from silver.features import MOMENTUM_12_1_DEFINITION
from silver.features.repository import (
    FeatureStoreError,
    FeatureStoreRepository,
    FeatureValueWrite,
    feature_definition_hash,
)


def test_ensure_feature_definition_inserts_stable_hash_and_sql_shape() -> None:
    connection = FakeFeatureConnection()
    repository = FeatureStoreRepository(connection)

    record = repository.ensure_feature_definition(MOMENTUM_12_1_DEFINITION)

    assert record.id == 501
    assert record.name == "momentum_12_1"
    assert record.version == 1
    assert record.definition_hash == feature_definition_hash(MOMENTUM_12_1_DEFINITION)

    sql, params = connection.executed[0]
    assert sql.startswith("INSERT INTO silver.feature_definitions")
    assert "definition_hash" in sql
    assert "ON CONFLICT (name, version) DO NOTHING" in sql
    assert params["name"] == "momentum_12_1"
    assert params["version"] == 1
    assert json.loads(params["computation_spec"]) == dict(
        MOMENTUM_12_1_DEFINITION.computation_spec
    )


def test_ensure_feature_definition_rejects_existing_version_hash_mismatch() -> None:
    existing = _definition_row(definition_hash="0" * 64)
    connection = FakeFeatureConnection(definitions={("momentum_12_1", 1): existing})
    repository = FeatureStoreRepository(connection)

    with pytest.raises(FeatureStoreError, match="different definition_hash"):
        repository.ensure_feature_definition(MOMENTUM_12_1_DEFINITION)


def test_write_feature_values_uses_idempotent_upsert_shape() -> None:
    connection = FakeFeatureConnection()
    repository = FeatureStoreRepository(connection)

    result = repository.write_feature_values(
        [
            FeatureValueWrite(
                security_id=101,
                asof_date=date(2024, 1, 2),
                feature_definition_id=501,
                value=0.25,
                available_at=datetime(2024, 1, 2, 23, tzinfo=timezone.utc),
                available_at_policy_id=3,
                computed_by_run_id=77,
                source_metadata={"window": {"start_date": "2023-01-03"}},
            )
        ]
    )

    assert result.rows_written == 1
    assert len(connection.feature_values) == 1
    sql, params = connection.executed[-1]
    assert sql.startswith("INSERT INTO silver.feature_values")
    assert "ON CONFLICT (security_id, asof_date, feature_definition_id)" in sql
    assert "IS DISTINCT FROM EXCLUDED.value" in sql
    assert "computed_by_run_id" in sql
    assert json.loads(params["source_metadata"]) == {
        "window": {"start_date": "2023-01-03"}
    }

    repository.write_feature_values(
        [
            FeatureValueWrite(
                security_id=101,
                asof_date=date(2024, 1, 2),
                feature_definition_id=501,
                value=0.25,
                available_at=datetime(2024, 1, 2, 23, tzinfo=timezone.utc),
                available_at_policy_id=3,
                computed_by_run_id=77,
                source_metadata={"window": {"start_date": "2023-01-03"}},
            )
        ]
    )

    assert len(connection.feature_values) == 1


def test_load_adjusted_prices_reads_only_succeeded_normalization_runs() -> None:
    connection = FakeFeatureConnection()
    repository = FeatureStoreRepository(connection)

    rows = repository.load_adjusted_prices(
        security_ids=(101,),
        end_date=date(2024, 1, 2),
        available_at_policy_id=3,
    )

    assert len(rows) == 1
    security_id, price = rows[0]
    assert security_id == 101
    assert price.price_date == date(2024, 1, 2)
    sql, params = connection.executed[-1]
    assert params == {
        "security_ids": [101],
        "end_date": date(2024, 1, 2),
        "available_at_policy_id": 3,
    }
    assert "JOIN silver.analytics_runs AS run" in sql
    assert "run.id = prices.normalized_by_run_id" in sql
    assert "run.status = 'succeeded'" in sql


def _definition_row(*, definition_hash: str | None = None) -> dict[str, Any]:
    return {
        "id": 501,
        "name": "momentum_12_1",
        "version": 1,
        "kind": "numeric",
        "computation_spec": dict(MOMENTUM_12_1_DEFINITION.computation_spec),
        "definition_hash": definition_hash
        or feature_definition_hash(MOMENTUM_12_1_DEFINITION),
        "notes": None,
    }


class FakeFeatureConnection:
    def __init__(
        self,
        *,
        definitions: dict[tuple[str, int], dict[str, Any]] | None = None,
    ) -> None:
        self.definitions = definitions if definitions is not None else {}
        self.feature_values: dict[tuple[int, date, int], dict[str, Any]] = {}
        self.adjusted_prices = [
            {
                "security_id": 101,
                "date": date(2024, 1, 2),
                "adj_close": "184.68",
                "available_at": datetime(2024, 1, 2, 23, tzinfo=timezone.utc),
            }
        ]
        self.executed: list[tuple[str, dict[str, Any]]] = []

    def cursor(self) -> FakeFeatureCursor:
        return FakeFeatureCursor(self)


class FakeFeatureCursor:
    def __init__(self, connection: FakeFeatureConnection) -> None:
        self.connection = connection
        self._one: dict[str, Any] | None = None
        self._many: list[dict[str, Any]] = []

    def __enter__(self) -> FakeFeatureCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, sql: str, params: dict[str, Any]) -> None:
        self.connection.executed.append((sql, dict(params)))
        if sql.startswith("INSERT INTO silver.feature_definitions"):
            self._insert_definition(params)
            return
        if sql.startswith("SELECT id, name, version, kind, computation_spec"):
            key = (params["name"], params["version"])
            self._one = self.connection.definitions.get(key)
            return
        if sql.startswith("INSERT INTO silver.feature_values"):
            self._upsert_feature_value(params)
            return
        if sql.startswith("SELECT prices.security_id, prices.date"):
            self._many = list(self.connection.adjusted_prices)
            return
        raise AssertionError(f"unexpected SQL: {sql}")

    def fetchone(self) -> dict[str, Any] | None:
        return self._one

    def fetchall(self) -> list[dict[str, Any]]:
        return self._many

    def _insert_definition(self, params: dict[str, Any]) -> None:
        key = (params["name"], params["version"])
        if key in self.connection.definitions:
            self._one = None
            return
        row = {
            "id": 501,
            "name": params["name"],
            "version": params["version"],
            "kind": params["kind"],
            "computation_spec": json.loads(params["computation_spec"]),
            "definition_hash": params["definition_hash"],
            "notes": params["notes"],
        }
        self.connection.definitions[key] = row
        self._one = row

    def _upsert_feature_value(self, params: dict[str, Any]) -> None:
        key = (
            params["security_id"],
            params["asof_date"],
            params["feature_definition_id"],
        )
        stored = dict(params)
        stored["source_metadata"] = json.loads(params["source_metadata"])
        self.connection.feature_values[key] = stored
