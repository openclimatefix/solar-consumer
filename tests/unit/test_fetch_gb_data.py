"""
Unit Tests for GSP merge weights logic in fetch_gb_data.py

Tests cover:
1. Config loading: YAML parses to the correct dict structure
2. Config loading: returns empty dict when config file is missing
3. Split remapping: deprecated GSP reconstructed as sum of its parts
4. Merge remapping: reconstruction with fractional weight
5. Negative weight: rejected by Pydantic validation (weights must be >= 0)
6. No-config: direct fetch behaviour is unchanged
7. Deprecated IDs absent from pvlive.gsp_ids are not fetched
8. reconstruct_gsp_from_weights: single source with weight=1.0
9. reconstruct_gsp_from_weights: two sources summed per timestamp
10. reconstruct_gsp_from_weights: fractional weight halves generation
11. reconstruct_gsp_from_weights: negative weight rejected by Pydantic
12. reconstruct_gsp_from_weights: uses cache and skips API call
13. reconstruct_gsp_from_weights: newly fetched sources populate cache
14. reconstruct_gsp_from_weights: empty weights_config returns None
15. reconstruct_gsp_from_weights: updated_gmt taken from first source
16. reconstruct_gsp_from_weights: capacity columns summed across sources
17. target GSP ID present in registry is skipped from direct fetch and reconstructed
"""
import textwrap
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
from pydantic import ValidationError

from solar_consumer.data.fetch_gb_data import (
    GSPMergeConfig,
    GSPMergeSource,
    fetch_gb_data_historic,
    load_gsp_merge_weights,
    reconstruct_gsp_from_weights,
)

START = datetime(2025, 1, 14, 6, 0, tzinfo=UTC)
END   = datetime(2025, 1, 14, 8, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_gsp_df(generation_mw: float, n_rows: int = 3) -> pd.DataFrame:
    """Return a minimal PVLive-style DataFrame with n_rows of data."""
    datetimes = pd.date_range("2025-01-14 06:00", periods=n_rows, freq="30min", tz="UTC")
    return pd.DataFrame(
        {
            "datetime_gmt": datetimes,
            "generation_mw": [generation_mw] * n_rows,
            "installedcapacity_mwp": [100.0] * n_rows,
            "capacity_mwp": [90.0] * n_rows,
            "updated_gmt": [datetime(2025, 1, 14, 8, 0, tzinfo=UTC)] * n_rows,
        }
    )


def _mock_pvlive(gsp_ids: list, between_side_effect) -> MagicMock:
    """Return a configured mock PVLive instance."""
    mock = MagicMock()
    mock.gsp_ids = gsp_ids
    mock.between.side_effect = between_side_effect
    return mock


# ---------------------------------------------------------------------------
# 1. test_config_loads_correctly
# ---------------------------------------------------------------------------

def test_config_loads_correctly(tmp_path):
    """1. Config loading: YAML parses to expected dict structure with int keys and float weights."""
    yaml_content = textwrap.dedent("""\
        4:
          pvlive_merge_weights:
            - gsp_id: 324
              weight: 1.0
            - gsp_id: 325
              weight: 1.0
        139:
          pvlive_merge_weights:
            - gsp_id: 323
              weight: 1.0
            - gsp_id: 334
              weight: 1.0
    """)
    config_file = tmp_path / "gsp_merge_weights.yaml"
    config_file.write_text(yaml_content)

    result = load_gsp_merge_weights(str(config_file))

    assert isinstance(result, dict)
    assert set(result.keys()) == {4, 139}

    # Keys are ints, values are GSPMergeConfig with a list of GSPMergeSource entries.
    for k, config in result.items():
        assert isinstance(k, int)
        assert isinstance(config, GSPMergeConfig)
        for src in config.pvlive_merge_weights:
            assert isinstance(src, GSPMergeSource)
            assert isinstance(src.gsp_id, int)
            assert isinstance(src.weight, float)

    assert [(s.gsp_id, s.weight) for s in result[4].pvlive_merge_weights] == [
        (324, 1.0),
        (325, 1.0),
    ]
    assert [(s.gsp_id, s.weight) for s in result[139].pvlive_merge_weights] == [
        (323, 1.0),
        (334, 1.0),
    ]


# ---------------------------------------------------------------------------
# 2. test_config_loads_missing_file
# ---------------------------------------------------------------------------

def test_config_loads_missing_file(tmp_path):
    """2. Config loading: returns empty dict when config file does not exist."""
    result = load_gsp_merge_weights(str(tmp_path / "nonexistent.yaml"))
    assert result == {}


# ---------------------------------------------------------------------------
# 3. test_split_remapping
# ---------------------------------------------------------------------------

def test_split_remapping(tmp_path, monkeypatch):
    """
    3. Split remapping: GSP 4 is absent from pvlive.gsp_ids (it was split). It is defined in the
    merge config with sources 324 (7 MW) and 325 (8 MW).
    Reconstructed generation for GSP 4 should be 15 MW per slot.
    """
    yaml_content = textwrap.dedent("""\
        4:
          pvlive_merge_weights:
            - gsp_id: 324
              weight: 1.0
            - gsp_id: 325
              weight: 1.0
    """)
    (tmp_path / "gsp_merge_weights.yaml").write_text(yaml_content)

    source_324 = _make_gsp_df(generation_mw=7.0)
    source_325 = _make_gsp_df(generation_mw=8.0)

    def mock_between(**kwargs):
        eid = kwargs["entity_id"]
        if eid == 324:
            return source_324.copy()
        if eid == 325:
            return source_325.copy()
        return _make_gsp_df(0.0)

    # gsp_ids from PVLive does NOT include ID 4 (it no longer exists in the registry).
    mock_pvl = _mock_pvlive(gsp_ids=[0, 1, 2, 3], between_side_effect=mock_between)

    monkeypatch.delenv("UK_PVLIVE_N_GSPS", raising=False)
    with patch(
        "solar_consumer.data.fetch_gb_data.load_gsp_merge_weights",
        return_value=load_gsp_merge_weights(str(tmp_path / "gsp_merge_weights.yaml")),
    ), patch("solar_consumer.data.fetch_gb_data.PVLive", return_value=mock_pvl):
        from solar_consumer.data.fetch_gb_data import fetch_gb_data_historic
        df = fetch_gb_data_historic(regime="in-day")

    gsp4 = df[df["gsp_id"] == 4]
    assert not gsp4.empty, "Expected rows for remapped GSP ID 4"
    assert (gsp4["solar_generation_kw"] == 15_000.0).all(), (
        f"Expected 15000 kW, got {gsp4['solar_generation_kw'].unique()}"
    )


# ---------------------------------------------------------------------------
# 4. test_merge_remapping
# ---------------------------------------------------------------------------

def test_merge_remapping(tmp_path, monkeypatch):
    """
    4. Merge remapping: Tests fractional weight reconstruction: a hypothetical target GSP (ID 999,
    absent from pvlive.gsp_ids) is reconstructed with weight 0.5 from source
    GSP 351 (10 MW). Expected target generation = 5 MW per slot.

    Note: GSP 225 was previously used here but was confirmed to never exist in
    PVLive's registry and has been removed from gsp_merge_weights.yaml.
    This test uses a clearly fictional ID (999) to test the logic in isolation.
    """
    yaml_content = textwrap.dedent("""\
        999:
          pvlive_merge_weights:
            - gsp_id: 351
              weight: 0.5
    """)
    (tmp_path / "gsp_merge_weights.yaml").write_text(yaml_content)

    source_351 = _make_gsp_df(generation_mw=10.0)

    def mock_between(**kwargs):
        if kwargs["entity_id"] == 351:
            return source_351.copy()
        return _make_gsp_df(0.0)

    mock_pvl = _mock_pvlive(gsp_ids=[0, 1, 2], between_side_effect=mock_between)

    monkeypatch.delenv("UK_PVLIVE_N_GSPS", raising=False)
    # Set a high cap so fictional GSP ID 999 is not filtered out.
    # The default cap (342) would exclude 999 (999 > 342).
    monkeypatch.setenv("UK_PVLIVE_MAX_GSP_ID", "9999")
    with patch(
        "solar_consumer.data.fetch_gb_data.load_gsp_merge_weights",
        return_value=load_gsp_merge_weights(str(tmp_path / "gsp_merge_weights.yaml")),
    ), patch("solar_consumer.data.fetch_gb_data.PVLive", return_value=mock_pvl):
        df = fetch_gb_data_historic(regime="in-day")
    gsp999 = df[df["gsp_id"] == 999]
    assert not gsp999.empty, "Expected rows for remapped GSP ID 999"
    assert (gsp999["solar_generation_kw"] == 5_000.0).all(), (
        f"Expected 5000 kW, got {gsp999['solar_generation_kw'].unique()}"
    )


# ---------------------------------------------------------------------------
# 5. test_negative_weight
# ---------------------------------------------------------------------------

def test_negative_weight(tmp_path):
    """
    5. Negative weight: A negative weight in the YAML config should fail Pydantic validation.
    """
    yaml_content = textwrap.dedent("""\
        158:
          pvlive_merge_weights:
            - gsp_id: 12
              weight: -1.0
    """)
    (tmp_path / "gsp_merge_weights.yaml").write_text(yaml_content)

    with pytest.raises(ValidationError):
        load_gsp_merge_weights(str(tmp_path / "gsp_merge_weights.yaml"))


# ---------------------------------------------------------------------------
# 6. test_no_merge_weights_unchanged
# ---------------------------------------------------------------------------

def test_no_merge_weights_unchanged(tmp_path, monkeypatch):
    """
    6. No-config: With an empty merge config, the loop iterates exactly over pvlive.gsp_ids.
    All returned IDs should appear in the output.
    """
    source_df = _make_gsp_df(generation_mw=5.0)

    def mock_between(**kwargs):
        return source_df.copy()

    # PVLive returns exactly IDs 0, 1, 2, 3.
    mock_pvl = _mock_pvlive(gsp_ids=[0, 1, 2, 3], between_side_effect=mock_between)

    monkeypatch.delenv("UK_PVLIVE_N_GSPS", raising=False)
    with patch(
        "solar_consumer.data.fetch_gb_data.load_gsp_merge_weights",
        return_value={},
    ), patch("solar_consumer.data.fetch_gb_data.PVLive", return_value=mock_pvl):
        df = fetch_gb_data_historic(regime="in-day")

    assert set(df["gsp_id"].unique()) == {0, 1, 2, 3}
    assert (df["solar_generation_kw"] == 5_000.0).all()


# ---------------------------------------------------------------------------
# 7. test_deprecated_ids_without_mapping_are_skipped
# ---------------------------------------------------------------------------

def test_deprecated_ids_without_mapping_are_skipped(tmp_path, monkeypatch):
    """
    7. Deprecated IDs absent from pvlive.gsp_ids are not fetched: IDs absent from pvlive.gsp_ids that have no merge config entry
    are simply never iterated — they do not appear in the output.
    """
    source_df = _make_gsp_df(generation_mw=1.0)

    def mock_between(**kwargs):
        return source_df.copy()

    # PVLive omits IDs 4 and 5 (they no longer exist in its registry).
    mock_pvl = _mock_pvlive(gsp_ids=[0, 1, 2, 3, 6], between_side_effect=mock_between)

    monkeypatch.delenv("UK_PVLIVE_N_GSPS", raising=False)
    with patch(
        "solar_consumer.data.fetch_gb_data.load_gsp_merge_weights",
        return_value={},
    ), patch("solar_consumer.data.fetch_gb_data.PVLive", return_value=mock_pvl):
        df = fetch_gb_data_historic(regime="in-day")

    returned_ids = set(df["gsp_id"].unique())
    # 4 and 5 are not in gsp_ids and have no merge entry — must be absent.
    assert 4 not in returned_ids, "GSP ID 4 should be absent (not in pvlive.gsp_ids)"
    assert 5 not in returned_ids, "GSP ID 5 should be absent (not in pvlive.gsp_ids)"
    # The IDs PVLive returned should all be present.
    assert {0, 1, 2, 3, 6}.issubset(returned_ids)


# ---------------------------------------------------------------------------
# reconstruct_gsp_from_weights — unit tests
# ---------------------------------------------------------------------------


def _make_pvlive_mock(source_data: dict[int, pd.DataFrame]) -> MagicMock:
    """Return a PVLive mock whose .between() returns from source_data by entity_id."""
    mock = MagicMock()
    mock.between.side_effect = lambda **kw: source_data[kw["entity_id"]].copy()
    return mock


def test_reconstruct_single_source():
    """8. Single source with weight=1.0 — result equals the source unchanged."""
    src_df = _make_gsp_df(generation_mw=10.0)
    pvlive = _make_pvlive_mock({1: src_df})
    config = [GSPMergeSource(gsp_id=1, weight=1.0)]

    result = reconstruct_gsp_from_weights(
        gsp_id=99, weights_config=config, pvlive=pvlive,
        start=START, end=END, fetched_cache={},
    )

    assert result is not None
    assert list(result.columns) == ["datetime_gmt", "generation_mw", "installedcapacity_mwp", "capacity_mwp", "updated_gmt"]
    assert (result["generation_mw"] == 10.0).all()


def test_reconstruct_two_sources_summed():
    """9. Two sources with weight=1.0 each — generation is summed per timestamp."""
    src_a = _make_gsp_df(generation_mw=7.0)
    src_b = _make_gsp_df(generation_mw=8.0)
    pvlive = _make_pvlive_mock({10: src_a, 11: src_b})
    config = [
        GSPMergeSource(gsp_id=10, weight=1.0),
        GSPMergeSource(gsp_id=11, weight=1.0),
    ]

    result = reconstruct_gsp_from_weights(
        gsp_id=99, weights_config=config, pvlive=pvlive,
        start=START, end=END, fetched_cache={},
    )

    assert result is not None
    assert (result["generation_mw"] == 15.0).all()


def test_reconstruct_fractional_weight():
    """10. weight=0.5 halves the source generation."""
    src_df = _make_gsp_df(generation_mw=10.0)
    pvlive = _make_pvlive_mock({5: src_df})
    config = [GSPMergeSource(gsp_id=5, weight=0.5)]

    result = reconstruct_gsp_from_weights(
        gsp_id=99, weights_config=config, pvlive=pvlive,
        start=START, end=END, fetched_cache={},
    )

    assert result is not None
    assert np.allclose(result["generation_mw"].values, 5.0)
    assert np.allclose(result["capacity_mwp"].values, 45.0)
    assert np.allclose(result["installedcapacity_mwp"].values, 50.0)


def test_reconstruct_negative_weight():
    """11. Negative weight is rejected by Pydantic validation."""
    with pytest.raises(ValidationError):
        GSPMergeSource(gsp_id=99, weight=-1.0)


def test_reconstruct_uses_cache_and_skips_api_call():
    """12. If source is already in fetched_cache, pvlive.between is NOT called for it."""
    cached_df = _make_gsp_df(generation_mw=6.0)
    pvlive = MagicMock()  # should never be called
    cache = {42: cached_df}
    config = [GSPMergeSource(gsp_id=42, weight=1.0)]

    result = reconstruct_gsp_from_weights(
        gsp_id=99, weights_config=config, pvlive=pvlive,
        start=START, end=END, fetched_cache=cache,
    )

    pvlive.between.assert_not_called()
    assert result is not None
    assert (result["generation_mw"] == 6.0).all()


def test_reconstruct_populates_cache():
    """13. Newly fetched sources are stored in fetched_cache for the caller."""
    src_df = _make_gsp_df(generation_mw=5.0)
    pvlive = _make_pvlive_mock({7: src_df})
    cache: dict = {}
    config = [GSPMergeSource(gsp_id=7, weight=1.0)]

    reconstruct_gsp_from_weights(
        gsp_id=99, weights_config=config, pvlive=pvlive,
        start=START, end=END, fetched_cache=cache,
    )

    assert 7 in cache, "Source GSP 7 should have been added to fetched_cache"


def test_reconstruct_empty_weights_returns_none():
    """14. Empty weights_config → no source_dfs → returns None."""
    pvlive = MagicMock()
    result = reconstruct_gsp_from_weights(
        gsp_id=99, weights_config=[], pvlive=pvlive,
        start=START, end=END, fetched_cache={},
    )

    assert result is None
    pvlive.between.assert_not_called()


def test_reconstruct_updated_gmt_from_first_source():
    """15. updated_gmt in the result is taken from the first source DataFrame."""
    t1 = datetime(2025, 1, 14, 7, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 14, 9, 0, tzinfo=UTC)

    datetimes = pd.date_range("2025-01-14 06:00", periods=2, freq="30min", tz="UTC")
    src_a = pd.DataFrame({
        "datetime_gmt": datetimes,
        "generation_mw": [5.0, 5.0],
        "installedcapacity_mwp": [100.0, 100.0],
        "capacity_mwp": [90.0, 90.0],
        "updated_gmt": [t1, t1],
    })
    src_b = pd.DataFrame({
        "datetime_gmt": datetimes,
        "generation_mw": [3.0, 3.0],
        "installedcapacity_mwp": [50.0, 50.0],
        "capacity_mwp": [45.0, 45.0],
        "updated_gmt": [t2, t2],
    })

    pvlive = _make_pvlive_mock({1: src_a, 2: src_b})
    config = [
        GSPMergeSource(gsp_id=1, weight=1.0),
        GSPMergeSource(gsp_id=2, weight=1.0),
    ]

    result = reconstruct_gsp_from_weights(
        gsp_id=99, weights_config=config, pvlive=pvlive,
        start=START, end=END, fetched_cache={},
    )

    assert result is not None
    # updated_gmt must come from src_a (the first source), not src_b.
    # .values strips timezone, so compare as tz-naive Timestamps.
    expected = pd.Timestamp(t1.replace(tzinfo=None))
    assert all(pd.Timestamp(ts) == expected for ts in result["updated_gmt"])


def test_reconstruct_capacity_columns_summed():
    """16. installedcapacity_mwp and capacity_mwp are summed across sources."""
    src_a = _make_gsp_df(generation_mw=0.0)  # capacity_mwp=90, installedcapacity_mwp=100
    src_b = _make_gsp_df(generation_mw=0.0)  # same
    pvlive = _make_pvlive_mock({1: src_a, 2: src_b})
    config = [
        GSPMergeSource(gsp_id=1, weight=1.0),
        GSPMergeSource(gsp_id=2, weight=1.0),
    ]

    result = reconstruct_gsp_from_weights(
        gsp_id=99, weights_config=config, pvlive=pvlive,
        start=START, end=END, fetched_cache={},
    )

    assert result is not None
    assert (result["capacity_mwp"] == 180.0).all()
    assert (result["installedcapacity_mwp"] == 200.0).all()


# ---------------------------------------------------------------------------
# 17. test_remapping_when_target_is_in_registry
# ---------------------------------------------------------------------------

def test_remapping_when_target_is_in_registry(tmp_path, monkeypatch):
    """
    17. Target GSP ID present in registry is skipped from direct fetch and reconstructed:
    Verify that even if a GSP ID is returned by pvlive.gsp_ids, if it is defined in the
    merge config, it is not fetched directly but is instead reconstructed from its sources.
    """
    yaml_content = textwrap.dedent("""\
        4:
          pvlive_merge_weights:
            - gsp_id: 324
              weight: 1.0
            - gsp_id: 325
              weight: 1.0
    """)
    (tmp_path / "gsp_merge_weights.yaml").write_text(yaml_content)

    source_324 = _make_gsp_df(generation_mw=7.0)
    source_325 = _make_gsp_df(generation_mw=8.0)

    def mock_between(**kwargs):
        eid = kwargs["entity_id"]
        if eid == 324:
            return source_324.copy()
        if eid == 325:
            return source_325.copy()
        if eid == 4:
            # If direct fetch occurs, return 999.0 MW
            return _make_gsp_df(999.0)
        return _make_gsp_df(0.0)

    # PVLive registry DOES include ID 4.
    mock_pvl = _mock_pvlive(gsp_ids=[0, 1, 2, 3, 4], between_side_effect=mock_between)

    monkeypatch.delenv("UK_PVLIVE_N_GSPS", raising=False)
    with patch(
        "solar_consumer.data.fetch_gb_data.load_gsp_merge_weights",
        return_value=load_gsp_merge_weights(str(tmp_path / "gsp_merge_weights.yaml")),
    ), patch("solar_consumer.data.fetch_gb_data.PVLive", return_value=mock_pvl):
        df = fetch_gb_data_historic(regime="in-day")

    gsp4 = df[df["gsp_id"] == 4]
    assert not gsp4.empty, "Expected rows for remapped GSP ID 4"
    # Reconstructed generation should be 15 MW (15_000 kW), NOT 999 MW (999_000 kW)
    assert (gsp4["solar_generation_kw"] == 15_000.0).all(), (
        f"Expected 15000 kW (reconstructed), got {gsp4['solar_generation_kw'].unique()}"
    )

