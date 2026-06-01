"""
Unit Tests for GSP merge weights logic in fetch_gb_data.py

Tests cover:
- Split remapping: deprecated GSP reconstructed as sum of its parts
- Merge remapping: reconstruction with fractional weight
- Negative weight: special ARMO_P subtraction case
- No-config: direct fetch behaviour is unchanged
- Config loading: YAML parses to the correct dict structure
- Deprecated IDs absent from pvlive.gsp_ids are not fetched
"""
import os
import textwrap
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

from solar_consumer.data.fetch_gb_data import load_gsp_merge_weights


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
            "updated_gmt": [datetime(2025, 1, 14, 8, 0, tzinfo=timezone.utc)] * n_rows,
        }
    )


def _mock_pvlive(gsp_ids: list, between_side_effect) -> MagicMock:
    """Return a configured mock PVLive instance."""
    mock = MagicMock()
    mock.gsp_ids = gsp_ids
    mock.between.side_effect = between_side_effect
    return mock


# ---------------------------------------------------------------------------
# test_config_loads_correctly
# ---------------------------------------------------------------------------

def test_config_loads_correctly(tmp_path):
    """YAML parses to expected dict structure with int keys and float weights."""
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

    # Keys are ints, values are lists of dicts with int gsp_id and float weight.
    for k in result:
        assert isinstance(k, int)
    for entries in result.values():
        for entry in entries:
            assert isinstance(entry["gsp_id"], int)
            assert isinstance(entry["weight"], float)

    assert result[4] == [
        {"gsp_id": 324, "weight": 1.0},
        {"gsp_id": 325, "weight": 1.0},
    ]
    assert result[139] == [
        {"gsp_id": 323, "weight": 1.0},
        {"gsp_id": 334, "weight": 1.0},
    ]


def test_config_loads_missing_file(tmp_path):
    """Returns empty dict when config file does not exist."""
    result = load_gsp_merge_weights(str(tmp_path / "nonexistent.yaml"))
    assert result == {}


# ---------------------------------------------------------------------------
# test_split_remapping
# ---------------------------------------------------------------------------

def test_split_remapping(tmp_path):
    """
    GSP 4 is absent from pvlive.gsp_ids (it was split). It is defined in the
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

    with patch(
        "solar_consumer.data.fetch_gb_data.load_gsp_merge_weights",
        return_value=load_gsp_merge_weights(str(tmp_path / "gsp_merge_weights.yaml")),
    ), patch("solar_consumer.data.fetch_gb_data.PVLive", return_value=mock_pvl):
        if "UK_PVLIVE_N_GSPS" in os.environ:
            del os.environ["UK_PVLIVE_N_GSPS"]
        from solar_consumer.data.fetch_gb_data import fetch_gb_data_historic
        df = fetch_gb_data_historic(regime="in-day")

    gsp4 = df[df["gsp_id"] == 4]
    assert not gsp4.empty, "Expected rows for remapped GSP ID 4"
    assert (gsp4["solar_generation_kw"] == 15_000.0).all(), (
        f"Expected 15000 kW, got {gsp4['solar_generation_kw'].unique()}"
    )


# ---------------------------------------------------------------------------
# test_merge_remapping
# ---------------------------------------------------------------------------

def test_merge_remapping(tmp_path):
    """
    Tests fractional weight reconstruction: a hypothetical target GSP (ID 999,
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

    with patch(
        "solar_consumer.data.fetch_gb_data.load_gsp_merge_weights",
        return_value=load_gsp_merge_weights(str(tmp_path / "gsp_merge_weights.yaml")),
    ), patch("solar_consumer.data.fetch_gb_data.PVLive", return_value=mock_pvl):
        if "UK_PVLIVE_N_GSPS" in os.environ:
            del os.environ["UK_PVLIVE_N_GSPS"]
        if "UK_PVLIVE_MAX_GSP_ID" in os.environ:
            del os.environ["UK_PVLIVE_MAX_GSP_ID"]
        from solar_consumer.data.fetch_gb_data import fetch_gb_data_historic
        df = fetch_gb_data_historic(regime="in-day")
    gsp999 = df[df["gsp_id"] == 999]
    assert not gsp999.empty, "Expected rows for remapped GSP ID 999"
    assert (gsp999["solar_generation_kw"] == 5_000.0).all(), (
        f"Expected 5000 kW, got {gsp999['solar_generation_kw'].unique()}"
    )



# ---------------------------------------------------------------------------
# test_negative_weight
# ---------------------------------------------------------------------------

def test_negative_weight(tmp_path):
    """
    ARMO_P case: source gsp_id=12 has weight=1.0 (10 MW) and
    gsp_id=99 has weight=-1.0 (3 MW). GSP 158 is absent from pvlive.gsp_ids.
    Expected reconstructed generation = 7 MW per slot.
    """
    yaml_content = textwrap.dedent("""\
        158:
          pvlive_merge_weights:
            - gsp_id: 12
              weight: 1.0
            - gsp_id: 99
              weight: -1.0
    """)
    (tmp_path / "gsp_merge_weights.yaml").write_text(yaml_content)

    source_12 = _make_gsp_df(generation_mw=10.0)
    source_99 = _make_gsp_df(generation_mw=3.0)

    def mock_between(**kwargs):
        eid = kwargs["entity_id"]
        if eid == 12:
            return source_12.copy()
        if eid == 99:
            return source_99.copy()
        return _make_gsp_df(0.0)

    mock_pvl = _mock_pvlive(gsp_ids=[0, 1, 2], between_side_effect=mock_between)

    with patch(
        "solar_consumer.data.fetch_gb_data.load_gsp_merge_weights",
        return_value=load_gsp_merge_weights(str(tmp_path / "gsp_merge_weights.yaml")),
    ), patch("solar_consumer.data.fetch_gb_data.PVLive", return_value=mock_pvl):
        if "UK_PVLIVE_N_GSPS" in os.environ:
            del os.environ["UK_PVLIVE_N_GSPS"]
        if "UK_PVLIVE_MAX_GSP_ID" in os.environ:
            del os.environ["UK_PVLIVE_MAX_GSP_ID"]
        from solar_consumer.data.fetch_gb_data import fetch_gb_data_historic
        df = fetch_gb_data_historic(regime="in-day")
    gsp158 = df[df["gsp_id"] == 158]
    assert not gsp158.empty, "Expected rows for remapped GSP ID 158"
    assert np.allclose(gsp158["solar_generation_kw"].values, 7_000.0), (
        f"Expected 7000 kW, got {gsp158['solar_generation_kw'].unique()}"
    )


# ---------------------------------------------------------------------------
# test_no_merge_weights_unchanged
# ---------------------------------------------------------------------------

def test_no_merge_weights_unchanged(tmp_path):
    """
    With an empty merge config, the loop iterates exactly over pvlive.gsp_ids.
    All returned IDs should appear in the output.
    """
    source_df = _make_gsp_df(generation_mw=5.0)

    def mock_between(**kwargs):
        return source_df.copy()

    # PVLive returns exactly IDs 0, 1, 2, 3.
    mock_pvl = _mock_pvlive(gsp_ids=[0, 1, 2, 3], between_side_effect=mock_between)

    with patch(
        "solar_consumer.data.fetch_gb_data.load_gsp_merge_weights",
        return_value={},
    ), patch("solar_consumer.data.fetch_gb_data.PVLive", return_value=mock_pvl):
        if "UK_PVLIVE_N_GSPS" in os.environ:
            del os.environ["UK_PVLIVE_N_GSPS"]
        from solar_consumer.data.fetch_gb_data import fetch_gb_data_historic
        df = fetch_gb_data_historic(regime="in-day")

    assert set(df["gsp_id"].unique()) == {0, 1, 2, 3}
    assert (df["solar_generation_kw"] == 5_000.0).all()


# ---------------------------------------------------------------------------
# test_deprecated_ids_without_mapping_are_skipped
# ---------------------------------------------------------------------------

def test_deprecated_ids_without_mapping_are_skipped(tmp_path):
    """
    IDs absent from pvlive.gsp_ids that have no merge config entry
    are simply never iterated — they do not appear in the output.
    """
    source_df = _make_gsp_df(generation_mw=1.0)

    def mock_between(**kwargs):
        return source_df.copy()

    # PVLive omits IDs 4 and 5 (they no longer exist in its registry).
    mock_pvl = _mock_pvlive(gsp_ids=[0, 1, 2, 3, 6], between_side_effect=mock_between)

    with patch(
        "solar_consumer.data.fetch_gb_data.load_gsp_merge_weights",
        return_value={},
    ), patch("solar_consumer.data.fetch_gb_data.PVLive", return_value=mock_pvl):
        if "UK_PVLIVE_N_GSPS" in os.environ:
            del os.environ["UK_PVLIVE_N_GSPS"]
        from solar_consumer.data.fetch_gb_data import fetch_gb_data_historic
        df = fetch_gb_data_historic(regime="in-day")

    returned_ids = set(df["gsp_id"].unique())
    # 4 and 5 are not in gsp_ids and have no merge entry — must be absent.
    assert 4 not in returned_ids, "GSP ID 4 should be absent (not in pvlive.gsp_ids)"
    assert 5 not in returned_ids, "GSP ID 5 should be absent (not in pvlive.gsp_ids)"
    # The IDs PVLive returned should all be present.
    assert {0, 1, 2, 3, 6}.issubset(returned_ids)
