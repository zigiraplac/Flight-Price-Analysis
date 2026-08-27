"""Unit tests for plugins/ingestion.py's file-existence pre-check -- no DB/Airflow needed."""

import pytest

from plugins.ingestion import MissingSourceFileError, assert_source_file_exists


def test_missing_file_is_rejected(tmp_path):
    missing_path = tmp_path / "does_not_exist.csv"
    with pytest.raises(MissingSourceFileError, match="not found"):
        assert_source_file_exists(str(missing_path))


def test_empty_file_is_rejected(tmp_path):
    empty_path = tmp_path / "empty.csv"
    empty_path.write_text("")
    with pytest.raises(MissingSourceFileError, match="is empty"):
        assert_source_file_exists(str(empty_path))


def test_present_non_empty_file_passes(tmp_path):
    csv_path = tmp_path / "flights.csv"
    csv_path.write_text("Airline,Source,Destination\n")
    assert_source_file_exists(str(csv_path))  # does not raise
