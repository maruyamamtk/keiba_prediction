"""
src.automation.data.result_integrity のテスト（Issue #440）
"""

from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from src.automation.data.jrdb_downloader import JRDBDownloader
from src.automation.data.load_to_bq import LoadResult
from src.automation.data.result_integrity import (
    IncompleteResultDate,
    find_incomplete_result_dates,
    refetch_sec_files,
)


class TestIncompleteResultDate:
    def test_properties(self):
        d = IncompleteResultDate(date(2026, 1, 24), expected_rows=500, actual_rows=500, idm_null_rows=448)
        assert d.yymmdd == "260124"
        assert d.idm_null_rate == 0.896
        assert d.to_dict() == {
            "race_date": "2026-01-24",
            "expected_rows": 500,
            "actual_rows": 500,
            "idm_null_rate": 0.896,
        }

    def test_idm_null_rate_when_no_results(self):
        """成績行が1件もない日は NULL 率 1.0 として扱う"""
        d = IncompleteResultDate(date(2024, 8, 25), expected_rows=498, actual_rows=0, idm_null_rows=0)
        assert d.idm_null_rate == 1.0


class TestFindIncompleteResultDates:
    def test_returns_rows_and_passes_parameters(self):
        client = MagicMock()
        client.query.return_value.result.return_value = [
            SimpleNamespace(race_date=date(2026, 2, 14), expected_rows=539, actual_rows=248, idm_null_rows=248),
        ]

        result = find_incomplete_result_dates(
            client, "proj", date(2026, 1, 1), date(2026, 3, 31),
            idm_null_rate_threshold=0.3, min_row_ratio=0.8,
        )

        assert result == [IncompleteResultDate(date(2026, 2, 14), 539, 248, 248)]
        query, = client.query.call_args.args
        assert "`proj.raw.race_results`" in query
        assert "`proj.raw.horse_results`" in query
        params = {p.name: p.value for p in client.query.call_args.kwargs["job_config"].query_parameters}
        assert params == {
            "start_date": date(2026, 1, 1),
            "end_date": date(2026, 3, 31),
            "min_row_ratio": 0.8,
            "idm_null_rate_threshold": 0.3,
        }

    def test_no_incomplete_dates(self):
        client = MagicMock()
        client.query.return_value.result.return_value = []
        assert find_incomplete_result_dates(client, "proj", date(2026, 1, 1), date(2026, 1, 31)) == []


def _make_mocks(tmp_path: Path):
    downloader = MagicMock()
    downloader.datatype_to_folder.side_effect = JRDBDownloader.datatype_to_folder
    downloader.get_output_dir.return_value = tmp_path
    downloader.download_single.return_value = True
    uploader = MagicMock()
    uploader.upload_file.return_value = True
    loader = MagicMock()
    loader.load_file.side_effect = lambda blob: LoadResult(
        file_name=blob, status="success", records_processed=500
    )
    return downloader, uploader, loader


class TestRefetchSecFiles:
    def test_success_forces_download_and_reloads(self, tmp_path):
        downloader, uploader, loader = _make_mocks(tmp_path)

        result = refetch_sec_files(downloader, uploader, loader, ["260124", "260125"])

        assert result.reloaded == ["260124", "260125"]
        assert result.failed == []
        assert result.records == 1000
        downloader.download_single.assert_any_call("SEC", "260124", force=True)
        uploader.upload_file.assert_any_call(tmp_path / "Sec" / "SEC260124.csv", "Sec/SEC260124.csv")
        loader.load_file.assert_any_call("Sec/SEC260125.csv")

    def test_download_failure_skips_upload_and_load(self, tmp_path):
        downloader, uploader, loader = _make_mocks(tmp_path)
        downloader.download_single.side_effect = [False, True]

        result = refetch_sec_files(downloader, uploader, loader, ["260124", "260125"])

        assert result.failed == ["260124"]
        assert result.reloaded == ["260125"]
        uploader.upload_file.assert_called_once()
        loader.load_file.assert_called_once_with("Sec/SEC260125.csv")

    def test_upload_failure_skips_load(self, tmp_path):
        downloader, uploader, loader = _make_mocks(tmp_path)
        uploader.upload_file.return_value = False

        result = refetch_sec_files(downloader, uploader, loader, ["260124"])

        assert result.failed == ["260124"]
        loader.load_file.assert_not_called()

    def test_load_failure(self, tmp_path):
        downloader, uploader, loader = _make_mocks(tmp_path)
        loader.load_file.side_effect = None
        loader.load_file.return_value = LoadResult(file_name="Sec/SEC260124.csv", status="failed", error="boom")

        result = refetch_sec_files(downloader, uploader, loader, ["260124"])

        assert result.failed == ["260124"]
        assert result.reloaded == []
        assert result.records == 0
