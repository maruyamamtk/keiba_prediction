"""
src.automation.data.result_integrity のテスト（Issue #440）
"""

from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from src.automation.data.jrdb_downloader import JRDBDownloader, is_preliminary_sec_file
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

        result = find_incomplete_result_dates(client, "proj", date(2026, 1, 1), date(2026, 3, 31))

        assert result == [IncompleteResultDate(date(2026, 2, 14), 539, 248, 248)]
        query, = client.query.call_args.args
        assert "`proj.raw.race_results`" in query
        assert "`proj.raw.horse_results`" in query
        params = {p.name: p.value for p in client.query.call_args.kwargs["job_config"].query_parameters}
        assert params == {
            "start_date": date(2026, 1, 1),
            "end_date": date(2026, 3, 31),
            "idm_null_rate_threshold": 0.2,
        }

    def test_dataset_id(self):
        """検知は再ロード先と同じデータセットを参照する"""
        client = MagicMock()
        client.query.return_value.result.return_value = []
        find_incomplete_result_dates(client, "proj", date(2026, 1, 1), date(2026, 1, 31), dataset_id="raw_test")
        query, = client.query.call_args.args
        assert "`proj.raw_test.race_results`" in query
        assert "`proj.raw.`" not in query and ".raw." not in query

    def test_no_incomplete_dates(self):
        client = MagicMock()
        client.query.return_value.result.return_value = []
        assert find_incomplete_result_dates(client, "proj", date(2026, 1, 1), date(2026, 1, 31)) == []


class TestIsPreliminarySecFile:
    """SECファイルの中身（IDM NULL率）で速報版を判定する"""

    @staticmethod
    def _run(tmp_path, idms):
        from unittest.mock import patch

        path = tmp_path / "SEC260124.csv"
        path.write_text("".join(f"{i}\n" for i in range(len(idms))), encoding="utf-8")
        with patch(
            "src.automation.data.jrdb_parser.JRDBParser.parse_sec_line",
            side_effect=lambda line: {"idm": idms[int(line)]},
        ):
            return is_preliminary_sec_file(path)

    def test_mostly_null_is_preliminary(self, tmp_path):
        assert self._run(tmp_path, [None] * 9 + [50.0]) is True

    def test_normal_null_rate_is_final(self, tmp_path):
        # 取消・競走中止などで数%は NULL になる
        assert self._run(tmp_path, [None] + [50.0] * 19) is False

    def test_empty_file_is_not_preliminary(self, tmp_path):
        """解析できる行がないファイルは判定できないため取り直し対象にしない（毎回の403再試行を防ぐ）"""
        assert self._run(tmp_path, []) is False


def _make_mocks(tmp_path: Path):
    downloader = MagicMock()
    downloader.datatype_to_folder.side_effect = JRDBDownloader.datatype_to_folder
    downloader.get_output_dir.return_value = tmp_path
    downloader.local_csv_path.side_effect = lambda dt, d: tmp_path / "Sec" / f"{dt}{d}.csv"
    downloader.download_single.return_value = True
    downloader.get_available_dates.return_value = ["260124", "260125"]
    uploader = MagicMock()
    uploader.upload_file.return_value = True
    loader = MagicMock()
    loader.bq_client.query.return_value.result.return_value = []
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

    def test_rechecks_only_reloaded_dates(self, tmp_path):
        """再ロードした日だけを再検査し、なお不完全な日を remaining に返す"""
        downloader, uploader, loader = _make_mocks(tmp_path)
        downloader.get_available_dates.return_value = ["260124", "260131"]
        loader.bq_client.query.return_value.result.return_value = [
            SimpleNamespace(race_date=date(2026, 1, 24), expected_rows=500, actual_rows=500, idm_null_rows=400),
            SimpleNamespace(race_date=date(2026, 1, 25), expected_rows=500, actual_rows=500, idm_null_rows=400),
        ]

        result = refetch_sec_files(downloader, uploader, loader, ["260124", "260131", "260208"])

        assert result.unavailable == ["260208"]
        # 1/25 は再ロード対象外なので remaining に含めない
        assert result.remaining == [IncompleteResultDate(date(2026, 1, 24), 500, 500, 400)]
        params = {p.name: p.value for p in loader.bq_client.query.call_args.kwargs["job_config"].query_parameters}
        assert (params["start_date"], params["end_date"]) == (date(2026, 1, 24), date(2026, 1, 31))

    def test_no_recheck_when_nothing_reloaded(self, tmp_path):
        downloader, uploader, loader = _make_mocks(tmp_path)
        downloader.download_single.return_value = False

        result = refetch_sec_files(downloader, uploader, loader, ["260124"])

        assert result.failed == ["260124"]
        loader.bq_client.query.assert_not_called()

    def test_exception_does_not_stop_remaining_dates(self, tmp_path):
        """1日分の例外（GCSの一時エラー等）で残りの日の再取得を止めない"""
        downloader, uploader, loader = _make_mocks(tmp_path)
        uploader.upload_file.side_effect = [Exception("503"), True]

        result = refetch_sec_files(downloader, uploader, loader, ["260124", "260125"])

        assert result.failed == ["260124"]
        assert result.reloaded == ["260125"]


    def test_unavailable_on_jrdb_is_not_failure(self, tmp_path):
        """JRDBにSECが公開されていない日（開催中止等）は failed ではなく unavailable"""
        downloader, uploader, loader = _make_mocks(tmp_path)

        result = refetch_sec_files(downloader, uploader, loader, ["260124", "260208"])

        assert result.unavailable == ["260208"]
        assert result.failed == []
        assert result.reloaded == ["260124"]
        downloader.download_single.assert_called_once_with("SEC", "260124", force=True)

    def test_index_fetch_failure_marks_all_failed(self, tmp_path):
        """公開日一覧の取得失敗を「全日公開なし」と誤判定せず failed にする"""
        downloader, uploader, loader = _make_mocks(tmp_path)
        downloader.get_available_dates.return_value = []

        result = refetch_sec_files(downloader, uploader, loader, ["260124", "260125"])

        assert result.failed == ["260124", "260125"]
        assert result.unavailable == []
        downloader.download_single.assert_not_called()


class TestRefetchSecArgs:
    """scripts/refetch_sec.py の引数解析"""

    def test_dates_ignores_empty_segments(self):
        from scripts.refetch_sec import parse_args

        args = parse_args(["--dates", "2026-01-24, 2026-01-25,"])
        assert args.dates == [date(2026, 1, 24), date(2026, 1, 25)]

    def test_invalid_date_is_parser_error(self):
        import pytest

        from scripts.refetch_sec import parse_args

        with pytest.raises(SystemExit):
            parse_args(["--dates", "2026/01/24"])

    def test_detect_requires_start_date(self):
        import pytest

        from scripts.refetch_sec import parse_args

        with pytest.raises(SystemExit):
            parse_args(["--detect"])

    def test_dates_with_start_date_is_error(self):
        import pytest

        from scripts.refetch_sec import parse_args

        with pytest.raises(SystemExit):
            parse_args(["--dates", "2026-01-24", "--start-date", "2026-01-01"])

    def test_invalid_start_date_is_parser_error(self):
        import pytest

        from scripts.refetch_sec import parse_args

        with pytest.raises(SystemExit):
            parse_args(["--detect", "--start-date", "2026/01/01"])

    def test_detect_dates_are_parsed(self):
        from scripts.refetch_sec import parse_args

        args = parse_args(["--detect", "--start-date", "2016-01-01", "--end-date", "2026-09-18"])
        assert (args.start_date, args.end_date) == (date(2016, 1, 1), date(2026, 9, 18))


class TestRefetchSecMain:
    """scripts/refetch_sec.py の終了コード"""

    def _run(self, refetch_result):
        from unittest.mock import patch

        from scripts import refetch_sec

        with patch.object(refetch_sec, "load_dotenv"), \
                patch.object(refetch_sec, "create_loader_from_env", return_value=MagicMock()), \
                patch.object(refetch_sec, "create_downloader_from_env", return_value=MagicMock()), \
                patch.object(refetch_sec, "create_uploader_from_env", return_value=MagicMock()), \
                patch.object(refetch_sec, "refetch_sec_files", return_value=refetch_result):
            return refetch_sec.main(["--dates", "2026-01-24,2026-02-08"])

    def test_unavailable_is_not_failure(self):
        """JRDBに公開がない日だけなら終了コード0"""
        from src.automation.data.result_integrity import RefetchResult

        assert self._run(RefetchResult(reloaded=["260124"], unavailable=["260208"])) == 0

    def test_remaining_after_reload_is_failure(self):
        from src.automation.data.result_integrity import RefetchResult

        still = IncompleteResultDate(date(2026, 1, 24), 500, 500, 450)
        assert self._run(RefetchResult(reloaded=["260124"], remaining=[still])) == 1

    def test_failed_is_failure(self):
        from src.automation.data.result_integrity import RefetchResult

        assert self._run(RefetchResult(failed=["260124"])) == 1
