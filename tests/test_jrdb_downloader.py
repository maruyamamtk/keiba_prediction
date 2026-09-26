#!/usr/bin/env python3
"""
JRDBダウンローダーのテスト

Issue #53: JRDBダウンローダーのコンテナ化
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.automation.data.jrdb_downloader import (
    CSV_DATATYPES,
    DownloadResult,
    JRDBDownloader,
    create_downloader_from_env,
)


class TestJRDBDownloaderHelpers:
    """ヘルパー関数のテスト"""

    def test_datatype_to_folder_normal(self):
        """通常のデータタイプのフォルダ名変換"""
        assert JRDBDownloader.datatype_to_folder("KAA") == "Kaa"
        assert JRDBDownloader.datatype_to_folder("BAA") == "Baa"
        assert JRDBDownloader.datatype_to_folder("KYF") == "Kyf"
        assert JRDBDownloader.datatype_to_folder("SEC") == "Sec"

    def test_datatype_to_folder_csv_types(self):
        """CSV系データタイプのフォルダ名変換"""
        assert JRDBDownloader.datatype_to_folder("CSA") == "Cs"
        assert JRDBDownloader.datatype_to_folder("KSA") == "Ks"

    def test_get_extension_lzh(self):
        """通常のデータタイプの拡張子"""
        assert JRDBDownloader.get_extension("KAA") == ".lzh"
        assert JRDBDownloader.get_extension("BAA") == ".lzh"
        assert JRDBDownloader.get_extension("KYF") == ".lzh"

    def test_get_extension_csv(self):
        """CSV系データタイプの拡張子"""
        assert JRDBDownloader.get_extension("CSA") == ".csv"
        assert JRDBDownloader.get_extension("KSA") == ".csv"


class TestJRDBDownloaderInit:
    """初期化のテスト"""

    def test_init_with_output_dir(self):
        """出力ディレクトリ指定での初期化"""
        with tempfile.TemporaryDirectory() as tmpdir:
            downloader = JRDBDownloader("user", "pass", Path(tmpdir))
            assert downloader.username == "user"
            assert downloader.password == "pass"
            assert downloader.output_dir == Path(tmpdir)

    def test_init_without_output_dir(self):
        """出力ディレクトリ未指定での初期化（downloaded_files/）"""
        downloader = JRDBDownloader("user", "pass")
        assert downloader.output_dir.exists()
        assert downloader.output_dir.name == "downloaded_files"

    def test_get_output_dir(self):
        """出力ディレクトリの取得"""
        with tempfile.TemporaryDirectory() as tmpdir:
            downloader = JRDBDownloader("user", "pass", Path(tmpdir))
            assert downloader.get_output_dir() == Path(tmpdir)


class TestCreateDownloaderFromEnv:
    """環境変数からの作成テスト"""

    def test_create_with_env_vars(self):
        """環境変数が設定されている場合"""
        with patch.dict(os.environ, {"JRDB_USER": "testuser", "JRDB_PASSWORD": "testpass"}):
            downloader = create_downloader_from_env()
            assert downloader is not None
            assert downloader.username == "testuser"
            assert downloader.password == "testpass"
            downloader.cleanup()

    def test_create_without_user(self):
        """JRDB_USERが未設定の場合"""
        env = {"JRDB_PASSWORD": "testpass"}
        # 既存のJRDB_USER, JRDB_PASSWORDをクリア
        with patch.dict(os.environ, env, clear=True):
            downloader = create_downloader_from_env()
            assert downloader is None

    def test_create_without_password(self):
        """JRDB_PASSWORDが未設定の場合"""
        env = {"JRDB_USER": "testuser"}
        with patch.dict(os.environ, env, clear=True):
            downloader = create_downloader_from_env()
            assert downloader is None

    def test_create_with_output_dir(self):
        """JRDB_OUTPUT_DIRが設定されている場合"""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(os.environ, {
                "JRDB_USER": "testuser",
                "JRDB_PASSWORD": "testpass",
                "JRDB_OUTPUT_DIR": tmpdir,
            }):
                downloader = create_downloader_from_env()
                assert downloader is not None
                assert downloader.output_dir == Path(tmpdir)


class TestJRDBDownloaderEncodingConversion:
    """エンコーディング変換のテスト"""

    def test_convert_encoding_success(self):
        """CP932からUTF-8への変換成功"""
        with tempfile.TemporaryDirectory() as tmpdir:
            downloader = JRDBDownloader("user", "pass", Path(tmpdir))

            # CP932でテストファイルを作成
            test_file = Path(tmpdir) / "test.txt"
            test_content = "テスト日本語"
            test_file.write_bytes(test_content.encode("cp932"))

            # 変換
            result = downloader._convert_encoding(test_file)
            assert result is True

            # UTF-8で読み込めることを確認
            content = test_file.read_text(encoding="utf-8")
            assert content == test_content

    def test_convert_encoding_invalid_file(self):
        """存在しないファイルの変換"""
        with tempfile.TemporaryDirectory() as tmpdir:
            downloader = JRDBDownloader("user", "pass", Path(tmpdir))
            result = downloader._convert_encoding(Path(tmpdir) / "nonexistent.txt")
            assert result is False


class TestJRDBDownloaderDownload:
    """ダウンロード処理のテスト"""

    def test_download_single_already_exists(self):
        """既にダウンロード済みのファイルをスキップ"""
        with tempfile.TemporaryDirectory() as tmpdir:
            downloader = JRDBDownloader("user", "pass", Path(tmpdir))

            # 既存ファイルを作成
            folder = Path(tmpdir) / "Kaa"
            folder.mkdir()
            (folder / "KAA240101.csv").write_text("dummy")

            # ダウンロードを実行（スキップされるはず）
            result = downloader.download_single("KAA", "240101")
            assert result is True

    @patch("urllib.request.urlretrieve")
    def test_download_single_new_file(self, mock_urlretrieve):
        """新規ファイルのダウンロード（CSVタイプ）"""
        with tempfile.TemporaryDirectory() as tmpdir:
            downloader = JRDBDownloader("user", "pass", Path(tmpdir))

            # urlretrieveをモック
            def fake_download(url, path):
                Path(path).write_bytes("テストデータ".encode("cp932"))

            mock_urlretrieve.side_effect = fake_download

            result = downloader.download_single("CSA", "240101")
            assert result is True
            mock_urlretrieve.assert_called_once()

    @patch("urllib.request.urlretrieve")
    def test_download_single_force_overwrites_existing(self, mock_urlretrieve):
        """force=True なら既存ファイルがあっても再ダウンロードして上書きする（Issue #440）"""
        with tempfile.TemporaryDirectory() as tmpdir:
            downloader = JRDBDownloader("user", "pass", Path(tmpdir))
            folder = Path(tmpdir) / "Cs"
            folder.mkdir()
            existing = folder / "CSA240101.csv"
            existing.write_text("速報版")

            def fake_download(url, path):
                Path(path).write_bytes("確定版".encode("cp932"))

            mock_urlretrieve.side_effect = fake_download

            result = downloader.download_single("CSA", "240101", force=True)

            assert result is True
            mock_urlretrieve.assert_called_once()
            assert existing.read_text(encoding="utf-8") == "確定版"

    @patch("urllib.request.urlretrieve")
    def test_download_single_force_keeps_existing_on_failure(self, mock_urlretrieve):
        """force=True で取得に失敗した場合は既存ファイルを残して False を返す"""
        import urllib.error

        with tempfile.TemporaryDirectory() as tmpdir:
            downloader = JRDBDownloader("user", "pass", Path(tmpdir))
            folder = Path(tmpdir) / "Cs"
            folder.mkdir()
            existing = folder / "CSA240101.csv"
            existing.write_text("速報版")
            mock_urlretrieve.side_effect = urllib.error.HTTPError("url", 403, "Forbidden", {}, None)

            result = downloader.download_single("CSA", "240101", force=True)

            assert result is False
            assert existing.read_text() == "速報版"
            assert list(folder.iterdir()) == [existing]

    @patch.object(JRDBDownloader, "_process_downloaded_file", side_effect=OSError("disk full"))
    @patch.object(JRDBDownloader, "_download_file")
    def test_download_single_force_restores_existing_on_exception(self, mock_download, _mock_process):
        """force=True で処理中に例外が出ても既存ファイルを戻す"""
        with tempfile.TemporaryDirectory() as tmpdir:
            downloader = JRDBDownloader("user", "pass", Path(tmpdir))
            folder = Path(tmpdir) / "Sec"
            folder.mkdir()
            existing = folder / "SEC260124.csv"
            existing.write_text("速報版")
            mock_download.return_value = folder / "SEC260124.lzh"

            with pytest.raises(OSError):
                downloader.download_single("SEC", "260124", force=True)

            assert existing.read_text() == "速報版"
            assert not (folder / "SEC260124.csv.stale").exists()

    @patch.object(JRDBDownloader, "_process_downloaded_file", return_value=True)
    @patch.object(JRDBDownloader, "_download_file")
    def test_download_single_force_fails_when_no_csv_produced(self, mock_download, _mock_process):
        """force=True で解凍後にCSVが生成されなければ失敗扱いにし、既存ファイルを戻す"""
        with tempfile.TemporaryDirectory() as tmpdir:
            downloader = JRDBDownloader("user", "pass", Path(tmpdir))
            folder = Path(tmpdir) / "Sec"
            folder.mkdir()
            existing = folder / "SEC260124.csv"
            existing.write_text("速報版")
            mock_download.return_value = folder / "SEC260124.lzh"

            result = downloader.download_single("SEC", "260124", force=True)

            assert result is False
            assert existing.read_text() == "速報版"


class TestDownloadFromDateWithEndDate:
    """download_from_date の end_date フィルタのテスト"""

    @patch("urllib.request.urlretrieve")
    @patch.object(JRDBDownloader, "get_available_dates")
    def test_download_from_date_with_end_date(self, mock_get_dates, mock_urlretrieve):
        """end_date が指定された場合、その日以降のファイルがスキップされること"""
        mock_get_dates.return_value = ["240101", "240601", "240701", "241201"]

        def fake_download(url, path):
            Path(path).write_bytes("テスト".encode("cp932"))

        mock_urlretrieve.side_effect = fake_download

        with tempfile.TemporaryDirectory() as tmpdir:
            downloader = JRDBDownloader("user", "pass", Path(tmpdir))
            result = downloader.download_from_date("CSA", "240101", "240630")

        # 240701 と 241201 はダウンロードされない（end_date=240630 を超えている）
        assert result.total_files == 2  # 240101, 240601 のみ

    @patch.object(JRDBDownloader, "download_from_date")
    @patch.object(JRDBDownloader, "get_available_datatypes")
    def test_download_all_from_date_with_end_date(self, mock_get_types, mock_dl_from_date):
        """end_date が download_from_date に渡されること"""
        mock_get_types.return_value = ["BAA", "KYF"]
        mock_dl_from_date.return_value = DownloadResult(0, 0, 0, 0)

        with tempfile.TemporaryDirectory() as tmpdir:
            downloader = JRDBDownloader("user", "pass", Path(tmpdir))
            downloader.download_all_from_date("240101", "240630")

        assert mock_dl_from_date.call_count == 2
        for call in mock_dl_from_date.call_args_list:
            assert call.args[1] == "240101"
            assert call.args[2] == "240630"

    @patch("urllib.request.urlretrieve")
    @patch.object(JRDBDownloader, "get_available_dates")
    def test_download_from_date_end_date_boundary_included(self, mock_get_dates, mock_urlretrieve):
        """end_date 当日のファイルはダウンロード対象に含まれること（境界値）"""
        mock_get_dates.return_value = ["240101", "240601", "240630", "240701"]

        def fake_download(url, path):
            Path(path).write_bytes("テスト".encode("cp932"))

        mock_urlretrieve.side_effect = fake_download

        with tempfile.TemporaryDirectory() as tmpdir:
            downloader = JRDBDownloader("user", "pass", Path(tmpdir))
            result = downloader.download_from_date("CSA", "240101", "240630")

        # 240630（end_date 当日）は含まれる、240701 は除外される
        assert result.total_files == 3  # 240101, 240601, 240630

    @patch("urllib.request.urlretrieve")
    @patch.object(JRDBDownloader, "get_available_dates")
    def test_download_from_date_without_end_date(self, mock_get_dates, mock_urlretrieve):
        """end_date 未指定の場合は上限なしで全件ダウンロード"""
        mock_get_dates.return_value = ["240101", "241201"]

        def fake_download(url, path):
            Path(path).write_bytes("テスト".encode("cp932"))

        mock_urlretrieve.side_effect = fake_download

        with tempfile.TemporaryDirectory() as tmpdir:
            downloader = JRDBDownloader("user", "pass", Path(tmpdir))
            result = downloader.download_from_date("CSA", "240101")

        assert result.total_files == 2

    @patch.object(JRDBDownloader, "get_available_dates")
    def test_download_from_date_start_after_end_raises(self, mock_get_dates):
        """start_date > end_date の場合は ValueError が発生すること"""
        mock_get_dates.return_value = []

        with tempfile.TemporaryDirectory() as tmpdir:
            downloader = JRDBDownloader("user", "pass", Path(tmpdir))
            with pytest.raises(ValueError, match="start_date"):
                downloader.download_from_date("CSA", "241231", "240101")


class TestDownloadResult:
    """DownloadResultのテスト"""

    def test_download_result_creation(self):
        """DownloadResultの作成"""
        result = DownloadResult(
            total_files=10,
            downloaded_files=8,
            skipped_files=1,
            failed_files=1,
        )
        assert result.total_files == 10
        assert result.downloaded_files == 8
        assert result.skipped_files == 1
        assert result.failed_files == 1


class TestCSVDatatypes:
    """CSV_DATATYPESの定義テスト"""

    def test_csv_datatypes_contains_csa(self):
        """CSV_DATATYPESにCSAが含まれる"""
        assert "CSA" in CSV_DATATYPES

    def test_csv_datatypes_contains_ksa(self):
        """CSV_DATATYPESにKSAが含まれる"""
        assert "KSA" in CSV_DATATYPES

    def test_csv_datatypes_does_not_contain_kaa(self):
        """CSV_DATATYPESにKAAが含まれない"""
        assert "KAA" not in CSV_DATATYPES


class TestPreliminaryRefetch:
    """速報版のまま残ったSECの取り直し（Issue #440）"""

    PRELIM = "src.automation.data.jrdb_downloader.is_preliminary_sec_file"

    @patch.object(JRDBDownloader, "get_available_dates", return_value=["260124", "260125"])
    @patch.object(JRDBDownloader, "_fetch", return_value=True)
    def test_download_from_date_refetches_preliminary_sec(self, mock_fetch, _mock_dates, tmp_path):
        """download_from_date は中身が速報版のSECを既存でもスキップしない"""
        downloader = JRDBDownloader("user", "pass", tmp_path)
        folder = tmp_path / "Sec"
        folder.mkdir()
        (folder / "SEC260124.csv").write_text("速報版")
        (folder / "SEC260125.csv").write_text("確定版")

        with patch(self.PRELIM, side_effect=lambda p: p.name == "SEC260124.csv"):
            result = downloader.download_from_date("SEC", "260124", "260125")

        mock_fetch.assert_called_once_with("SEC", "260124", folder / "SEC260124.csv")
        assert (result.downloaded_files, result.skipped_files) == (1, 1)

    @patch.object(JRDBDownloader, "_fetch")
    def test_non_sec_existing_is_skipped_without_content_check(self, mock_fetch, tmp_path):
        """SEC以外の既存ファイルは中身を読まずにスキップ"""
        downloader = JRDBDownloader("user", "pass", tmp_path)
        (tmp_path / "Kaa").mkdir()
        (tmp_path / "Kaa" / "KAA260124.csv").write_text("x")

        with patch(self.PRELIM) as mock_prelim:
            assert downloader.download_single("KAA", "260124") is True

        mock_prelim.assert_not_called()
        mock_fetch.assert_not_called()

    @patch.object(JRDBDownloader, "_download_file", return_value=None)
    def test_orphan_stale_is_restored(self, _mock_download, tmp_path):
        """前回の再取得が中断されて残った .stale は次回呼び出しで元に戻す"""
        downloader = JRDBDownloader("user", "pass", tmp_path)
        (tmp_path / "Sec").mkdir()
        (tmp_path / "Sec" / "SEC260124.csv.stale").write_text("確定版")

        with patch(self.PRELIM, return_value=False):
            assert downloader.download_single("SEC", "260124") is True
        assert (tmp_path / "Sec" / "SEC260124.csv").read_text() == "確定版"
        assert not (tmp_path / "Sec" / "SEC260124.csv.stale").exists()

    @patch.object(JRDBDownloader, "_process_downloaded_file", return_value=False)
    @patch.object(JRDBDownloader, "_download_file")
    def test_new_fetch_failure_removes_lzh(self, mock_download, _mock_process, tmp_path):
        """新規取得で解凍に失敗した .lzh は残さない（GCSへの誤アップロード防止）"""
        downloader = JRDBDownloader("user", "pass", tmp_path)
        (tmp_path / "Sec").mkdir()
        lzh = tmp_path / "Sec" / "SEC260124.lzh"
        lzh.write_bytes(b"broken")
        mock_download.return_value = lzh

        assert downloader.download_single("SEC", "260124") is False
        assert not lzh.exists()

    @patch.object(JRDBDownloader, "_process_downloaded_file", return_value=True)
    @patch.object(JRDBDownloader, "_download_file")
    def test_package_type_new_fetch_is_success(self, mock_download, _mock_process, tmp_path):
        """JRDBパッケージのように別名ファイルへ展開されるタイプは、同名CSVがなくても新規取得成功"""
        downloader = JRDBDownloader("user", "pass", tmp_path)
        mock_download.return_value = tmp_path / "Jrdb" / "JRDB260124.lzh"

        assert downloader.download_single("JRDB", "260124") is True

    def test_stale_with_new_csv_is_removed(self, tmp_path):
        """新しいCSV生成後・退避ファイル削除前に中断された場合は、退避ファイルを削除する"""
        downloader = JRDBDownloader("user", "pass", tmp_path)
        (tmp_path / "Sec").mkdir()
        (tmp_path / "Sec" / "SEC260124.csv").write_text("確定版")
        (tmp_path / "Sec" / "SEC260124.csv.stale").write_text("速報版")

        path = downloader.local_csv_path("SEC", "260124")

        assert path.read_text() == "確定版"
        assert not (tmp_path / "Sec" / "SEC260124.csv.stale").exists()
