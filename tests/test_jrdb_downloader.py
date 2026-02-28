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
