#!/usr/bin/env python3
"""
upload_to_gcs.pyのテスト

GCSアップローダークラスの単体テスト
Issue #54: GCSアップロード処理のCloud Run対応
"""

import hashlib
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.data.upload_to_gcs import (
    GCSUploader,
    UploadResult,
    create_uploader_from_env,
    format_bytes,
)


class TestUploadResult:
    """UploadResultデータクラスのテスト"""

    def test_upload_result_creation(self):
        """UploadResultが正しく作成されることを確認"""
        result = UploadResult(
            total_files=100,
            uploaded_files=80,
            skipped_files=15,
            failed_files=5,
            uploaded_bytes=1024 * 1024,
        )

        assert result.total_files == 100
        assert result.uploaded_files == 80
        assert result.skipped_files == 15
        assert result.failed_files == 5
        assert result.uploaded_bytes == 1024 * 1024


class TestFormatBytes:
    """format_bytes関数のテスト"""

    def test_format_bytes_bytes(self):
        """バイト単位の変換"""
        assert "512.00 B" == format_bytes(512)

    def test_format_bytes_kilobytes(self):
        """キロバイト単位の変換"""
        assert "1.00 KB" == format_bytes(1024)

    def test_format_bytes_megabytes(self):
        """メガバイト単位の変換"""
        assert "1.00 MB" == format_bytes(1024 * 1024)

    def test_format_bytes_gigabytes(self):
        """ギガバイト単位の変換"""
        assert "1.00 GB" == format_bytes(1024 * 1024 * 1024)

    def test_format_bytes_terabytes(self):
        """テラバイト単位の変換"""
        assert "1.00 TB" == format_bytes(1024 * 1024 * 1024 * 1024)


class TestGCSUploaderCreation:
    """GCSUploaderの初期化テスト"""

    def test_create_with_credentials_path(self):
        """サービスアカウントキーファイルで初期化"""
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".json"
        ) as f:
            f.write('{"type": "service_account"}')
            credentials_path = f.name

        try:
            with patch(
                "src.data.upload_to_gcs.service_account.Credentials"
                ".from_service_account_file"
            ) as mock_creds, patch(
                "src.data.upload_to_gcs.storage.Client"
            ) as mock_client:
                mock_creds.return_value = MagicMock()
                uploader = GCSUploader(
                    project_id="test-project",
                    bucket_name="test-bucket",
                    credentials_path=credentials_path,
                )
                assert uploader.project_id == "test-project"
                assert uploader.bucket_name == "test-bucket"
                mock_creds.assert_called_once_with(credentials_path)
        finally:
            os.unlink(credentials_path)

    def test_create_with_missing_credentials_path_raises_error(self):
        """存在しないサービスアカウントキーファイルを指定した場合はエラー"""
        with pytest.raises(RuntimeError, match="サービスアカウントキーファイルが見つかりません"):
            GCSUploader(
                project_id="test-project",
                bucket_name="test-bucket",
                credentials_path="/nonexistent/path.json",
            )

    def test_create_with_adc(self):
        """Application Default Credentialsで初期化"""
        with patch(
            "src.data.upload_to_gcs.auth_default"
        ) as mock_auth_default, patch(
            "src.data.upload_to_gcs.storage.Client"
        ) as mock_client:
            mock_auth_default.return_value = (MagicMock(), "detected-project")
            uploader = GCSUploader(
                project_id="test-project",
                bucket_name="test-bucket",
            )
            assert uploader.project_id == "test-project"
            mock_auth_default.assert_called_once()

    def test_create_with_env_credentials(self):
        """環境変数のサービスアカウントキーで初期化"""
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".json"
        ) as f:
            f.write('{"type": "service_account"}')
            credentials_path = f.name

        try:
            with patch.dict(
                os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": credentials_path}
            ), patch(
                "src.data.upload_to_gcs.service_account.Credentials"
                ".from_service_account_file"
            ) as mock_creds, patch(
                "src.data.upload_to_gcs.storage.Client"
            ) as mock_client:
                mock_creds.return_value = MagicMock()
                uploader = GCSUploader(
                    project_id="test-project",
                    bucket_name="test-bucket",
                )
                assert uploader.project_id == "test-project"
        finally:
            os.unlink(credentials_path)

    def test_create_without_credentials_raises_error(self):
        """認証情報がない場合はエラー"""
        from google.auth.exceptions import DefaultCredentialsError

        with patch.dict(os.environ, {}, clear=True), patch(
            "src.data.upload_to_gcs.auth_default"
        ) as mock_auth_default:
            mock_auth_default.side_effect = DefaultCredentialsError(
                "Could not automatically determine credentials"
            )
            with pytest.raises(RuntimeError, match="GCP認証情報が見つかりません"):
                GCSUploader(
                    project_id="test-project",
                    bucket_name="test-bucket",
                )


class TestGCSUploaderMD5:
    """GCSUploaderのMD5計算テスト"""

    def test_calculate_md5(self):
        """MD5ハッシュ計算が正しいことを確認"""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            f.write("test content")
            temp_path = Path(f.name)

        try:
            with patch("src.data.upload_to_gcs.storage.Client"), patch(
                "src.data.upload_to_gcs.auth_default",
                return_value=(MagicMock(), "project"),
            ):
                uploader = GCSUploader(
                    project_id="test-project",
                    bucket_name="test-bucket",
                    local_base_dir=temp_path.parent,
                )

            # 期待されるMD5を計算
            expected_md5 = hashlib.md5(b"test content").hexdigest()
            actual_md5 = uploader._calculate_md5(temp_path)

            assert expected_md5 == actual_md5
        finally:
            temp_path.unlink()

    def test_calculate_md5_large_file(self):
        """大きなファイルのMD5計算が正しいことを確認"""
        # 16KB以上のファイルを作成（8KB単位でチャンク読み込みされる）
        content = b"x" * (16 * 1024)

        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            f.write(content)
            temp_path = Path(f.name)

        try:
            with patch("src.data.upload_to_gcs.storage.Client"), patch(
                "src.data.upload_to_gcs.auth_default",
                return_value=(MagicMock(), "project"),
            ):
                uploader = GCSUploader(
                    project_id="test-project",
                    bucket_name="test-bucket",
                    local_base_dir=temp_path.parent,
                )

            expected_md5 = hashlib.md5(content).hexdigest()
            actual_md5 = uploader._calculate_md5(temp_path)

            assert expected_md5 == actual_md5
        finally:
            temp_path.unlink()


class TestGCSUploaderShouldUpload:
    """GCSUploaderの_should_upload判定テスト"""

    def test_should_upload_when_blob_not_exists(self):
        """GCS上にファイルが存在しない場合はアップロード必要"""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            f.write("test content")
            temp_path = Path(f.name)

        try:
            with patch("src.data.upload_to_gcs.storage.Client"), patch(
                "src.data.upload_to_gcs.auth_default",
                return_value=(MagicMock(), "project"),
            ):
                uploader = GCSUploader(
                    project_id="test-project",
                    bucket_name="test-bucket",
                    local_base_dir=temp_path.parent,
                )

            # _get_gcs_blob_md5がNoneを返す（ファイルが存在しない）
            with patch.object(uploader, "_get_gcs_blob_md5", return_value=None):
                assert uploader._should_upload(temp_path, "test/file.txt") is True
        finally:
            temp_path.unlink()

    def test_should_upload_when_md5_differs(self):
        """MD5が異なる場合はアップロード必要"""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            f.write("test content")
            temp_path = Path(f.name)

        try:
            with patch("src.data.upload_to_gcs.storage.Client"), patch(
                "src.data.upload_to_gcs.auth_default",
                return_value=(MagicMock(), "project"),
            ):
                uploader = GCSUploader(
                    project_id="test-project",
                    bucket_name="test-bucket",
                    local_base_dir=temp_path.parent,
                )

            # 異なるMD5を返す
            with patch.object(
                uploader, "_get_gcs_blob_md5", return_value="different_md5"
            ):
                assert uploader._should_upload(temp_path, "test/file.txt") is True
        finally:
            temp_path.unlink()

    def test_should_not_upload_when_md5_matches(self):
        """MD5が一致する場合はアップロード不要"""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            f.write("test content")
            temp_path = Path(f.name)

        try:
            with patch("src.data.upload_to_gcs.storage.Client"), patch(
                "src.data.upload_to_gcs.auth_default",
                return_value=(MagicMock(), "project"),
            ):
                uploader = GCSUploader(
                    project_id="test-project",
                    bucket_name="test-bucket",
                    local_base_dir=temp_path.parent,
                )

            # ローカルファイルのMD5を計算
            local_md5 = uploader._calculate_md5(temp_path)

            # 同じMD5を返す
            with patch.object(uploader, "_get_gcs_blob_md5", return_value=local_md5):
                assert uploader._should_upload(temp_path, "test/file.txt") is False
        finally:
            temp_path.unlink()


class TestGCSUploaderUploadFile:
    """GCSUploaderのupload_fileテスト"""

    def test_upload_file_success(self):
        """単一ファイルのアップロード成功"""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write("test data")
            temp_path = Path(f.name)

        try:
            with patch("src.data.upload_to_gcs.storage.Client"), patch(
                "src.data.upload_to_gcs.auth_default",
                return_value=(MagicMock(), "project"),
            ):
                uploader = GCSUploader(
                    project_id="test-project",
                    bucket_name="test-bucket",
                )

            with patch.object(
                uploader, "_should_upload", return_value=True
            ), patch.object(uploader, "_upload_file_with_retry", return_value=True):
                result = uploader.upload_file(temp_path, "test/file.csv")

            assert result is True
        finally:
            temp_path.unlink()

    def test_upload_file_skipped(self):
        """単一ファイルがスキップされる場合"""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write("test data")
            temp_path = Path(f.name)

        try:
            with patch("src.data.upload_to_gcs.storage.Client"), patch(
                "src.data.upload_to_gcs.auth_default",
                return_value=(MagicMock(), "project"),
            ):
                uploader = GCSUploader(
                    project_id="test-project",
                    bucket_name="test-bucket",
                )

            with patch.object(uploader, "_should_upload", return_value=False):
                result = uploader.upload_file(temp_path, "test/file.csv")

            assert result is True  # スキップも成功扱い
        finally:
            temp_path.unlink()

    def test_upload_file_not_found(self):
        """存在しないファイルをアップロードしようとした場合"""
        with patch("src.data.upload_to_gcs.storage.Client"), patch(
            "src.data.upload_to_gcs.auth_default",
            return_value=(MagicMock(), "project"),
        ):
            uploader = GCSUploader(
                project_id="test-project",
                bucket_name="test-bucket",
            )

        result = uploader.upload_file(Path("/nonexistent/file.csv"), "test/file.csv")

        assert result is False

    def test_upload_file_force(self):
        """force=Trueで強制アップロード"""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write("test data")
            temp_path = Path(f.name)

        try:
            with patch("src.data.upload_to_gcs.storage.Client"), patch(
                "src.data.upload_to_gcs.auth_default",
                return_value=(MagicMock(), "project"),
            ):
                uploader = GCSUploader(
                    project_id="test-project",
                    bucket_name="test-bucket",
                )

            with patch.object(
                uploader, "_should_upload"
            ) as mock_should_upload, patch.object(
                uploader, "_upload_file_with_retry", return_value=True
            ):
                result = uploader.upload_file(temp_path, "test/file.csv", force=True)

            # force=Trueの場合は_should_uploadは呼ばれない
            mock_should_upload.assert_not_called()
            assert result is True
        finally:
            temp_path.unlink()


class TestGCSUploaderUploadDirectory:
    """GCSUploaderのupload_directoryテスト"""

    def test_upload_directory_empty(self):
        """存在しないディレクトリの場合は空の結果を返す"""
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch("src.data.upload_to_gcs.storage.Client"), patch(
                "src.data.upload_to_gcs.auth_default",
                return_value=(MagicMock(), "project"),
            ):
                uploader = GCSUploader(
                    project_id="test-project",
                    bucket_name="test-bucket",
                    local_base_dir=Path(temp_dir),
                )

            result = uploader.upload_directory("nonexistent")

            assert result.total_files == 0
            assert result.uploaded_files == 0
            assert result.skipped_files == 0
            assert result.failed_files == 0

    def test_upload_directory_filters_extensions(self):
        """サポートされる拡張子のファイルのみ対象となる"""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir) / "TestData"
            data_dir.mkdir()

            # サポートされる拡張子
            (data_dir / "file1.csv").write_text("data1")
            (data_dir / "file2.txt").write_text("data2")

            # サポートされない拡張子
            (data_dir / "file3.json").write_text("{}")
            (data_dir / "file4.py").write_text("code")

            with patch("src.data.upload_to_gcs.storage.Client"), patch(
                "src.data.upload_to_gcs.auth_default",
                return_value=(MagicMock(), "project"),
            ):
                uploader = GCSUploader(
                    project_id="test-project",
                    bucket_name="test-bucket",
                    local_base_dir=Path(temp_dir),
                )

            # ドライランで実行
            with patch.object(uploader, "_should_upload", return_value=True):
                result = uploader.upload_directory("TestData", dry_run=True)

            # csv, txtの2ファイルのみ対象
            assert result.total_files == 2
            assert result.uploaded_files == 2

    def test_upload_directory_dry_run(self):
        """ドライランでは実際にアップロードしない"""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir) / "TestData"
            data_dir.mkdir()
            (data_dir / "file1.csv").write_text("data1")

            mock_client = MagicMock()
            mock_bucket = MagicMock()
            mock_client.bucket.return_value = mock_bucket

            with patch(
                "src.data.upload_to_gcs.storage.Client", return_value=mock_client
            ), patch(
                "src.data.upload_to_gcs.auth_default",
                return_value=(MagicMock(), "project"),
            ):
                uploader = GCSUploader(
                    project_id="test-project",
                    bucket_name="test-bucket",
                    local_base_dir=Path(temp_dir),
                )

            with patch.object(uploader, "_should_upload", return_value=True):
                result = uploader.upload_directory("TestData", dry_run=True)

            # アップロードは実行されない
            mock_bucket.blob.return_value.upload_from_filename.assert_not_called()
            assert result.uploaded_files == 1

    def test_upload_directory_skips_unchanged(self):
        """変更のないファイルはスキップされる"""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir) / "TestData"
            data_dir.mkdir()
            (data_dir / "file1.csv").write_text("data1")
            (data_dir / "file2.csv").write_text("data2")

            with patch("src.data.upload_to_gcs.storage.Client"), patch(
                "src.data.upload_to_gcs.auth_default",
                return_value=(MagicMock(), "project"),
            ):
                uploader = GCSUploader(
                    project_id="test-project",
                    bucket_name="test-bucket",
                    local_base_dir=Path(temp_dir),
                )

            # 1つ目はアップロード不要、2つ目はアップロード必要
            with patch.object(
                uploader, "_should_upload", side_effect=[False, True]
            ), patch.object(uploader, "_upload_file_with_retry", return_value=True):
                result = uploader.upload_directory("TestData")

            assert result.total_files == 2
            assert result.uploaded_files == 1
            assert result.skipped_files == 1

    def test_upload_directory_force_uploads_all(self):
        """force=Trueの場合は重複チェックをスキップ"""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir) / "TestData"
            data_dir.mkdir()
            (data_dir / "file1.csv").write_text("data1")

            with patch("src.data.upload_to_gcs.storage.Client"), patch(
                "src.data.upload_to_gcs.auth_default",
                return_value=(MagicMock(), "project"),
            ):
                uploader = GCSUploader(
                    project_id="test-project",
                    bucket_name="test-bucket",
                    local_base_dir=Path(temp_dir),
                )

            with patch.object(
                uploader, "_should_upload"
            ) as mock_should_upload, patch.object(
                uploader, "_upload_file_with_retry", return_value=True
            ):
                result = uploader.upload_directory("TestData", force=True)

            # _should_uploadは呼ばれない
            mock_should_upload.assert_not_called()
            assert result.uploaded_files == 1


class TestGCSUploaderUploadAll:
    """GCSUploaderのupload_allテスト"""

    def test_upload_all_aggregates_results(self):
        """upload_allが全ディレクトリの結果を集計する"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 複数のデータタイプディレクトリを作成
            for data_type in ["Baa", "Kyf"]:
                data_dir = Path(temp_dir) / data_type
                data_dir.mkdir()
                (data_dir / "file1.csv").write_text("data1")

            with patch("src.data.upload_to_gcs.storage.Client"), patch(
                "src.data.upload_to_gcs.auth_default",
                return_value=(MagicMock(), "project"),
            ):
                uploader = GCSUploader(
                    project_id="test-project",
                    bucket_name="test-bucket",
                    local_base_dir=Path(temp_dir),
                )

            with patch.object(
                uploader, "_should_upload", return_value=True
            ), patch.object(uploader, "_upload_file_with_retry", return_value=True):
                result = uploader.upload_all()

            # 2ディレクトリ x 1ファイル = 2ファイル
            assert result.total_files == 2
            assert result.uploaded_files == 2

    def test_upload_all_ignores_hidden_directories(self):
        """隠しディレクトリは無視される"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 通常のディレクトリ
            data_dir = Path(temp_dir) / "Baa"
            data_dir.mkdir()
            (data_dir / "file1.csv").write_text("data1")

            # 隠しディレクトリ
            hidden_dir = Path(temp_dir) / ".hidden"
            hidden_dir.mkdir()
            (hidden_dir / "secret.csv").write_text("secret")

            with patch("src.data.upload_to_gcs.storage.Client"), patch(
                "src.data.upload_to_gcs.auth_default",
                return_value=(MagicMock(), "project"),
            ):
                uploader = GCSUploader(
                    project_id="test-project",
                    bucket_name="test-bucket",
                    local_base_dir=Path(temp_dir),
                )

            with patch.object(
                uploader, "_should_upload", return_value=True
            ), patch.object(uploader, "_upload_file_with_retry", return_value=True):
                result = uploader.upload_all()

            # 隠しディレクトリのファイルは含まれない
            assert result.total_files == 1

    def test_upload_all_nonexistent_base_dir(self):
        """ベースディレクトリが存在しない場合は空の結果を返す"""
        with patch("src.data.upload_to_gcs.storage.Client"), patch(
            "src.data.upload_to_gcs.auth_default",
            return_value=(MagicMock(), "project"),
        ):
            uploader = GCSUploader(
                project_id="test-project",
                bucket_name="test-bucket",
                local_base_dir=Path("/nonexistent/path"),
            )

        result = uploader.upload_all()

        assert result.total_files == 0
        assert result.uploaded_files == 0


class TestGCSUploaderVerifyBucket:
    """GCSUploaderのverify_bucket_existsテスト"""

    def test_verify_bucket_exists_true(self):
        """バケットが存在する場合Trueを返す"""
        mock_client = MagicMock()

        with patch(
            "src.data.upload_to_gcs.storage.Client", return_value=mock_client
        ), patch(
            "src.data.upload_to_gcs.auth_default",
            return_value=(MagicMock(), "project"),
        ):
            uploader = GCSUploader(
                project_id="test-project",
                bucket_name="test-bucket",
            )

        assert uploader.verify_bucket_exists() is True
        mock_client.get_bucket.assert_called_once_with("test-bucket")

    def test_verify_bucket_exists_false(self):
        """バケットが存在しない場合Falseを返す"""
        from google.cloud.exceptions import NotFound

        mock_client = MagicMock()
        mock_client.get_bucket.side_effect = NotFound("Bucket not found")

        with patch(
            "src.data.upload_to_gcs.storage.Client", return_value=mock_client
        ), patch(
            "src.data.upload_to_gcs.auth_default",
            return_value=(MagicMock(), "project"),
        ):
            uploader = GCSUploader(
                project_id="test-project",
                bucket_name="test-bucket",
            )

        assert uploader.verify_bucket_exists() is False


class TestGCSUploaderRetry:
    """GCSUploaderのリトライ処理テスト"""

    def test_upload_file_with_retry_success_first_attempt(self):
        """最初の試行で成功する場合"""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write("test data")
            temp_path = Path(f.name)

        try:
            mock_client = MagicMock()
            mock_bucket = MagicMock()
            mock_blob = MagicMock()
            mock_client.bucket.return_value = mock_bucket
            mock_bucket.blob.return_value = mock_blob

            with patch(
                "src.data.upload_to_gcs.storage.Client", return_value=mock_client
            ), patch(
                "src.data.upload_to_gcs.auth_default",
                return_value=(MagicMock(), "project"),
            ):
                uploader = GCSUploader(
                    project_id="test-project",
                    bucket_name="test-bucket",
                )

            result = uploader._upload_file_with_retry(temp_path, "test/file.csv")

            assert result is True
            mock_blob.upload_from_filename.assert_called_once()
        finally:
            temp_path.unlink()

    def test_upload_file_with_retry_success_after_retry(self):
        """リトライ後に成功する場合"""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write("test data")
            temp_path = Path(f.name)

        try:
            mock_client = MagicMock()
            mock_bucket = MagicMock()
            mock_blob = MagicMock()
            mock_client.bucket.return_value = mock_bucket
            mock_bucket.blob.return_value = mock_blob

            # 最初は失敗、2回目で成功
            mock_blob.upload_from_filename.side_effect = [
                Exception("Temporary error"),
                None,
            ]

            with patch(
                "src.data.upload_to_gcs.storage.Client", return_value=mock_client
            ), patch(
                "src.data.upload_to_gcs.auth_default",
                return_value=(MagicMock(), "project"),
            ), patch(
                "src.data.upload_to_gcs.time.sleep"
            ):
                uploader = GCSUploader(
                    project_id="test-project",
                    bucket_name="test-bucket",
                )

            result = uploader._upload_file_with_retry(temp_path, "test/file.csv")

            assert result is True
            assert mock_blob.upload_from_filename.call_count == 2
        finally:
            temp_path.unlink()

    def test_upload_file_with_retry_all_attempts_fail(self):
        """すべてのリトライが失敗する場合"""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write("test data")
            temp_path = Path(f.name)

        try:
            mock_client = MagicMock()
            mock_bucket = MagicMock()
            mock_blob = MagicMock()
            mock_client.bucket.return_value = mock_bucket
            mock_bucket.blob.return_value = mock_blob

            # すべて失敗
            mock_blob.upload_from_filename.side_effect = Exception("Permanent error")

            with patch(
                "src.data.upload_to_gcs.storage.Client", return_value=mock_client
            ), patch(
                "src.data.upload_to_gcs.auth_default",
                return_value=(MagicMock(), "project"),
            ), patch(
                "src.data.upload_to_gcs.time.sleep"
            ):
                uploader = GCSUploader(
                    project_id="test-project",
                    bucket_name="test-bucket",
                )

            result = uploader._upload_file_with_retry(
                temp_path, "test/file.csv", max_retries=3
            )

            assert result is False
            assert mock_blob.upload_from_filename.call_count == 3
        finally:
            temp_path.unlink()


class TestCreateUploaderFromEnv:
    """create_uploader_from_env関数のテスト"""

    def test_create_uploader_from_env_success(self):
        """環境変数から正常にUploaderを作成"""
        with patch.dict(
            os.environ, {"GCP_PROJECT_ID": "test-project", "GCS_BUCKET_RAW": "raw-data"}
        ), patch("src.data.upload_to_gcs.storage.Client"), patch(
            "src.data.upload_to_gcs.auth_default",
            return_value=(MagicMock(), "project"),
        ):
            uploader = create_uploader_from_env()

        assert uploader is not None
        assert uploader.project_id == "test-project"
        assert uploader.bucket_name == "test-project-raw-data"

    def test_create_uploader_from_env_default_bucket(self):
        """デフォルトのバケットサフィックスを使用"""
        with patch.dict(
            os.environ, {"GCP_PROJECT_ID": "test-project"}, clear=True
        ), patch("src.data.upload_to_gcs.storage.Client"), patch(
            "src.data.upload_to_gcs.auth_default",
            return_value=(MagicMock(), "project"),
        ):
            uploader = create_uploader_from_env()

        assert uploader is not None
        assert uploader.bucket_name == "test-project-keiba-raw-data"

    def test_create_uploader_from_env_missing_project_id(self):
        """プロジェクトIDがない場合はNoneを返す"""
        with patch.dict(os.environ, {}, clear=True):
            uploader = create_uploader_from_env()

        assert uploader is None

    def test_create_uploader_from_env_with_local_base_dir(self):
        """local_base_dirを指定してUploaderを作成"""
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict(
                os.environ, {"GCP_PROJECT_ID": "test-project"}
            ), patch("src.data.upload_to_gcs.storage.Client"), patch(
                "src.data.upload_to_gcs.auth_default",
                return_value=(MagicMock(), "project"),
            ):
                uploader = create_uploader_from_env(local_base_dir=Path(temp_dir))

            assert uploader is not None
            assert uploader.local_base_dir == Path(temp_dir)

    def test_create_uploader_from_env_auth_failure(self):
        """認証失敗時はNoneを返す"""
        from google.auth.exceptions import DefaultCredentialsError

        with patch.dict(os.environ, {"GCP_PROJECT_ID": "test-project"}), patch(
            "src.data.upload_to_gcs.auth_default"
        ) as mock_auth:
            mock_auth.side_effect = DefaultCredentialsError("No credentials")
            uploader = create_uploader_from_env()

        assert uploader is None
