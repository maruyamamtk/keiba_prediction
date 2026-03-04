#!/usr/bin/env python3
"""
JRDBダウンローダーモジュール

JRDBからデータをダウンロードし、解凍・エンコーディング変換を行う。
Cloud Run環境での実行を想定し、環境変数から認証情報を取得する。

Issue #53: JRDBダウンローダーのコンテナ化
"""

import logging
import os
import re
import shutil
import subprocess
import tempfile
import http
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# CSA/KSAはCSVファイルとして直接ダウンロード可能
CSV_DATATYPES = {"CSA", "KSA"}


@dataclass
class DownloadResult:
    """ダウンロード結果を格納するデータクラス"""

    total_files: int
    downloaded_files: int
    skipped_files: int
    failed_files: int


class JRDBDownloader:
    """JRDBからデータをダウンロードするクラス"""

    JRDB_BASE_URL = "http://www.jrdb.com/member/data/"
    JRDB_DATAINDEX_URL = "http://www.jrdb.com/member/dataindex.html"

    def __init__(
        self,
        username: str,
        password: str,
        output_dir: Optional[Path] = None,
    ):
        """
        初期化

        Args:
            username: JRDB認証ユーザー名
            password: JRDBパスワード
            output_dir: ダウンロード先ディレクトリ（デフォルト: downloaded_files/）
        """
        self.username = username
        self.password = password

        if output_dir is None:
            self.output_dir = Path(__file__).resolve().parent.parent.parent.parent / "downloaded_files"
        else:
            self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self._setup_auth_handler()

    def _setup_auth_handler(self) -> None:
        """Basic認証ハンドラーをセットアップ"""
        password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        password_mgr.add_password(None, self.JRDB_BASE_URL, self.username, self.password)
        password_mgr.add_password(
            None, self.JRDB_DATAINDEX_URL, self.username, self.password
        )
        handler = urllib.request.HTTPBasicAuthHandler(password_mgr)
        opener = urllib.request.build_opener(handler)
        urllib.request.install_opener(opener)

    @staticmethod
    def datatype_to_folder(datatype: str) -> str:
        """
        データタイプからフォルダ名を生成

        例: KAA -> Kaa, BAA -> Baa
        例外: CSA -> Cs, KSA -> Ks
        """
        if datatype in CSV_DATATYPES:
            return datatype[0] + datatype[1].lower()
        return datatype[0] + datatype[1:].lower()

    @staticmethod
    def get_extension(datatype: str) -> str:
        """データタイプに応じた拡張子を返す"""
        return ".csv" if datatype in CSV_DATATYPES else ".lzh"

    def get_available_datatypes(self) -> list[str]:
        """
        dataindex.htmlから利用可能なデータタイプを取得

        Returns:
            データタイプのリスト
        """
        try:
            response = urllib.request.urlopen(self.JRDB_DATAINDEX_URL, timeout=30)
            html = response.read().decode("utf-8", errors="ignore")
        except Exception as e:
            logger.error(f"dataindex.htmlの取得に失敗: {e}")
            return []

        # 各行からデータタイプを抽出
        rows = re.findall(r"<tr>.*?</tr>", html, re.DOTALL)
        datatypes = []

        for row in rows:
            links = re.findall(r'<a href="[^"]*">([A-Z0-9]+)</a>', row)
            if links:
                latest = links[-1]
                if latest not in datatypes:
                    datatypes.append(latest)

        return datatypes

    def get_available_dates(self, datatype: str) -> list[str]:
        """
        指定データタイプの利用可能な日付リストを取得

        Args:
            datatype: データタイプ（例: KAA）

        Returns:
            日付（yymmdd形式）のリスト
        """
        folder = self.datatype_to_folder(datatype)
        url = f"{self.JRDB_BASE_URL}{folder}/"

        try:
            response = urllib.request.urlopen(url, timeout=30)
            html = response.read().decode("utf-8", errors="ignore")
        except urllib.error.HTTPError as e:
            if e.code == http.HTTPStatus.FORBIDDEN:
                logger.warning(f"{url} へのアクセスが禁止されています（403 Forbidden）。このデータタイプは利用できません。")
            else:
                logger.error(f"{url}の取得に失敗: {e}")
            return []
        except Exception as e:
            logger.error(f"{url}の取得に失敗: {e}")
            return []

        extension = "csv" if datatype in CSV_DATATYPES else "lzh"
        pattern = rf"{datatype}(\d{{6}})\.{extension}"
        matches = re.findall(pattern, html, re.IGNORECASE)

        return sorted(set(matches))

    def _download_file(self, datatype: str, filedate: str) -> Optional[Path]:
        """
        ファイルをダウンロード

        Args:
            datatype: データタイプ
            filedate: ファイル日付（yymmdd）

        Returns:
            ダウンロードしたファイルのパス（失敗時はNone）
        """
        folder = self.datatype_to_folder(datatype)
        ext = self.get_extension(datatype)
        filename = f"{datatype}{filedate}{ext}"
        url = f"{self.JRDB_BASE_URL}{folder}/{filename}"

        # ディレクトリ作成
        output_folder = self.output_dir / folder
        output_folder.mkdir(parents=True, exist_ok=True)

        output_path = output_folder / filename

        try:
            logger.info(f"ダウンロード中: {filename}")
            urllib.request.urlretrieve(url, str(output_path))
            return output_path
        except urllib.error.HTTPError as e:
            if e.code == http.HTTPStatus.FORBIDDEN:
                logger.warning(f"ダウンロード失敗 {filename}: アクセスが禁止されています（403 Forbidden）。")
            else:
                logger.error(f"ダウンロード失敗 {filename}: {e}")
            return None
        except Exception as e:
            logger.error(f"ダウンロード失敗 {filename}: {e}")
            return None

    def _decompress_lzh(self, lzh_path: Path, output_dir: Path) -> bool:
        """
        lzhファイルを解凍

        Args:
            lzh_path: lzhファイルのパス
            output_dir: 解凍先ディレクトリ

        Returns:
            成功した場合True
        """
        try:
            # lhasaコマンドを使用（Linuxの場合）
            result = subprocess.run(
                ["lha", "-xw=" + str(output_dir), str(lzh_path)],
                capture_output=True,
                text=True,
                timeout=60,
            )
            if result.returncode != 0:
                # p7zipを試す
                result = subprocess.run(
                    ["7z", "x", "-y", f"-o{output_dir}", str(lzh_path)],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
            return result.returncode == 0
        except subprocess.TimeoutExpired:
            logger.error(f"解凍タイムアウト: {lzh_path}")
            return False
        except FileNotFoundError:
            logger.error("lhaまたは7zコマンドが見つかりません")
            return False
        except Exception as e:
            logger.error(f"解凍失敗 {lzh_path}: {e}")
            return False

    def _convert_encoding(self, file_path: Path) -> bool:
        """
        CP932からUTF-8にエンコーディング変換

        Args:
            file_path: 変換対象ファイル

        Returns:
            成功した場合True
        """
        try:
            with open(file_path, "rb") as f:
                content = f.read()

            # CP932からUTF-8に変換
            decoded = content.decode("cp932")
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(decoded)

            return True
        except Exception as e:
            logger.error(f"エンコーディング変換失敗 {file_path}: {e}")
            return False

    def _process_downloaded_file(
        self, datatype: str, filedate: str, downloaded_path: Path
    ) -> bool:
        """
        ダウンロードしたファイルを処理（解凍、エンコーディング変換）

        Args:
            datatype: データタイプ
            filedate: ファイル日付
            downloaded_path: ダウンロードしたファイルのパス

        Returns:
            成功した場合True
        """
        folder = self.datatype_to_folder(datatype)
        output_folder = self.output_dir / folder

        if datatype in CSV_DATATYPES:
            # CSVファイルの場合はエンコーディング変換のみ
            return self._convert_encoding(downloaded_path)

        # lzhファイルの解凍
        if not self._decompress_lzh(downloaded_path, output_folder):
            return False

        # 解凍後のlzhファイルを削除
        downloaded_path.unlink(missing_ok=True)

        # バックアップディレクトリを作成
        backup_dir = output_folder / "txt_backup"
        backup_dir.mkdir(exist_ok=True)

        # 解凍されたtxtファイルのエンコーディング変換
        for txt_file in output_folder.glob("*.txt"):
            if not self._convert_encoding(txt_file):
                continue

            # 元のtxtファイルをバックアップディレクトリにコピー
            shutil.copy(txt_file, backup_dir / txt_file.name)

            # 拡張子を.csvに変更（固定長フォーマットは維持）
            csv_file = txt_file.with_suffix(".csv")
            txt_file.rename(csv_file)

        return True

    def download_single(self, datatype: str, filedate: str) -> bool:
        """
        単一ファイルをダウンロード・処理

        Args:
            datatype: データタイプ
            filedate: ファイル日付（yymmdd）

        Returns:
            成功した場合True
        """
        folder = self.datatype_to_folder(datatype)
        csv_path = self.output_dir / folder / f"{datatype}{filedate}.csv"

        # 既にダウンロード済みならスキップ
        if csv_path.exists():
            logger.info(f"スキップ（既存）: {datatype}{filedate}")
            return True

        # ダウンロード
        downloaded_path = self._download_file(datatype, filedate)
        if downloaded_path is None:
            return False

        # 処理（解凍、エンコーディング変換）
        return self._process_downloaded_file(datatype, filedate, downloaded_path)

    def download_from_date(
        self,
        datatype: str,
        start_date: str,
        end_date: Optional[str] = None,
    ) -> DownloadResult:
        """
        指定日付範囲のファイルをダウンロード

        Args:
            datatype: データタイプ
            start_date: 開始日付（yymmdd形式）
            end_date: 終了日付（yymmdd形式、省略時は上限なし）

        Returns:
            ダウンロード結果
        """
        if end_date and start_date > end_date:
            raise ValueError(
                f"start_date ({start_date}) が end_date ({end_date}) より後の日付です"
            )

        logger.info(f"利用可能な日付を取得中: {datatype}")
        available_dates = self.get_available_dates(datatype)

        if not available_dates:
            logger.warning(f"利用可能なファイルがありません: {datatype}")
            return DownloadResult(
                total_files=0, downloaded_files=0, skipped_files=0, failed_files=0
            )

        # 日付範囲でフィルタリング
        # 90以上のyyは1990年代なので除外
        target_dates = []
        for date in available_dates:
            yy = int(date[:2])
            if yy >= 90:
                continue
            if date < start_date:
                continue
            if end_date and date > end_date:
                continue
            target_dates.append(date)

        total = len(target_dates)
        downloaded = 0
        skipped = 0
        failed = 0

        logger.info(f"ダウンロード対象: {total}ファイル ({datatype})")

        folder = self.datatype_to_folder(datatype)
        for date in target_dates:
            csv_path = self.output_dir / folder / f"{datatype}{date}.csv"

            if csv_path.exists():
                skipped += 1
                continue

            # download_singleはスキップ判定済みなので直接ダウンロード処理を呼ぶ
            downloaded_path = self._download_file(datatype, date)
            if downloaded_path is None:
                failed += 1
                continue

            if self._process_downloaded_file(datatype, date, downloaded_path):
                downloaded += 1
            else:
                failed += 1

        return DownloadResult(
            total_files=total,
            downloaded_files=downloaded,
            skipped_files=skipped,
            failed_files=failed,
        )

    def download_all_from_date(
        self,
        start_date: str,
        end_date: Optional[str] = None,
    ) -> dict[str, DownloadResult]:
        """
        すべてのデータタイプについて指定日付範囲のファイルをダウンロード

        Args:
            start_date: 開始日付（yymmdd形式）
            end_date: 終了日付（yymmdd形式、省略時は上限なし）

        Returns:
            データタイプごとのダウンロード結果
        """
        datatypes = self.get_available_datatypes()
        if not datatypes:
            logger.error("データタイプの取得に失敗しました")
            return {}

        logger.info(f"利用可能なデータタイプ: {', '.join(datatypes)}")

        results = {}
        for datatype in datatypes:
            logger.info(f"処理中: {datatype}")
            results[datatype] = self.download_from_date(datatype, start_date, end_date)

        return results

    def get_output_dir(self) -> Path:
        """出力ディレクトリを取得"""
        return self.output_dir

    def cleanup(self) -> None:
        """一時ディレクトリを削除"""
        if self.output_dir.exists() and str(self.output_dir).startswith(
            tempfile.gettempdir()
        ):
            shutil.rmtree(self.output_dir, ignore_errors=True)
            logger.info(f"一時ディレクトリを削除: {self.output_dir}")


def create_downloader_from_env() -> Optional[JRDBDownloader]:
    """
    環境変数からJRDBDownloaderを作成

    環境変数:
        JRDB_USER: JRDBユーザー名
        JRDB_PASSWORD: JRDBパスワード
        JRDB_OUTPUT_DIR: 出力ディレクトリ（オプション）

    Returns:
        JRDBDownloaderインスタンス（認証情報がない場合はNone）
    """
    username = os.environ.get("JRDB_USER")
    password = os.environ.get("JRDB_PASSWORD")

    if not username or not password:
        logger.error("JRDB_USER または JRDB_PASSWORD 環境変数が設定されていません")
        return None

    output_dir = os.environ.get("JRDB_OUTPUT_DIR")
    if output_dir:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
    else:
        output_path = None

    return JRDBDownloader(username, password, output_path)


def main():
    """メイン処理（CLIからの実行用）"""
    import argparse

    from dotenv import load_dotenv

    # ロギング設定
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(description="JRDBダウンローダー")
    parser.add_argument(
        "--start-date",
        type=str,
        required=True,
        help="開始日付（yymmdd形式、例: 240101）",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="終了日付（yymmdd形式、例: 241231）。省略時は上限なし",
    )
    parser.add_argument(
        "--datatype",
        type=str,
        default=None,
        help="データタイプ（例: KAA）。指定しない場合はすべて",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="出力ディレクトリ",
    )
    args = parser.parse_args()

    # .envを読み込み
    load_dotenv()

    # 環境変数を設定
    if args.output_dir:
        os.environ["JRDB_OUTPUT_DIR"] = args.output_dir

    downloader = create_downloader_from_env()
    if not downloader:
        return 1

    try:
        if args.datatype:
            result = downloader.download_from_date(
                args.datatype.upper(), args.start_date, args.end_date
            )
            print(f"\n結果: {args.datatype}")
            print(f"  総ファイル数: {result.total_files}")
            print(f"  ダウンロード: {result.downloaded_files}")
            print(f"  スキップ: {result.skipped_files}")
            print(f"  失敗: {result.failed_files}")
        else:
            results = downloader.download_all_from_date(args.start_date, args.end_date)
            print("\n=== 結果サマリー ===")
            total = DownloadResult(0, 0, 0, 0)
            for datatype, result in results.items():
                print(f"{datatype}: DL={result.downloaded_files}, "
                      f"Skip={result.skipped_files}, Fail={result.failed_files}")
                total = DownloadResult(
                    total.total_files + result.total_files,
                    total.downloaded_files + result.downloaded_files,
                    total.skipped_files + result.skipped_files,
                    total.failed_files + result.failed_files,
                )
            print(f"\n合計: DL={total.downloaded_files}, "
                  f"Skip={total.skipped_files}, Fail={total.failed_files}")

        print(f"\n出力ディレクトリ: {downloader.get_output_dir()}")
        return 0

    except Exception as e:
        logger.error(f"エラーが発生しました: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
