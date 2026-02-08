#!/usr/bin/env python3
"""
BigQueryデータセットとテーブルを作成するスクリプト

Issue #4: BigQueryデータセットとテーブルの作成
"""

import json
import os
import sys
from pathlib import Path
from typing import List

from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


class BigQueryTableCreator:
    """BigQueryテーブル作成クラス"""

    def __init__(self, project_id: str):
        """
        初期化

        Args:
            project_id: GCPプロジェクトID
        """
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
        self.config_dir = Path(__file__).parent.parent.parent / "config"

    def create_dataset(self, dataset_id: str, location: str = "asia-northeast1", description: str = "") -> None:
        """
        データセットを作成

        Args:
            dataset_id: データセットID
            location: ロケーション
            description: データセットの説明
        """
        dataset_ref = f"{self.project_id}.{dataset_id}"

        try:
            # データセットが既に存在するか確認
            self.client.get_dataset(dataset_ref)
            print(f"✓ データセット {dataset_ref} は既に存在します。スキップします。")
            return
        except NotFound:
            pass

        # データセットを作成
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        dataset.description = description

        dataset = self.client.create_dataset(dataset, timeout=30)
        print(f"✓ データセット {dataset_ref} を作成しました。")

    def load_schema(self, schema_file: str) -> List[bigquery.SchemaField]:
        """
        スキーマファイルを読み込み

        Args:
            schema_file: スキーマファイル名

        Returns:
            BigQueryスキーマフィールドのリスト
        """
        schema_path = self.config_dir / schema_file

        if not schema_path.exists():
            raise FileNotFoundError(f"スキーマファイルが見つかりません: {schema_path}")

        with open(schema_path, "r", encoding="utf-8") as f:
            schema_json = json.load(f)

        schema = []
        for field in schema_json:
            schema.append(
                bigquery.SchemaField(
                    name=field["name"],
                    field_type=field["type"],
                    mode=field.get("mode", "NULLABLE"),
                    description=field.get("description", ""),
                )
            )

        return schema

    def create_table(
        self,
        dataset_id: str,
        table_id: str,
        schema_file: str,
        partition_field: str = None,
        clustering_fields: List[str] = None,
        description: str = "",
    ) -> None:
        """
        テーブルを作成

        Args:
            dataset_id: データセットID
            table_id: テーブルID
            schema_file: スキーマファイル名
            partition_field: パーティションフィールド名
            clustering_fields: クラスタリングフィールド名のリスト
            description: テーブルの説明
        """
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"

        try:
            # テーブルが既に存在するか確認
            self.client.get_table(table_ref)
            print(f"✓ テーブル {table_ref} は既に存在します。スキップします。")
            return
        except NotFound:
            pass

        # スキーマを読み込み
        schema = self.load_schema(schema_file)

        # テーブルを作成
        table = bigquery.Table(table_ref, schema=schema)
        table.description = description

        # パーティショニング設定
        if partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field,
            )

        # クラスタリング設定
        if clustering_fields:
            table.clustering_fields = clustering_fields

        table = self.client.create_table(table)
        print(f"✓ テーブル {table_ref} を作成しました。")

    def create_all_datasets(self) -> None:
        """すべてのデータセットを作成"""
        print("=" * 60)
        print("BigQueryデータセットを作成しています...")
        print("=" * 60)

        datasets = [
            {
                "id": "raw",
                "description": "JRDBから取得した生データ",
            },
            {
                "id": "features",
                "description": "機械学習用の特徴量データ",
            },
            {
                "id": "predictions",
                "description": "予測結果データ",
            },
            {
                "id": "backtests",
                "description": "バックテスト結果データ",
            },
        ]

        for dataset in datasets:
            self.create_dataset(
                dataset_id=dataset["id"],
                description=dataset["description"],
            )

        print()

    def create_all_tables(self) -> None:
        """すべてのテーブルを作成"""
        print("=" * 60)
        print("BigQueryテーブルを作成しています...")
        print("=" * 60)

        tables = [
            # rawデータセット
            {
                "dataset_id": "raw",
                "table_id": "race_info",
                "schema_file": "bq_schema_race_info.json",
                "partition_field": "race_date",
                "clustering_fields": ["venue_code", "race_number"],
                "description": "レース情報 (BAA: 番組データ)",
            },
            {
                "dataset_id": "raw",
                "table_id": "horse_results",
                "schema_file": "bq_schema_horse_results.json",
                "partition_field": None,  # race_idからパーティションを抽出する必要があるため、後で設定
                "clustering_fields": ["horse_id", "jockey_id", "trainer_id"],
                "description": "競走馬成績データ (KYF: 競走馬データ + 過去成績)",
            },
            {
                "dataset_id": "raw",
                "table_id": "pedigree",
                "schema_file": "bq_schema_pedigree.json",
                "partition_field": None,
                "clustering_fields": ["sire_id", "dam_sire_id"],
                "description": "血統データ",
            },
            {
                "dataset_id": "raw",
                "table_id": "odds",
                "schema_file": "bq_schema_odds.json",
                "partition_field": "odds_timestamp",
                "clustering_fields": ["race_id", "horse_id", "odds_type"],
                "description": "オッズデータ",
            },
            # featuresデータセット
            {
                "dataset_id": "features",
                "table_id": "training_data",
                "schema_file": "bq_schema_training_data.json",
                "partition_field": "race_date",
                "clustering_fields": ["venue_code", "horse_id"],
                "description": "機械学習用の特徴量データ",
            },
        ]

        for table in tables:
            self.create_table(
                dataset_id=table["dataset_id"],
                table_id=table["table_id"],
                schema_file=table["schema_file"],
                partition_field=table.get("partition_field"),
                clustering_fields=table.get("clustering_fields"),
                description=table["description"],
            )

        print()

    def verify_setup(self) -> None:
        """セットアップの検証"""
        print("=" * 60)
        print("セットアップを検証しています...")
        print("=" * 60)

        # データセットの一覧
        datasets = list(self.client.list_datasets())
        if datasets:
            print(f"\n作成されたデータセット ({len(datasets)}個):")
            for dataset in datasets:
                dataset_id = dataset.dataset_id
                # テーブル数を取得
                tables = list(self.client.list_tables(f"{self.project_id}.{dataset_id}"))
                print(f"  - {dataset_id}: {len(tables)}テーブル")
        else:
            print("データセットが見つかりません。")

        print("\n✓ セットアップが完了しました！")


def main():
    """メイン処理"""
    # .envファイルを読み込み
    load_dotenv()

    # 環境変数からプロジェクトIDを取得
    project_id = os.getenv("GCP_PROJECT_ID")

    if not project_id:
        print("エラー: GCP_PROJECT_ID環境変数が設定されていません。")
        print(".envファイルを作成し、GCP_PROJECT_IDを設定してください。")
        sys.exit(1)

    print(f"GCPプロジェクトID: {project_id}\n")

    # BigQueryテーブル作成インスタンスを作成
    creator = BigQueryTableCreator(project_id)

    try:
        # データセットを作成
        creator.create_all_datasets()

        # テーブルを作成
        creator.create_all_tables()

        # セットアップを検証
        creator.verify_setup()

    except Exception as e:
        print(f"\nエラーが発生しました: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
