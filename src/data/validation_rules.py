#!/usr/bin/env python3
"""
データ品質チェック用のバリデーションルール定義

Issue #8: データ品質チェックスクリプトの実装
"""

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional


class Severity(Enum):
    """チェック結果の重要度"""

    ERROR = "ERROR"  # 即座に対処が必要
    WARNING = "WARNING"  # 注意が必要
    INFO = "INFO"  # 情報のみ


@dataclass
class ValidationRule:
    """バリデーションルールの定義"""

    name: str  # ルール名
    description: str  # ルールの説明
    severity: Severity  # 重要度
    threshold: Optional[float] = None  # 閾値（必要な場合）


@dataclass
class TableValidationConfig:
    """テーブルごとのバリデーション設定"""

    dataset_id: str
    table_id: str
    description: str
    primary_key_columns: List[str]  # 重複チェック用のキー列
    not_null_columns: List[str]  # NULL不可の列
    date_columns: List[str]  # 日付範囲チェック用の列
    numeric_columns: List[str]  # 数値範囲チェック用の列
    expected_min_rows: int = 0  # 最低レコード数


# テーブルごとのバリデーション設定
TABLE_VALIDATION_CONFIGS = [
    TableValidationConfig(
        dataset_id="raw",
        table_id="race_info",
        description="レース情報テーブル",
        primary_key_columns=["race_id"],
        not_null_columns=["race_id", "race_date", "venue_code", "race_number"],
        date_columns=["race_date"],
        numeric_columns=["race_number", "distance", "num_horses"],
        expected_min_rows=1000,
    ),
    TableValidationConfig(
        dataset_id="raw",
        table_id="horse_results",
        description="競走馬成績テーブル",
        primary_key_columns=["race_id", "horse_id"],
        not_null_columns=["race_id", "horse_id"],
        date_columns=[],
        numeric_columns=["finish_position", "odds", "popularity"],
        expected_min_rows=10000,
    ),
    TableValidationConfig(
        dataset_id="raw",
        table_id="pedigree",
        description="血統テーブル",
        primary_key_columns=["horse_id"],
        not_null_columns=["horse_id"],
        date_columns=[],
        numeric_columns=[],
        expected_min_rows=1000,
    ),
    TableValidationConfig(
        dataset_id="raw",
        table_id="odds",
        description="オッズテーブル",
        primary_key_columns=["race_id", "horse_id", "odds_type", "odds_timestamp"],
        not_null_columns=["race_id", "horse_id", "odds_type"],
        date_columns=["odds_timestamp"],
        numeric_columns=["odds_value"],
        expected_min_rows=1000,
    ),
    TableValidationConfig(
        dataset_id="features",
        table_id="training_data",
        description="学習用特徴量テーブル",
        primary_key_columns=["race_id", "horse_id"],
        not_null_columns=["race_id", "horse_id", "race_date"],
        date_columns=["race_date"],
        numeric_columns=[],
        expected_min_rows=10000,
    ),
]

# 共通バリデーションルール
COMMON_VALIDATION_RULES = [
    ValidationRule(
        name="null_check",
        description="必須カラムのNULL値チェック",
        severity=Severity.ERROR,
    ),
    ValidationRule(
        name="duplicate_check",
        description="主キーの重複チェック",
        severity=Severity.ERROR,
    ),
    ValidationRule(
        name="row_count_check",
        description="レコード数の最低値チェック",
        severity=Severity.WARNING,
    ),
    ValidationRule(
        name="date_range_check",
        description="日付範囲の整合性チェック",
        severity=Severity.WARNING,
    ),
    ValidationRule(
        name="numeric_range_check",
        description="数値範囲の妥当性チェック",
        severity=Severity.INFO,
    ),
]

# 日付範囲の設定（JRDBデータの有効期間）
DATE_RANGE_CONFIG = {
    "min_date": "2016-01-01",  # データ開始日
    "max_future_days": 7,  # 未来日の許容日数
}

# 数値範囲の設定
NUMERIC_RANGE_CONFIG = {
    "race_number": {"min": 1, "max": 12},
    "distance": {"min": 800, "max": 4000},
    "num_horses": {"min": 1, "max": 18},
    "finish_position": {"min": 1, "max": 18},
    "odds": {"min": 1.0, "max": 10000.0},
    "popularity": {"min": 1, "max": 18},
    "odds_value": {"min": 1.0, "max": 100000.0},
}
