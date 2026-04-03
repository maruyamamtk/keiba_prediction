"""
JRDBParser のユニットテスト

Issue #214: parse_baa_line が start_time を正しく返すことを検証する。
"""

import sys
from pathlib import Path

import pytest

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

from src.automation.data.jrdb_parser import JRDBParser


def _make_baa_line(start_time: str = "1015") -> str:
    """
    テスト用の BAA 固定長行を生成する。

    BAAフォーマット（抜粋）:
      [0:8]   レースキー (場コード2 + 年2 + 回1 + 日1 + R2)
      [8:16]  日付 (YYYYMMDD)
      [16:20] 発走時刻 (HHMM)
      [20:24] 距離
      [24:25] 芝ダ障害コード
      [25:26] 右左
      [26:27] 内外
      [27:29] 種別コード
      [29:31] 条件コード
      [31:34] 記号
      [34:35] 重量種別
      [35:36] グレード
      [36:86] レース名 (50バイト)
      [86:]   残余（頭数など）
    """
    race_key = "05260101"        # 東京26年1回1日1R
    race_date = "20260104"       # 2026-01-04
    distance = "1600"
    course_type = "1"            # 芝
    direction = "2"              # 左
    inner_outer = "1"
    age_condition = "13"         # 3歳以上
    race_condition = "OP"
    symbol = "   "               # 3バイト
    weight_type = "4"            # 定量
    grade = " "
    race_name = "TEST RACE" + " " * 41  # 50バイト

    line = (
        race_key        # 0:8
        + race_date     # 8:16
        + start_time    # 16:20
        + distance      # 20:24
        + course_type   # 24:25
        + direction     # 25:26
        + inner_outer   # 26:27
        + age_condition # 27:29
        + race_condition# 29:31
        + symbol        # 31:34
        + weight_type   # 34:35
        + grade         # 35:36
        + race_name     # 36:86
        + " " * 10      # 86: 残余パディング（90バイト以上を確保）
    )
    return line


class TestParseBaaLineStartTime:
    """parse_baa_line の start_time フィールドに関するテスト"""

    def test_start_time_present(self):
        """発走時刻が存在する行で start_time が正しく取得できること"""
        line = _make_baa_line(start_time="1015")
        result = JRDBParser.parse_baa_line(line)
        assert result is not None
        assert result["start_time"] == "1015"

    def test_start_time_empty_returns_none(self):
        """発走時刻が空白の行で start_time が None になること"""
        line = _make_baa_line(start_time="    ")
        result = JRDBParser.parse_baa_line(line)
        assert result is not None
        assert result["start_time"] is None

    def test_start_time_key_exists_in_return_dict(self):
        """return dict に start_time キーが含まれること"""
        line = _make_baa_line(start_time="1530")
        result = JRDBParser.parse_baa_line(line)
        assert result is not None
        assert "start_time" in result

    def test_start_time_various_values(self):
        """様々な発走時刻が正しく取得できること"""
        for hhmm in ["1000", "1200", "1530", "1645"]:
            line = _make_baa_line(start_time=hhmm)
            result = JRDBParser.parse_baa_line(line)
            assert result is not None, f"start_time={hhmm} のパースに失敗"
            assert result["start_time"] == hhmm, f"期待値: {hhmm}, 実際: {result['start_time']}"

    def test_line_too_short_returns_none(self):
        """90バイト未満の行は None を返すこと"""
        result = JRDBParser.parse_baa_line("05260101" + "20260104" + "1015")
        assert result is None
