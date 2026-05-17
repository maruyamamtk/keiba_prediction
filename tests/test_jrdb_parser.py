"""
JRDBParser のユニットテスト

Issue #214: parse_baa_line が start_time を正しく返すことを検証する。
Issue #272: parse_cha_line が調教本追切データを正しく解析することを検証する。
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


# ---------------------------------------------------------------------------
# CHA (調教本追切データ) パーサーテスト (Issue #272)
# ---------------------------------------------------------------------------

def _make_cha_line(
    race_key: str = "06161101",
    horse_number: str = "01",
    day_of_week: str = "木",
    training_date: str = "20151231",
    training_count: str = "1",
    course_code: str = "02",
    intensity: str = "3",
    condition: str = "02",
    rider: str = "3",
    furlongs: str = "5",
    ten_f: str = "149",
    mid_f: str = "138",
    last_f: str = "133",
    ten_idx: str = " 16",
    mid_idx: str = " 13",
    last_idx: str = " 18",
    training_idx: str = " 45",
) -> str:
    """
    テスト用の CHA 固定長行を生成する。

    Shift-JIS 基準のフィールドレイアウト (Python char インデックス):
      [0:8]   レースキー
      [8:10]  馬番
      [10]    曜日 (全角1文字)
      [11:19] 調教年月日 (YYYYMMDD)
      [19]    回数
      [20:22] 調教コースコード
      [22]    追切種類
      [23:25] 追い状態
      [25]    乗り役
      [26]    調教F
      [27:30] テンF (ZZ9, 1/10秒単位)
      [30:33] 中間F
      [33:36] 終いF
      [36:39] テンF指数
      [39:42] 中間F指数
      [42:45] 終いF指数
      [45:48] 追切指数
      [48:63] 残余・改行
    """
    return (
        race_key        # [0:8]
        + horse_number  # [8:10]
        + day_of_week   # [10]  (全角1文字)
        + training_date # [11:19]
        + training_count# [19]
        + course_code   # [20:22]
        + intensity     # [22]
        + condition     # [23:25]
        + rider         # [25]
        + furlongs      # [26]
        + ten_f         # [27:30]
        + mid_f         # [30:33]
        + last_f        # [33:36]
        + ten_idx       # [36:39]
        + mid_idx       # [39:42]
        + last_idx      # [42:45]
        + training_idx  # [45:48]
        + " " * 15      # 残余パディング
    )


class TestParseChaLine:
    """parse_cha_line の各フィールドに関するテスト (Issue #272)"""

    def test_basic_parse(self):
        """サンプルデータが正常に解析できること"""
        line = _make_cha_line()
        result = JRDBParser.parse_cha_line(line)
        assert result is not None
        assert result["race_id"] == "06161101"
        assert result["horse_number"] == 1

    def test_training_date_parsed(self):
        """調教年月日が DATE 形式で取得されること"""
        line = _make_cha_line(training_date="20151231")
        result = JRDBParser.parse_cha_line(line)
        assert result is not None
        assert result["training_date"] == "2015-12-31"

    def test_lap_times_converted_to_seconds(self):
        """テンF・中間F・終いF が 1/10秒単位から秒に変換されること"""
        line = _make_cha_line(ten_f="149", mid_f="138", last_f="133")
        result = JRDBParser.parse_cha_line(line)
        assert result is not None
        assert result["ten_f_time"] == pytest.approx(14.9)
        assert result["middle_f_time"] == pytest.approx(13.8)
        assert result["last_3f_time"] == pytest.approx(13.3)

    def test_training_index_parsed(self):
        """追切指数が正しく取得されること"""
        line = _make_cha_line(training_idx=" 45")
        result = JRDBParser.parse_cha_line(line)
        assert result is not None
        assert result["training_index"] == 45

    def test_intensity_code_parsed(self):
        """追切種類 (1=一杯, 2=強目, 3=馬なり) が取得されること"""
        for code in ["1", "2", "3"]:
            line = _make_cha_line(intensity=code)
            result = JRDBParser.parse_cha_line(line)
            assert result is not None
            assert result["intensity_code"] == int(code)

    def test_course_code_parsed(self):
        """調教コースコードが文字列で取得されること"""
        line = _make_cha_line(course_code="11")
        result = JRDBParser.parse_cha_line(line)
        assert result is not None
        assert result["training_course_code"] == "11"

    def test_too_short_returns_none(self):
        """50文字未満の行は None を返すこと"""
        result = JRDBParser.parse_cha_line("06161101" + "01" + "木")
        assert result is None

    def test_real_sample_line(self):
        """実ファイルのサンプル行が正常に解析できること"""
        # downloaded_files/Cha/CHA160105.csv の1行目
        sample = "0616110101木201512311023023514913813316 13 18 4512 2A1       "
        result = JRDBParser.parse_cha_line(sample)
        assert result is not None
        assert result["race_id"] == "06161101"
        assert result["horse_number"] == 1
        assert result["training_date"] == "2015-12-31"
