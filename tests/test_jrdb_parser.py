"""
JRDBParser のユニットテスト

Issue #214: parse_baa_line が start_time を正しく返すことを検証する。
Issue #272: parse_cha_line が調教本追切データを正しく解析することを検証する。
Issue #290: parse_kyf_line が base_popularity を10+人気でも正しく返すことを検証する。
Issue #446: parse_sec_line が corner_position_1〜4 を正しく返し、他フィールドを変えないことを検証する。
Issue #441: parse_kyf_line が blinker を仕様位置（文字位置152）から読むことを検証する。
Issue #452: parse_kyf_line の各フィールドが JRDB KYI仕様のバイト位置どおりに読まれることを検証する。
Issue #450: parse_sec_line が馬体重増減の「- 2」形式（符号と数字の間に空白）を負の値として読むことを検証する。
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


def _make_kyf_line(odds_section: str = " 2.3 1  1.3 1  5 ") -> str:
    """
    テスト用の KYF 固定長行を生成する。

    KYFフォーマット（抜粋）:
      [0:8]   レースキー
      [8:10]  馬番
      [10:18] 血統登録番号
      [18:36] 馬名 (全角18文字スロット)
      [36:77] 各種指数・脚質など
      [77:82] 基準オッズ (5文字 "ZZ9.9"。ここでは先頭1文字を指数側の空白で埋める)
      [82:84] 基準人気 (2文字 " N" or "NN")
      [84:89] 基準複勝オッズ (5文字)
      [89:91] 基準複勝人気 (2文字)
      [91:]   残余
    """
    return (
        "06263301"   # [0:8]  race_key
        + "01"       # [8:10] horse_number
        + "00000000" # [10:18] horse_id
        + "A" * 18   # [18:36] horse_name (ASCIIでパディング)
        + " " * 42   # [36:78] 指数フィールド群
        + odds_section  # [78:95] オッズ・人気セクション
        + " " * 350  # 残余パディング
    )


class TestParseKyfLineBasePopularity:
    """parse_kyf_line の base_popularity フィールドに関するテスト (Issue #290)"""

    def test_single_digit_popularity(self):
        """1〜9番人気が正しく解析されること"""
        # " 1" → 1番人気、odds=2.3、place_odds=1.3
        line = _make_kyf_line(" 2.3 1  1.3 1  5 ")
        result = JRDBParser.parse_kyf_line(line)
        assert result is not None
        assert result["base_popularity"] == 1
        assert result["base_odds"] == pytest.approx(2.3)

    def test_ninth_popularity(self):
        """9番人気が正しく解析されること"""
        line = _make_kyf_line("48.3 9  9.0 9  0 ")
        result = JRDBParser.parse_kyf_line(line)
        assert result is not None
        assert result["base_popularity"] == 9
        assert result["base_odds"] == pytest.approx(48.3)

    def test_tenth_popularity(self):
        """10番人気が正しく解析されること（旧コードでは0と誤解析）"""
        # 実ファイルのデータ: horse 10 (10th pop, 67.2 odds)
        line = _make_kyf_line("67.210 11.910  0 ")
        result = JRDBParser.parse_kyf_line(line)
        assert result is not None
        assert result["base_popularity"] == 10, (
            "10番人気馬は base_popularity=10 でなければならない（旧バグでは0）"
        )
        assert result["base_odds"] == pytest.approx(67.2)
        assert result["base_place_popularity"] == 10

    def test_fifteenth_popularity(self):
        """15番人気が正しく解析されること（旧コードでは5と誤解析）"""
        line = _make_kyf_line("77.915 30.415  0 ")
        result = JRDBParser.parse_kyf_line(line)
        assert result is not None
        assert result["base_popularity"] == 15, (
            "15番人気馬は base_popularity=15 でなければならない（旧バグでは5）"
        )
        assert result["base_place_popularity"] == 15

    def test_sixteenth_popularity(self):
        """16番人気が正しく解析されること（旧コードでは6と誤解析）"""
        line = _make_kyf_line("84.016 31.416  0 ")
        result = JRDBParser.parse_kyf_line(line)
        assert result is not None
        assert result["base_popularity"] == 16
        assert result["base_odds"] == pytest.approx(84.0)
        assert result["base_place_odds"] == pytest.approx(31.4)
        assert result["base_place_popularity"] == 16

    def test_place_odds_not_collide_with_popularity(self):
        """base_place_odds が base_place_popularity と重複しないこと"""
        # horse 08: place_odds=1.3, place_pop=1
        line = _make_kyf_line(" 2.3 1  1.3 1  5 ")
        result = JRDBParser.parse_kyf_line(line)
        assert result is not None
        assert result["base_place_odds"] == pytest.approx(1.3)
        assert result["base_place_popularity"] == 1


def _make_sec_line(
    race_key: str = "06263309",
    horse_num: str = "01",
    finish_pos: str = "01",
    abnormal_code: str = "0",
    win_popularity: str = " 5",
    abbr: str = "　　　　",
) -> str:
    """
    テスト用の SEC 固定長行を生成する。

    SECフォーマット（抜粋）:
      [0:8]    レースキー
      [8:10]   馬番
      [10:18]  血統登録番号
      [18:26]  日付
      [26:44]  馬名 (全角18文字スロット)
      [44:87]  距離・コース等 (43文字)
      [87:89]  頭数 (2文字)
      [89:93]  略称 (全角4文字 = CP932 8バイト基準)
      [93:95]  着順
      [95:96]  異常コード
      [96:115] タイム・斤量等 (19文字)
      [115:121] win_odds (6文字)
      [121:123] win_popularity (2文字)
      [123:]   残余
    """
    return (
        race_key      # [0:8]
        + horse_num   # [8:10]
        + "00000000"  # [10:18] horse_id
        + "20260404"  # [18:26] race_date
        + "A" * 18    # [26:44] horse_name
        + " " * 43    # [44:87] 距離・コース等
        + "15"        # [87:89] num_horses
        + abbr        # [89:93] 略称 (全角4文字がデフォルト = CP932 8バイト)
        + finish_pos  # [93:95]
        + abnormal_code  # [95:96]
        + " " * 19    # [96:115] タイム・斤量等
        + "      "    # [115:121] win_odds (空白)
        + win_popularity  # [121:123]
        + " " * 152   # 残余パディング（200文字以上確保）
    )


class TestParseSecLineWinPopularity:
    """parse_sec_line の win_popularity フィールドに関するテスト"""

    def test_normal_popularity_returned(self):
        """1〜18番人気は正常に返されること"""
        line = _make_sec_line(finish_pos="05", win_popularity=" 5")
        result = JRDBParser.parse_sec_line(line)
        assert result is not None
        assert result["win_popularity"] == 5

    def test_cancelled_horse_sentinel_99_returns_none(self):
        """取消馬の JRDB センチネル値 99 は None に変換されること"""
        line = _make_sec_line(
            finish_pos="00", abnormal_code="2", win_popularity="99"
        )
        result = JRDBParser.parse_sec_line(line)
        assert result is not None
        assert result["finish_position"] == 0
        assert result["win_popularity"] is None, (
            "取消馬の win_popularity=99 は None でなければならない"
        )

    def test_value_above_18_returns_none(self):
        """18 超の値は無効なため None を返すこと"""
        line = _make_sec_line(win_popularity="99")
        result = JRDBParser.parse_sec_line(line)
        assert result is not None
        assert result["win_popularity"] is None

    def test_max_valid_popularity_18(self):
        """18番人気（最大頭数）は正常に返されること"""
        line = _make_sec_line(win_popularity="18")
        result = JRDBParser.parse_sec_line(line)
        assert result is not None
        assert result["win_popularity"] == 18

    def test_zero_popularity_returns_none(self):
        """取消馬が win_popularity=0 を持つ場合も None に変換されること"""
        line = _make_sec_line(finish_pos="00", abnormal_code="2", win_popularity=" 0")
        result = JRDBParser.parse_sec_line(line)
        assert result is not None
        assert result["win_popularity"] is None, (
            "win_popularity=0 は無効値（1未満）のため None でなければならない"
        )


def _make_sec_line_with_offset(
    abbr: str,
    finish_pos: str,
    abnormal_code: str = "0",
    win_popularity: str = " 5",
    race_key: str = "99999901",
    horse_num: str = "01",
    race_date: str = "20260531",
) -> str:
    """略称フィールドが可変長の場合の SEC 固定長行生成ヘルパー。

    abbr に半角文字が含まれる場合、finish_pos は abbr の直後に配置される。
    これは実際の SEC ファイルのバイト構造（CP932 8バイト固定）を反映している。
    """
    prefix = (
        race_key           # [0:8]
        + horse_num        # [8:10]
        + "00000000"       # [10:18] horse_id
        + race_date        # [18:26] race_date
        + "A" * 18         # [26:44] horse_name
        + " " * 43         # [44:87] 距離等
        + "15"             # [87:89] num_horses
        + abbr             # [89:89+len(abbr)] 略称 (CP932 8バイト相当)
        + finish_pos       # finish_position
        + abnormal_code    # abnormal_code
        + " " * 19         # タイム等
        + "      "         # win_odds
        + win_popularity   # win_popularity
    )
    return prefix + " " * max(200 - len(prefix), 100)


class TestParseSecLineAbbreviationOffset:
    """略称フィールドの半角スペース混入によるオフセットずれバグの修正テスト。

    Issue #323: 薫風ステークス等で全角3文字+半角スペース2個の略称が使われ、
    finish_position のオフセットが1文字ずれて着順が 0/1 バイナリになる問題。
    """

    def test_fullwidth_abbreviation_finish_position_correct(self):
        """全角4文字の略称では着順が正しく取得できること（正常ケース）"""
        abbr = "東京優駿"  # 全角4文字 = CP932 8バイト = UTF-8 4文字
        line = _make_sec_line_with_offset(abbr=abbr, finish_pos="01")
        result = JRDBParser.parse_sec_line(line)
        assert result is not None
        assert result["finish_position"] == 1

    def test_halfwidth_space_in_abbreviation_finish_position_correct(self):
        """略称フィールドに半角スペースが混入しても着順が正しく取得できること（バグ再現ケース）。

        薫風Ｓ(全角3文字=6バイト) + 半角スペース2個(2バイト) = 8バイト = 5 UTF-8文字。
        修正前: line[93:95]が半角スペース+着順十の位 → safe_int(' 0')=0 になる。
        修正後: CP932バイト幅を動的計算して正しいオフセットで着順を取得。
        """
        abbr = "薫風Ｓ" + "  "  # 全角3文字 + 半角スペース2個 = 8 CP932バイト = 5 UTF-8文字
        line = _make_sec_line_with_offset(
            abbr=abbr,
            finish_pos="01",
            race_key="05262c09",
            race_date="20260531",
        )
        result = JRDBParser.parse_sec_line(line)
        assert result is not None
        assert result["finish_position"] == 1, (
            "略称フィールドの半角スペースによるオフセットずれで "
            "finish_position が誤記録されてはならない（Issue #323）"
        )

    def test_halfwidth_space_abbreviation_rank_10_or_higher_correct(self):
        """半角スペース混入略称で10着以上の馬の着順も正しく取得できること"""
        abbr = "薫風Ｓ" + "  "
        line = _make_sec_line_with_offset(abbr=abbr, finish_pos="12")
        result = JRDBParser.parse_sec_line(line)
        assert result is not None
        assert result["finish_position"] == 12, (
            "10着以上の馬も finish_position が正しく返されること"
        )

    def test_fullwidth_abbreviation_rank_15_correct(self):
        """全角略称で15着の着順が正しく取得できること"""
        abbr = "　　　　"  # 全角スペース4文字
        line = _make_sec_line_with_offset(abbr=abbr, finish_pos="15")
        result = JRDBParser.parse_sec_line(line)
        assert result is not None
        assert result["finish_position"] == 15

    def test_halfwidth_katakana_in_abbreviation_correct(self):
        """半角カタカナを含む略称でも着順が正しく取得できること。

        半角カタカナ(U+FF61-U+FF9F)はCP932で1バイトだがUTF-8では3バイト(ord>0x80)。
        旧実装 `ord(c) < 0x80` では2バイトとして誤計上し o が過小になっていた。
        `.encode('cp932')` で正確に1バイトと計算されること。
        """
        # 全角2文字(4バイト) + 半角カタカナ4文字(4バイト) = 8バイト = 6 UTF-8文字
        abbr = "函館" + "ｱｲｳｴ"
        assert sum(len(c.encode('cp932')) for c in abbr) == 8
        assert len(abbr) == 6  # UTF-8文字数は6
        line = _make_sec_line_with_offset(abbr=abbr, finish_pos="05")
        result = JRDBParser.parse_sec_line(line)
        assert result is not None
        assert result["finish_position"] == 5, (
            "半角カタカナを含む略称でも finish_position が正しく取得されること"
        )

    def test_mixed_ascii_and_fullwidth_abbreviation(self):
        """全角3文字 + ASCII1文字（例: 'Ｓ'でなく'S'）の略称でも正しく動作すること"""
        abbr = "函館スS"  # 全角3文字(6バイト) + ASCII 1文字(1バイト) = 7バイト
        # CP932 7バイトは8バイトに満たないが実在しうるエッジケース
        # この場合は略称終端が確定せず abbr の後続文字を読む
        # テストとしては「パーサーがクラッシュしないこと」を確認
        line = _make_sec_line_with_offset(abbr=abbr, finish_pos="03")
        result = JRDBParser.parse_sec_line(line)
        assert result is not None


# ===================================================================
# Issue #324: KYFパーサーの jockey_code / trainer_code 修正テスト
# ===================================================================

def _get_kyf_lines_by_jockey(jockey_name: str) -> list[str]:
    """指定騎手名の KYF 行を複数ファイルから収集する。"""
    import os
    kyf_dir = ROOT_DIR / "downloaded_files" / "Kyf"
    lines = []
    for fname in sorted(os.listdir(kyf_dir)):
        if not fname.endswith(".csv"):
            continue
        fpath = kyf_dir / fname
        with open(fpath) as f:
            for line in f:
                line = line.rstrip("\n")
                if len(line) > 159 and line[153:159].strip().replace("　", "") == jockey_name:
                    lines.append(line)
        if len(lines) >= 5:
            break
    return lines


def _get_kyf_lines_by_trainer(trainer_name: str) -> list[str]:
    """指定調教師名の KYF 行を複数ファイルから収集する。"""
    import os
    kyf_dir = ROOT_DIR / "downloaded_files" / "Kyf"
    lines = []
    for fname in sorted(os.listdir(kyf_dir), reverse=True):
        if not fname.endswith(".csv"):
            continue
        fpath = kyf_dir / fname
        with open(fpath) as f:
            for line in f:
                line = line.rstrip("\n")
                if len(line) > 169 and line[163:169].strip().replace("　", "") == trainer_name:
                    lines.append(line)
        if len(lines) >= 3:
            break
    return lines


class TestParseKyfLineJockeyTrainerCode:
    """KYFパーサーの jockey_code / trainer_code 修正テスト（Issue #324）。

    修正前: line[295:300] / line[300:305] （誤位置）
    修正後: line[303:308] / line[308:313] （正しいCP932位置）
    """

    TARGET_JOCKEYS = [
        "武豊", "川田将雅", "Ｃ．ルメール", "松山弘平",
        "横山武史", "坂井瑠星", "岩田望来",
    ]
    TARGET_TRAINERS = [
        "矢作芳人", "友道康夫", "中内田充正", "木村哲也",
        "堀宣行", "斉藤崇史",
    ]

    def test_jockey_code_unique_per_jockey(self):
        """同一騎手名のレコードで jockey_code が全て同一値になること。"""
        for jockey_name in self.TARGET_JOCKEYS:
            lines = _get_kyf_lines_by_jockey(jockey_name)
            if not lines:
                pytest.skip(f"{jockey_name}: no test data found")

            codes = set()
            for line in lines:
                result = JRDBParser.parse_kyf_line(line)
                if result and result.get("jockey_name") == jockey_name:
                    codes.add(result["jockey_code"])

            assert len(codes) == 1, (
                f"{jockey_name}: jockey_code が一意でない → {codes}"
            )

    def test_trainer_code_unique_per_trainer(self):
        """同一調教師名のレコードで trainer_code が全て同一値になること。"""
        for trainer_name in self.TARGET_TRAINERS:
            lines = _get_kyf_lines_by_trainer(trainer_name)
            if not lines:
                pytest.skip(f"{trainer_name}: no test data found")

            codes = set()
            for line in lines:
                result = JRDBParser.parse_kyf_line(line)
                if result and result.get("trainer_name") == trainer_name:
                    codes.add(result["trainer_code"])

            assert len(codes) == 1, (
                f"{trainer_name}: trainer_code が一意でない → {codes}"
            )

    def test_jockey_code_is_5_digit_numeric(self):
        """jockey_code が 5桁数字であること。"""
        import os
        kyf_dir = ROOT_DIR / "downloaded_files" / "Kyf"
        checked = 0
        for fname in sorted(os.listdir(kyf_dir), reverse=True):
            if not fname.endswith(".csv"):
                continue
            fpath = kyf_dir / fname
            with open(fpath) as f:
                for line in f:
                    line = line.rstrip("\n")
                    result = JRDBParser.parse_kyf_line(line)
                    if result and result.get("jockey_code"):
                        code = result["jockey_code"]
                        assert code.isdigit() and len(code) == 5, (
                            f"jockey_code={code!r} が 5桁数字でない"
                        )
                        checked += 1
            if checked >= 100:
                break
        assert checked > 0, "テスト対象行が見つからない"

    def test_trainer_code_is_5_digit_numeric(self):
        """trainer_code が 5桁数字であること。"""
        import os
        kyf_dir = ROOT_DIR / "downloaded_files" / "Kyf"
        checked = 0
        for fname in sorted(os.listdir(kyf_dir), reverse=True):
            if not fname.endswith(".csv"):
                continue
            fpath = kyf_dir / fname
            with open(fpath) as f:
                for line in f:
                    line = line.rstrip("\n")
                    result = JRDBParser.parse_kyf_line(line)
                    if result and result.get("trainer_code"):
                        code = result["trainer_code"]
                        assert code.isdigit() and len(code) == 5, (
                            f"trainer_code={code!r} が 5桁数字でない"
                        )
                        checked += 1
            if checked >= 100:
                break
        assert checked > 0, "テスト対象行が見つからない"

    def test_jockey_trainer_name_unchanged_after_fix(self):
        """修正後も jockey_name / trainer_name が変わらないこと。"""
        jockeys_by_code = {}  # code → set of jockey_names
        trainers_by_code = {}

        lines = _get_kyf_lines_by_jockey("武豊")
        if not lines:
            pytest.skip("no test data found")

        for line in lines:
            result = JRDBParser.parse_kyf_line(line)
            if not result:
                continue
            j_code = result.get("jockey_code")
            j_name = result.get("jockey_name")
            t_code = result.get("trainer_code")
            t_name = result.get("trainer_name")

            if j_code and j_name:
                jockeys_by_code.setdefault(j_code, set()).add(j_name)
            if t_code and t_name:
                trainers_by_code.setdefault(t_code, set()).add(t_name)

        # 同一コードに複数の名前が混在しないこと
        for code, names in jockeys_by_code.items():
            assert len(names) == 1, f"jockey_code={code} に複数の騎手名: {names}"
        for code, names in trainers_by_code.items():
            assert len(names) == 1, f"trainer_code={code} に複数の調教師名: {names}"


# ===================================================================
# Issue #446: SEC コーナー順位1〜4 の解析
# ===================================================================

# downloaded_files/Sec/SEC250210.csv の1行目（実データ）
_SEC_LINE_1800M = '08251301012210299020250210ベイストラトラ\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u300018002111012A30023 \u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u300012\u3000\u3000\u3000\u30000601573560高杉吏麒\u3000\u3000鮫島一歩\u3000\u3000  20.305 18 18 -3                        333832MS-30.1-29.7 -9.3  4.5スーパージョ028398405\u3000\u3000\u3000\u3000\u3000\u3000                 2.9  14.2   2.510100806-28-161063310357476- 21 4   '

# downloaded_files/Sec/SEC170805.csv の1行目（実データ）
_SEC_LINE_1000M = '01171301011510179620170805レベルスリー\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u300010002111111A30023 \u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u300008\u3000\u3000\u3000\u30000401006540中井裕二\u3000\u3000松永康利\u3000\u3000  14.406 31 25-12  5  1                  433823SS-20.5-13.0-15.0-10.0サニーダンサ007365359\u3000\u3000\u3000\u3000\u3000\u3000                 2.4  13.2   2.800070705    -61056110403442- 4114   '

_SEC_LINE_1800M_EXPECTED = {
    'race_id': '08251301',
    'horse_number': 1,
    'horse_id': '22102990',
    'race_date': '2025-02-10',
    'horse_name': 'ベイストラトラ',
    'distance': 1800,
    'course_type': 'dirt',
    'direction': 'right',
    'track_condition': '良',
    'race_type': '12',
    'race_condition': 'A3',
    'grade': None,
    'race_name': None,
    'num_horses': 12,
    'finish_position': 6,
    'abnormal_code': 0,
    'finish_time': 157.3,
    'weight_carried': 56.0,
    'jockey_name': '高杉吏麒',
    'trainer_name': '鮫島一歩',
    'win_odds': 20.3,
    'win_popularity': 5,
    'idm': 18.0,
    'raw_score': 18.0,
    'track_bias': -3.0,
    'pace': None,
    'late_start': None,
    'position_fault': None,
    'disadvantage': None,
    'front_disadvantage': None,
    'mid_disadvantage': None,
    'back_disadvantage': None,
    'race_score': None,
    'course_position': 3,
    'improvement_code': 3,
    'class_code': 38,
    'body_code': 3,
    'condition_code': 2,
    'race_pace': 'M',
    'horse_pace': 'S',
    'ten_index': -30.1,
    'agari_index': -29.7,
    'pace_index': -9.3,
    'race_pace_index': 4.5,
    'winner_name': 'スーパージョ',
    'winner_time_diff': 2.8,
    'front_3f_time': 39.8,
    'last_3f_time': 40.5,
    'remarks': None,
    'place_odds': 2.9,
    'odds_10am_win': 14.2,
    'odds_10am_place': 2.5,
    'corner_position_1': 10,
    'corner_position_2': 10,
    'corner_position_3': 8,
    'corner_position_4': 6,
    'front_3f_lead_diff': None,
    'last_3f_lead_diff': None,
    'jockey_code': None,
    'trainer_code': None,
    'horse_weight': 476,
    'horse_weight_diff': -2,  # 実データは "- 2"（Issue #450 以前は NULL）
    'weather_code': 1,
    'course_code': None,
    'race_running_style': '4',
}

_SEC_LINE_1000M_EXPECTED = {
    'race_id': '01171301',
    'horse_number': 1,
    'horse_id': '15101796',
    'race_date': '2017-08-05',
    'horse_name': 'レベルスリー',
    'distance': 1000,
    'course_type': 'dirt',
    'direction': 'right',
    'track_condition': '速良',
    'race_type': '11',
    'race_condition': 'A3',
    'grade': None,
    'race_name': None,
    'num_horses': 8,
    'finish_position': 4,
    'abnormal_code': 0,
    'finish_time': 100.6,
    'weight_carried': 54.0,
    'jockey_name': '中井裕二',
    'trainer_name': '松永康利',
    'win_odds': 14.4,
    'win_popularity': 6,
    'idm': 31.0,
    'raw_score': 25.0,
    'track_bias': -12.0,
    'pace': 5.0,
    'late_start': 1.0,
    'position_fault': None,
    'disadvantage': None,
    'front_disadvantage': None,
    'mid_disadvantage': None,
    'back_disadvantage': None,
    'race_score': None,
    'course_position': 4,
    'improvement_code': 3,
    'class_code': 38,
    'body_code': 2,
    'condition_code': 3,
    'race_pace': 'S',
    'horse_pace': 'S',
    'ten_index': -20.5,
    'agari_index': -13.0,
    'pace_index': -15.0,
    'race_pace_index': -10.0,
    'winner_name': 'サニーダンサ',
    'winner_time_diff': 0.7,
    'front_3f_time': 36.5,
    'last_3f_time': 35.9,
    'remarks': None,
    'place_odds': 2.4,
    'odds_10am_win': 13.2,
    'odds_10am_place': 2.8,
    'corner_position_1': None,
    'corner_position_2': 7,
    'corner_position_3': 7,
    'corner_position_4': 5,
    'front_3f_lead_diff': None,
    'last_3f_lead_diff': None,
    'jockey_code': None,
    'trainer_code': None,
    'horse_weight': 442,
    'horse_weight_diff': -4,  # 実データは "- 4"（Issue #450 以前は NULL）
    'weather_code': 1,
    'course_code': '1',
    'race_running_style': '4',
}


def _with_corners(line: str, corners: str, o: int = 0) -> str:
    """SEC 行のコーナー順位1〜4（UTF-8文字位置 237-245 + o）を差し替える。"""
    return line[:237 + o] + corners + line[245 + o:]


class TestParseSecLineCornerPosition:
    """parse_sec_line の corner_position_1〜4 に関するテスト"""

    @pytest.mark.parametrize(
        "line, expected",
        [
            (_SEC_LINE_1800M, _SEC_LINE_1800M_EXPECTED),
            (_SEC_LINE_1000M, _SEC_LINE_1000M_EXPECTED),
        ],
    )
    def test_real_line_all_fields(self, line, expected):
        """実データ行の全フィールドが期待値と一致すること（コーナー以外は修正前の出力と同一）"""
        result = JRDBParser.parse_sec_line(line)
        result.pop("created_at")
        result.pop("updated_at")
        assert result == expected

    def test_zero_corner_is_none(self):
        """通過しないコーナー（"00"）は None になること"""
        result = JRDBParser.parse_sec_line(_SEC_LINE_1000M)
        assert result["corner_position_1"] is None
        assert (result["corner_position_2"], result["corner_position_3"], result["corner_position_4"]) == (7, 7, 5)

    def test_blank_corner_is_none(self):
        """空白のコーナー順位は None になること"""
        result = JRDBParser.parse_sec_line(_with_corners(_SEC_LINE_1800M, "        "))
        assert [result[f"corner_position_{i}"] for i in range(1, 5)] == [None] * 4

    def test_corner_change_does_not_affect_other_fields(self):
        """コーナー順位の値が変わっても他フィールドは変わらないこと"""
        result = JRDBParser.parse_sec_line(_with_corners(_SEC_LINE_1800M, "18011203"))
        assert [result[f"corner_position_{i}"] for i in range(1, 5)] == [18, 1, 12, 3]
        for key, value in _SEC_LINE_1800M_EXPECTED.items():
            if not key.startswith("corner_position_"):
                assert result[key] == value, key

    def test_abbreviation_offset_applied(self):
        """略称に半角文字が混在する場合もオフセット補正後の位置から取得すること"""
        # 全角4文字の略称を「全角3文字+半角2文字」に置き換える（CP932 8バイトのまま o=1）
        line = _SEC_LINE_1800M[:89] + "　　　  " + _SEC_LINE_1800M[93:]
        result = JRDBParser.parse_sec_line(line)
        assert [result[f"corner_position_{i}"] for i in range(1, 5)] == [10, 10, 8, 6]
        assert result["place_odds"] == 2.9
        assert result["horse_weight"] == 476

    def test_short_line_corner_none_other_fields_kept(self):
        """コーナー順位まで届かない短い行は corner が None で、既存フィールドは従来どおり取得されること"""
        result = JRDBParser.parse_sec_line(_SEC_LINE_1800M[:240])
        assert [result[f"corner_position_{i}"] for i in range(1, 5)] == [None] * 4
        assert result["place_odds"] == 2.9
        assert result["odds_10am_place"] == 2.5
        assert result["finish_position"] == 6


# 実データ KYF250113 の1行目（ブリンカー非装着・文字位置152は空白）
_KYF_LINE_REAL = '062515010122103450ロジステート\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000 17.8  1.4  1.5  0.0  0.0  0.0 20.72 3 11  6.9 3  1.9 3  0  1  1  5     6  8 14 55     235 12.1  4.13324.014924218   横山武史\u3000\u3000550 尾形和幸\u3000\u3000美浦22103450202410192210345020241006                                                0524450205244204                        1    5363 3 1059010450      0    00-21.6-15.0-27.3 -3.3H 6132 6104102734        2久米田\u3000正平氏\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u30000600 2 4 91015 511.436.42         '


def _with_blinker(line: str, code: str) -> str:
    """KYF 行のブリンカー（UTF-8文字位置152、騎手名の直前）を差し替える。"""
    return line[:152] + code + line[153:]


class TestParseKyfLineBlinker:
    """parse_kyf_line の blinker に関するテスト (Issue #441)"""

    def test_real_line_position(self):
        """実データ行でブリンカー位置の直後が騎手名であること（位置の前提確認）"""
        assert _KYF_LINE_REAL[152] == " "
        assert _KYF_LINE_REAL[153:157] == "横山武史"

    @pytest.mark.parametrize(
        "code, expected",
        [("1", "1"), ("2", "2"), ("3", "3"), (" ", None)],
    )
    def test_blinker_codes(self, code, expected):
        """1:初装着 / 2:再装着 / 3:ブリンカ / 空白=None が格納されること"""
        result = JRDBParser.parse_kyf_line(_with_blinker(_KYF_LINE_REAL, code))
        assert result["blinker"] == expected

    def test_other_fields_unchanged(self):
        """ブリンカーを差し替えても他のフィールドは変わらないこと"""
        base = JRDBParser.parse_kyf_line(_KYF_LINE_REAL)
        changed = JRDBParser.parse_kyf_line(_with_blinker(_KYF_LINE_REAL, "1"))
        for result in (base, changed):
            for key in ("blinker", "created_at", "updated_at"):
                result.pop(key)
        assert changed == base
        assert base["jockey_name"] == "横山武史"


# 実データ KYF170521 の26行目（1番人気・印あり・距離適性あり）
_KYF_LINE_REAL_2017 = '041718021014101964グロワールシチー\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000 44.0  3.1  4.0  0.0  0.0  0.0 51.1213  2  2.5 1  1.3 1  5  2  0  0    33 25  4  3     796 19.8 21.72352.318122337   義英真\u3000\u3000\u30005501岡田稲男\u3000\u3000栗東141019642017043014101964201704091410196420170211                                041712050917260208172504                6  111111  21057510376    365    00  4.8  9.5-12.3 -1.0H 2 12 1 02 1 010        1㈱友駿ホースクラブ\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u3000\u30000700 1 1 1 1 5 131.166.23         '

# 仕様（kyi_doc.txt 第11版）のバイト位置どおりに切り出した期待値
_KYF_EXPECTED_REAL = {
    "total_index": 20.7, "running_style": 2, "distance_aptitude": None, "improvement": 3, "rotation": 11,
    "base_odds": 6.9, "base_popularity": 3, "base_place_odds": 1.9, "base_place_popularity": 3,
    "specific_mark_circle": 0, "specific_mark_circle2": 1, "specific_mark_triangle": 1,
    "specific_mark_triangle2": 5, "specific_mark_x": None,
    "total_mark_circle": 6, "total_mark_circle2": 8, "total_mark_triangle": 14,
    "total_mark_triangle2": 55, "total_mark_x": None,
    "popularity_index": 235, "training_index": 12.1, "stable_index": 4.1,
    "training_arrow_code": 3, "stable_eval_code": 3, "jockey_expected_win_rate": 24.0, "surge_index": 149,
    "hoof_code": 24, "heavy_aptitude_code": 2, "class_code": 18, "blinker": None,
    "jockey_name": "横山武史", "weight_carried": 55.0, "apprentice_class": None,
    "trainer_name": "尾形和幸", "trainer_affiliation": "美浦",
    "prev_race_key_1": "2210345020241019", "prev_race_key_2": "2210345020241006", "prev_race_key_3": None,
    "bracket_number": 1,
    "overall_mark": None, "idm_mark": None, "info_mark": 5, "jockey_mark": 3, "stable_mark": 6,
    "training_mark": 3, "surge_mark": None, "turf_aptitude": "3", "dirt_aptitude": None,
    "jockey_code": "10590", "trainer_code": "10450",
    "prize_money": 0, "earned_prize": 0, "condition_class": 0,
    "ten_index": -21.6, "pace_index": -15.0, "agari_index": -27.3, "position_index": -3.3, "pace_forecast": "H",
    "mid_position": 6, "mid_gap": 13, "mid_inside_outside": 2,
    "last_3f_position": 6, "last_3f_gap": 10, "last_3f_inside_outside": 4,
    "goal_position": 10, "goal_gap": 27, "goal_inside_outside": 3, "development_code": "4",
    "confirmed_weight": None, "confirmed_weight_diff": None, "sex_code": 2, "owner_name": "久米田正平氏",
}

_KYF_EXPECTED_REAL_2017 = {
    "total_index": 51.1, "running_style": 2, "distance_aptitude": 1, "improvement": 3, "rotation": 2,
    "base_odds": 2.5, "base_popularity": 1, "base_place_odds": 1.3, "base_place_popularity": 1,
    "specific_mark_circle": 5, "specific_mark_circle2": 2, "specific_mark_triangle": 0,
    "specific_mark_triangle2": 0, "specific_mark_x": None,
    "total_mark_circle": 33, "total_mark_circle2": 25, "total_mark_triangle": 4,
    "total_mark_triangle2": 3, "total_mark_x": None,
    "popularity_index": 796, "training_index": 19.8, "stable_index": 21.7,
    "training_arrow_code": 2, "stable_eval_code": 3, "jockey_expected_win_rate": 52.3, "surge_index": 181,
    "hoof_code": 22, "heavy_aptitude_code": 3, "class_code": 37, "blinker": None,
    "jockey_name": "義英真", "weight_carried": 55.0, "apprentice_class": 1,
    "trainer_name": "岡田稲男", "trainer_affiliation": "栗東",
    "prev_race_key_1": "1410196420170430", "prev_race_key_2": "1410196420170409",
    "prev_race_key_3": "1410196420170211", "prev_race_key_4": None,
    "bracket_number": 6,
    "overall_mark": 1, "idm_mark": 1, "info_mark": 1, "jockey_mark": 1, "stable_mark": 1,
    "training_mark": 1, "surge_mark": None, "turf_aptitude": None, "dirt_aptitude": "2",
    "jockey_code": "10575", "trainer_code": "10376",
    "prize_money": 365, "earned_prize": 0, "condition_class": 0,
    "ten_index": 4.8, "pace_index": 9.5, "agari_index": -12.3, "position_index": -1.0, "pace_forecast": "H",
    "mid_position": 2, "mid_gap": 1, "mid_inside_outside": 2,
    "last_3f_position": 1, "last_3f_gap": 0, "last_3f_inside_outside": 2,
    "goal_position": 1, "goal_gap": 0, "goal_inside_outside": 1, "development_code": "0",
    "confirmed_weight": None, "confirmed_weight_diff": None, "sex_code": 1, "owner_name": "㈱友駿ホースクラブ",
}


class TestParseKyfLineSpecPositions:
    """parse_kyf_line の各フィールドが仕様位置どおりに読まれること (Issue #452)"""

    @pytest.mark.parametrize(
        "line, expected",
        [(_KYF_LINE_REAL, _KYF_EXPECTED_REAL), (_KYF_LINE_REAL_2017, _KYF_EXPECTED_REAL_2017)],
        ids=["KYF250113", "KYF170521"],
    )
    def test_real_line_all_fields(self, line, expected):
        """実データ行で、仕様位置から切り出した値と全フィールドが一致すること"""
        result = JRDBParser.parse_kyf_line(line)
        assert {k: result[k] for k in expected} == expected

    def test_key_and_head_fields_unchanged(self):
        """レースキー〜情報指数（修正対象外の先頭部）が従来どおりであること"""
        result = JRDBParser.parse_kyf_line(_KYF_LINE_REAL_2017)
        assert (result["race_id"], result["horse_number"], result["horse_id"], result["horse_name"]) == (
            "04171802", 10, "14101964", "グロワールシチー"
        )
        assert (result["idm"], result["jockey_index"], result["info_index"]) == (44.0, 3.1, 4.0)

    def test_position_index_two_digit_negative(self):
        """位置指数が -10.0 以下でも先頭の符号を落とさないこと（旧実装は 342:346 で '-12.3' → 12.3）"""
        line = _KYF_LINE_REAL[:341] + "-12.3" + _KYF_LINE_REAL[346:]
        assert JRDBParser.parse_kyf_line(line)["position_index"] == -12.3

    @pytest.mark.parametrize(
        "weight, diff, expected_diff",
        [("486", "+ 4", 4), ("450", "-12", -12), ("502", "  0", 0)],
    )
    def test_confirmed_weight(self, weight, diff, expected_diff):
        """確定馬体重（文字位置364:367）と増減（367:370、符号+数字）が読まれること"""
        line = _KYF_LINE_REAL[:364] + weight + diff + _KYF_LINE_REAL[370:]
        result = JRDBParser.parse_kyf_line(line)
        assert result["confirmed_weight"] == int(weight)
        assert result["confirmed_weight_diff"] == expected_diff
        assert result["sex_code"] == 2
def _with_weight_diff(line: str, diff: str) -> str:
    """SEC 行の馬体重増減（UTF-8文字位置 264-267、この行は略称オフセット o=0）を差し替える。"""
    return line[:264] + diff + line[267:]


class TestParseSecLineWeightDiff:
    """parse_sec_line の horse_weight_diff に関するテスト (Issue #450)"""

    def test_real_line_position(self):
        """実データ行で馬体重の直後が増減であること（位置の前提確認）"""
        assert _SEC_LINE_1800M[261:267] == "476- 2"

    @pytest.mark.parametrize(
        "diff, expected",
        [("- 2", -2), ("+ 2", 2), ("-10", -10), ("+12", 12), ("  0", 0), ("   ", None)],
    )
    def test_weight_diff_formats(self, diff, expected):
        """符号と数字の間に空白がある1桁・2桁・0・空白が正しく読まれること"""
        result = JRDBParser.parse_sec_line(_with_weight_diff(_SEC_LINE_1800M, diff))
        assert result["horse_weight_diff"] == expected

    def test_other_fields_unchanged(self):
        """増減を差し替えても他のフィールドは変わらないこと"""
        base = JRDBParser.parse_sec_line(_SEC_LINE_1800M)
        changed = JRDBParser.parse_sec_line(_with_weight_diff(_SEC_LINE_1800M, "+ 8"))
        for result in (base, changed):
            for key in ("horse_weight_diff", "created_at", "updated_at"):
                result.pop(key, None)
        assert changed == base
