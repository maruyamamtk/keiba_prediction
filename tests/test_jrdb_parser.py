"""
JRDBParser のユニットテスト

Issue #214: parse_baa_line が start_time を正しく返すことを検証する。
Issue #272: parse_cha_line が調教本追切データを正しく解析することを検証する。
Issue #290: parse_kyf_line が base_popularity を10+人気でも正しく返すことを検証する。
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
      [36:78] 各種指数・脚質など (42文字)
      [78:82] 基準オッズ (4文字 "XX.X")
      [82:84] 基準人気 (2文字 " N" or "NN")
      [84:85] セパレータ (スペース)
      [85:89] 基準複勝オッズ (4文字)
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
