"""
JRDBParser の parse_sec_line ユニットテスト

着順などのフィールド位置が正しく解析されることを検証する。

SEC レコード構造 (CP932 デコード後の Python 文字列位置):
  0- 7: レースキー (8 ASCII)
  8- 9: 馬番 (2 ASCII)
 10-17: 血統登録番号 (8 ASCII)
 18-25: 年月日 (8 ASCII, YYYYMMDD)
 26-43: 馬名 (18 全角 = 18 Python chars)
 44-47: 距離 (4 ASCII)
 48   : 芝ダ障害コード (1)
 49   : 右左コード (1)
 50   : 内外コード (1)
 51-52: 馬場状態コード (2)
 53-54: 種別コード (2)
 55-56: 条件コード (2)
 57-60: 記号コード (4)
 61   : グレードコード (1)
 62-86: レース名 (25 全角 = 25 Python chars)
 87-88: 頭数 (2 ASCII)
 89-90: 馬番 再掲 (2 ASCII, スキップ)
 91-92: 着順 (2 ASCII)  ← finish_position
 93   : 異常コード (1 ASCII)
 94-97: 走破タイム 0.1秒単位 (4 ASCII)
 98-100: 斤量 0.1kg単位 (3 ASCII)
101-106: 騎手名 (6 全角 = 6 Python chars)
107-112: 調教師名 (6 全角 = 6 Python chars)
113-118: 単勝オッズ (6 ASCII)
119-120: 単勝人気 (2 ASCII)
121-123: IDM (3 ASCII)
...
182-187: 1着馬名 (6 全角 = 6 Python chars)
188-190: 1着タイム差 (3 ASCII)
191-193: 前3F (3 ASCII)
194-196: 後3F (3 ASCII)
197-206: 備考 (10 全角 = 10 Python chars)
207-218: コーナー順位等 (12 ASCII, 非抽出)
219-224: 確定複勝オッズ下 (6 ASCII)  ← 仕様書 相対291, 検証済み
225-230: 10時単勝オッズ (6 ASCII)
231-236: 10時複勝オッズ (6 ASCII)
...
261-263: 馬体重 (3 ASCII)  ← 仕様書 byte 333-335, 検証済み
264-266: 馬体重増減 (3 ASCII)
267   : 天候コード (1)
268   : コース (1)
269   : レース脚質 (1)
"""

import pytest
from src.automation.data.jrdb_parser import JRDBParser


def _build_sec_line(
    race_key: str = "08261712",
    horse_number: str = "03",
    horse_id: str = "23045678",
    race_date: str = "20260118",
    horse_name_full: str = "サク",          # 全角で指定、18文字にパディング
    distance: str = "1600",
    course_type: str = "1",
    direction: str = "2",
    inner_outer: str = " ",
    track_condition: str = "11",
    race_type: str = "11",
    race_condition: str = "05",
    mark_code: str = "    ",
    grade_code: str = " ",
    race_name_full: str = "4歳以上1勝クラス",  # 25文字にパディング
    num_horses: str = "12",
    horse_number_2: str = "03",
    finish_pos: str = " 1",
    abnormal_code: str = "0",
    time_val: str = "0961",
    weight_carried: str = "560",
    jockey_name_full: str = "川田将雅",       # 6全角にパディング
    trainer_name_full: str = "友道康夫",      # 6全角にパディング
    win_odds: str = " 43.5",
    win_popularity: str = " 5",
    idm: str = "   ",
    raw_score: str = "   ",
    track_bias: str = "   ",
    pace_val: str = "   ",
    late_start: str = "   ",
    position_fault: str = "   ",
    disadvantage: str = "   ",
    front_disadvantage: str = "   ",
    mid_disadvantage: str = "   ",
    back_disadvantage: str = "   ",
    race_score: str = "   ",
    course_position: str = " ",
    improvement_code: str = " ",
    class_code: str = "  ",
    body_code: str = " ",
    condition_code: str = " ",
    race_pace: str = " ",
    horse_pace: str = " ",
    ten_index: str = "     ",
    agari_index: str = "     ",
    pace_index: str = "     ",
    race_pace_index: str = "     ",
    winner_name_full: str = "サク",          # 6全角にパディング
    winner_diff: str = "  0",
    front_3f: str = "347",
    last_3f: str = "345",
    remarks_full: str = "",                 # 10全角にパディング
    corner_data: str = "            ",      # 12 ASCII (コーナー順位等)
    place_odds: str = "  12.0",
    odds_10am_win: str = "  43.5",
    odds_10am_place: str = "  12.0",
    # 219-260: 残りフィールド (42 ASCII)
    intermediate: str = " " * 24,
    horse_weight: str = "470",
    weight_diff: str = "+10",
    weather_code: str = "1",
    course_code: str = "1",
    race_running_style: str = "3",
) -> str:
    """テスト用 SEC 行を構築するヘルパー関数。"""

    def _pad_full(text: str, width: int) -> str:
        """全角文字列を指定文字数にパディング (全角スペースで埋める)。"""
        padded = text.ljust(width, '　')
        return padded[:width]

    def _pad_ascii(text: str, width: int) -> str:
        """ASCII文字列を指定文字数にパディング (半角スペースで埋める)。"""
        padded = text.ljust(width, ' ')
        return padded[:width]

    # 各セクションを組み立てる
    # 0- 7: レースキー
    sec = _pad_ascii(race_key, 8)
    # 8- 9: 馬番
    sec += _pad_ascii(horse_number, 2)
    # 10-17: 血統登録番号
    sec += _pad_ascii(horse_id, 8)
    # 18-25: 年月日
    sec += _pad_ascii(race_date, 8)
    # 26-43: 馬名 (18全角)
    sec += _pad_full(horse_name_full, 18)
    # 44-47: 距離
    sec += _pad_ascii(distance, 4)
    # 48: 芝ダ障害コード
    sec += course_type
    # 49: 右左コード
    sec += direction
    # 50: 内外コード
    sec += inner_outer
    # 51-52: 馬場状態コード
    sec += _pad_ascii(track_condition, 2)
    # 53-54: 種別コード
    sec += _pad_ascii(race_type, 2)
    # 55-56: 条件コード
    sec += _pad_ascii(race_condition, 2)
    # 57-60: 記号コード
    sec += _pad_ascii(mark_code, 4)
    # 61: グレードコード
    sec += grade_code
    # 62-86: レース名 (25全角)
    sec += _pad_full(race_name_full, 25)
    # 87-88: 頭数
    sec += _pad_ascii(num_horses, 2)
    # 89-90: 馬番 再掲 (スキップ)
    sec += _pad_ascii(horse_number_2, 2)
    # 91-92: 着順
    assert len(sec) == 91, f"Expected 91 chars before finish_pos, got {len(sec)}"
    sec += _pad_ascii(finish_pos, 2)
    # 93: 異常コード
    sec += abnormal_code
    # 94-97: 走破タイム
    sec += _pad_ascii(time_val, 4)
    # 98-100: 斤量
    sec += _pad_ascii(weight_carried, 3)
    # 101-106: 騎手名 (6全角)
    sec += _pad_full(jockey_name_full, 6)
    # 107-112: 調教師名 (6全角)
    sec += _pad_full(trainer_name_full, 6)
    # 113-118: 単勝オッズ (6桁)
    sec += _pad_ascii(win_odds, 6)
    # 119-120: 単勝人気
    sec += _pad_ascii(win_popularity, 2)
    # 121-123: IDM (3桁)
    sec += _pad_ascii(idm, 3)
    # 124-126: 素点
    sec += _pad_ascii(raw_score, 3)
    # 127-129: 馬場差
    sec += _pad_ascii(track_bias, 3)
    # 130-132: ペース
    sec += _pad_ascii(pace_val, 3)
    # 133-135: 出遅
    sec += _pad_ascii(late_start, 3)
    # 136-138: 位置取
    sec += _pad_ascii(position_fault, 3)
    # 139-141: 不利
    sec += _pad_ascii(disadvantage, 3)
    # 142-144: 前不利
    sec += _pad_ascii(front_disadvantage, 3)
    # 145-147: 中不利
    sec += _pad_ascii(mid_disadvantage, 3)
    # 148-150: 後不利
    sec += _pad_ascii(back_disadvantage, 3)
    # 151-153: レース指数
    sec += _pad_ascii(race_score, 3)
    # 154: コース取
    sec += course_position
    # 155: 上昇度コード
    sec += improvement_code
    # 156-157: クラスコード
    sec += _pad_ascii(class_code, 2)
    # 158: 馬体コード
    sec += body_code
    # 159: 気配コード
    sec += condition_code
    # 160: レースペース
    sec += race_pace
    # 161: 馬ペース
    sec += horse_pace
    # 162-166: テン指数 (5桁)
    sec += _pad_ascii(ten_index, 5)
    # 167-171: 上がり指数
    sec += _pad_ascii(agari_index, 5)
    # 172-176: ペース指数
    sec += _pad_ascii(pace_index, 5)
    # 177-181: レースP指数
    sec += _pad_ascii(race_pace_index, 5)
    # 182-187: 1着馬名 (6全角)
    sec += _pad_full(winner_name_full, 6)
    # 188-190: 1着タイム差 (3桁)
    sec += _pad_ascii(winner_diff, 3)
    # 191-193: 前3F
    sec += _pad_ascii(front_3f, 3)
    # 194-196: 後3F
    sec += _pad_ascii(last_3f, 3)
    # 197-206: 備考 (10全角)
    sec += _pad_full(remarks_full, 10)
    # 207-218: コーナー順位等 (12 ASCII)
    assert len(sec) == 207, f"Expected 207 chars before corner_data, got {len(sec)}"
    sec += _pad_ascii(corner_data, 12)
    # 219-224: 確定複勝オッズ下 (6桁) ← 検証済み位置
    assert len(sec) == 219, f"Expected 219 chars before place_odds, got {len(sec)}"
    sec += _pad_ascii(place_odds, 6)
    # 225-230: 10時単勝オッズ
    sec += _pad_ascii(odds_10am_win, 6)
    # 231-236: 10時複勝オッズ
    sec += _pad_ascii(odds_10am_place, 6)
    # 237-260: 中間フィールド (24 ASCII)
    sec += _pad_ascii(intermediate, 24)
    # 261-263: 馬体重 (3桁) ← 検証済み位置 (仕様書 byte 333-335)
    assert len(sec) == 261, f"Expected 261 chars before horse_weight, got {len(sec)}"
    sec += _pad_ascii(horse_weight, 3)
    # 264-266: 馬体重増減
    sec += _pad_ascii(weight_diff, 3)
    # 267: 天候コード
    sec += weather_code
    # 268: コース
    sec += course_code
    # 269: レース脚質
    sec += race_running_style

    return sec


class TestSecParserFinishPosition:
    """着順フィールドの正しい解析を検証する。"""

    def test_finish_position_first(self):
        """1着の着順が正しく取得できること。"""
        line = _build_sec_line(
            horse_name_full="サク",
            finish_pos=" 1",
            abnormal_code="0",
            time_val="0961",
        )
        result = JRDBParser.parse_sec_line(line)
        assert result is not None
        assert result["finish_position"] == 1

    def test_finish_position_second(self):
        """2着の着順が正しく取得できること。"""
        line = _build_sec_line(
            horse_name_full="レッドフェルメール",
            horse_number="11",
            horse_number_2="11",
            finish_pos=" 2",
            time_val="0961",
        )
        result = JRDBParser.parse_sec_line(line)
        assert result is not None
        assert result["finish_position"] == 2

    def test_finish_position_third(self):
        """3着の着順が正しく取得できること。"""
        line = _build_sec_line(
            horse_name_full="パスコード",
            horse_number="01",
            horse_number_2="01",
            finish_pos=" 3",
        )
        result = JRDBParser.parse_sec_line(line)
        assert result is not None
        assert result["finish_position"] == 3

    def test_finish_position_not_zero(self):
        """着順が 0 にならないこと（バグ回帰テスト）。"""
        for pos in range(1, 13):
            line = _build_sec_line(finish_pos=f"{pos:2d}", abnormal_code="0")
            result = JRDBParser.parse_sec_line(line)
            assert result is not None
            assert result["finish_position"] == pos, (
                f"finish_position should be {pos}, got {result['finish_position']}"
            )

    def test_abnormal_code_normal(self):
        """正常完走時の異常コードが 0 であること。"""
        line = _build_sec_line(finish_pos=" 1", abnormal_code="0")
        result = JRDBParser.parse_sec_line(line)
        assert result is not None
        assert result["abnormal_code"] == 0

    def test_finish_time_parsed_correctly(self):
        """走破タイムが正しく解析されること (0.1秒単位)。"""
        # 96.1秒 = 0961
        line = _build_sec_line(finish_pos=" 1", time_val="0961")
        result = JRDBParser.parse_sec_line(line)
        assert result is not None
        assert result["finish_time"] == pytest.approx(96.1)

    def test_weight_carried_parsed(self):
        """斤量が正しく解析されること (0.1kg単位 → kg)。"""
        line = _build_sec_line(weight_carried="560")  # 56.0kg
        result = JRDBParser.parse_sec_line(line)
        assert result is not None
        assert result["weight_carried"] == pytest.approx(56.0)

    def test_jockey_name_parsed(self):
        """騎手名が正しく解析されること。"""
        line = _build_sec_line(jockey_name_full="川田将雅")
        result = JRDBParser.parse_sec_line(line)
        assert result is not None
        assert result["jockey_name"] == "川田将雅"

    def test_horse_name_parsed(self):
        """馬名が正しく解析されること。"""
        line = _build_sec_line(horse_name_full="サク")
        result = JRDBParser.parse_sec_line(line)
        assert result is not None
        assert result["horse_name"] == "サク"

    def test_horse_weight_at_correct_position(self):
        """馬体重が正しい位置 (Python 261-263) で解析されること。"""
        line = _build_sec_line(horse_weight="470")
        result = JRDBParser.parse_sec_line(line)
        assert result is not None
        assert result["horse_weight"] == 470

    def test_place_odds_at_correct_position(self):
        """確定複勝オッズが正しい位置 (Python 219-224) で解析されること。"""
        line = _build_sec_line(place_odds="  12.0")
        result = JRDBParser.parse_sec_line(line)
        assert result is not None
        # 12.0 として解析される
        assert result["place_odds"] == pytest.approx(12.0)

    def test_race_id_parsed(self):
        """レースキーが正しく解析されること。"""
        line = _build_sec_line(race_key="08261712")
        result = JRDBParser.parse_sec_line(line)
        assert result is not None
        assert result["race_id"] == "08261712"

    def test_num_horses_parsed(self):
        """頭数が正しく解析されること。"""
        line = _build_sec_line(num_horses="12")
        result = JRDBParser.parse_sec_line(line)
        assert result is not None
        assert result["num_horses"] == 12

    def test_win_odds_parsed(self):
        """単勝オッズが正しく解析されること。"""
        line = _build_sec_line(win_odds=" 43.5")
        result = JRDBParser.parse_sec_line(line)
        assert result is not None
        assert result["win_odds"] == pytest.approx(43.5)
