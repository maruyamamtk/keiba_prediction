"""
JRDB データパーサー

GCSにアップロードされたJRDBデータファイルを解析し、
BigQueryにロード可能な形式に変換します。

サポートするデータタイプ:
- BAA/BAB/BAC: 番組データ (レース情報)
- KYF/KYG/KYH: 競走馬データ
- SEC: 成績データ
- KAA/KAB: 開催データ
- OZ: オッズデータ
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime
import re

logger = logging.getLogger(__name__)


class JRDBParser:
    """JRDBデータのパーサークラス"""

    # 場コードマッピング
    VENUE_CODE_MAP = {
        '01': '札幌', '02': '函館', '03': '福島', '04': '新潟', '05': '東京',
        '06': '中山', '07': '中京', '08': '京都', '09': '阪神', '10': '小倉'
    }

    # コース種別マッピング
    COURSE_TYPE_MAP = {
        '1': 'turf',    # 芝
        '2': 'dirt',    # ダート
        '3': 'obstacle' # 障害
    }

    # 馬場状態マッピング
    TRACK_CONDITION_MAP = {
        '10': '良', '11': '速良', '12': '遅良',
        '20': '稍重', '21': '速稍重', '22': '遅稍重',
        '30': '重', '31': '速重', '32': '遅重',
        '40': '不良', '41': '速不良', '42': '遅不良',
        '1': '良', '2': '稍重', '3': '重', '4': '不良'
    }

    # 天候マッピング
    WEATHER_MAP = {
        '1': '晴', '2': '曇', '3': '小雨', '4': '雨', '5': '小雪', '6': '雪'
    }

    @staticmethod
    def parse_race_id(race_key: str) -> Dict[str, any]:
        """
        レースキーを解析

        Args:
            race_key: レースキー (8バイト: 場コード2 + 年2 + 回1 + 日1 + R2)

        Returns:
            解析結果の辞書
        """
        if len(race_key) != 8:
            raise ValueError(f"Invalid race key length: {race_key}")

        venue_code = race_key[0:2]
        year = int(race_key[2:4])
        kai = int(race_key[4:5])
        day = int(race_key[5:6])
        race_num = int(race_key[6:8])

        # 2000年以降の年を補完
        if year >= 0 and year <= 50:
            year += 2000
        else:
            year += 1900

        return {
            'race_id': race_key,
            'venue_code': venue_code,
            'year': year,
            'kai': kai,
            'day': day,
            'race_number': race_num
        }

    @staticmethod
    def parse_date(date_str: str) -> Optional[str]:
        """
        日付文字列を解析してISO形式に変換

        Args:
            date_str: YYYYMMDD形式の日付文字列

        Returns:
            ISO形式の日付文字列 (YYYY-MM-DD) or None
        """
        try:
            if not date_str or date_str.strip() == '':
                return None
            date_obj = datetime.strptime(date_str.strip(), '%Y%m%d')
            return date_obj.strftime('%Y-%m-%d')
        except Exception as e:
            logger.warning(f"Date parse error: {date_str} - {e}")
            return None

    @staticmethod
    def safe_int(value: str, default: Optional[int] = None) -> Optional[int]:
        """文字列を安全にintに変換"""
        try:
            cleaned = value.strip()
            if not cleaned or cleaned == '':
                return default
            return int(cleaned)
        except (ValueError, AttributeError):
            return default

    @staticmethod
    def safe_float(value: str, default: Optional[float] = None) -> Optional[float]:
        """文字列を安全にfloatに変換"""
        try:
            cleaned = value.strip()
            if not cleaned or cleaned == '':
                return default
            return float(cleaned)
        except (ValueError, AttributeError):
            return default

    @staticmethod
    def parse_baa_line(line: str) -> Optional[Dict]:
        """
        BAA (番組データ) を解析

        Args:
            line: 固定長レコード文字列 (約118バイト)

        Returns:
            解析結果の辞書 or None
        """
        try:
            if len(line) < 90:
                logger.warning(f"BAA line too short: {len(line)} bytes")
                return None

            race_key = line[0:8].strip()
            race_info = JRDBParser.parse_race_id(race_key)

            # 日付 (YYYYMMDD形式、8バイト)
            race_date = JRDBParser.parse_date(line[8:16])

            # 発走時刻 (HHMM)
            start_time = line[16:20].strip()

            # 距離
            distance = JRDBParser.safe_int(line[20:24])

            # コース種別 (芝/ダート/障害)
            course_type_code = line[24:25]
            course_type = JRDBParser.COURSE_TYPE_MAP.get(course_type_code, None)

            # 右左
            direction_code = line[25:26]
            direction = 'right' if direction_code == '1' else 'left' if direction_code == '2' else 'straight'

            # 内外
            inner_outer = line[26:27].strip()

            # 種別コード
            age_condition = line[27:29].strip()

            # 条件コード
            race_condition = line[29:31].strip()

            # 記号 (性別・外国産馬など)
            symbol = line[31:34].strip()

            # 重量種別
            weight_type = line[34:35].strip()

            # グレード
            grade_code = line[35:36].strip()
            race_class = None
            if grade_code == '1':
                race_class = 'G1'
            elif grade_code == '2':
                race_class = 'G2'
            elif grade_code == '3':
                race_class = 'G3'
            elif grade_code == '5':
                race_class = 'Listed'
            elif race_condition == 'OP':
                race_class = 'Open'
            else:
                race_class = race_condition

            # レース名 (約50バイト、全角文字を含む可能性があるため安全に取得)
            race_name = line[36:86].strip() if len(line) > 86 else line[36:].strip()

            # 出走頭数 (行の後半から正規表現で取得)
            num_horses = None
            prize_1st = None
            prize_2nd = None
            prize_3rd = None

            # 行の後半部分(80文字以降)から頭数を探す
            # パターン: 頭数(1-2桁) + スペース(0-1) + コード(1-2桁) + レース略称(オプション)
            if len(line) > 80:
                tail = line[80:].strip()
                match = re.search(r'(\d{1,2})[ ]?(\d{1,2})(?:[^\d]*)$', tail)
                if match:
                    num_horses = JRDBParser.safe_int(match.group(1))

            venue_name = JRDBParser.VENUE_CODE_MAP.get(race_info['venue_code'], '')

            return {
                'race_id': race_key,
                'race_date': race_date,
                'venue_code': race_info['venue_code'],
                'venue_name': venue_name,
                'race_number': race_info['race_number'],
                'race_name': race_name,
                'course_type': course_type,
                'distance': distance,
                'direction': direction,
                'race_class': race_class,
                'age_condition': age_condition,
                'sex_condition': symbol,
                'weather': None,  # BAAでは未定
                'track_condition': None,  # BAAでは未定
                'num_horses': num_horses,
                'prize_1st': prize_1st,
                'prize_2nd': prize_2nd,
                'prize_3rd': prize_3rd,
                'created_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"Error parsing BAA line: {e}", exc_info=True)
            return None

    @staticmethod
    def parse_kyf_line(line: str) -> Optional[Dict]:
        """
        KYF (競走馬データ) を解析 - KYIスキーマ準拠

        UTF-8デコード後の文字位置に基づいて各フィールドを抽出します。
        実際のTXTファイル分析に基づいて位置を修正済み。

        Args:
            line: 固定長レコード文字列 (UTF-8変換済み)

        Returns:
            解析結果の辞書 or None
        """
        try:
            if len(line) < 170:
                logger.warning(f"KYF line too short: {len(line)} chars")
                return None

            # === 固定位置フィールド (ASCII部分) ===
            race_key = line[0:8].strip()           # レースキー (06261101)
            horse_number = JRDBParser.safe_int(line[8:10])  # 馬番 (01)
            horse_id = line[10:18].strip()         # 血統登録番号 (23107385)

            # === 馬名 (全角18文字 = 位置18-36) ===
            horse_name = line[18:36].strip()
            # 全角スペースも除去
            horse_name = horse_name.replace('　', '').strip()

            # === 各種指数 (位置36以降) ===
            # IDM (5文字: 36-41) → " 20.0"
            idm = JRDBParser.safe_float(line[36:41])
            # 騎手指数 (5文字: 41-46) → "  0.2"
            jockey_index = JRDBParser.safe_float(line[41:46])
            # 情報指数 (5文字: 46-51) → " -1.0"
            info_index = JRDBParser.safe_float(line[46:51])
            # 予備フィールド (51-67)
            # 総合指数 (5文字: 67-72) → "19.23"
            total_index = JRDBParser.safe_float(line[67:72])

            # === 脚質・距離適性 (位置72以降) ===
            running_style = JRDBParser.safe_int(line[73:74])  # 脚質 (1文字: 73-74) → "3"
            distance_aptitude = JRDBParser.safe_int(line[76:77])  # 距離適性 (1文字: 76-77) → "8"
            improvement = None  # 上昇度は別位置

            # === 基準オッズ・人気 (位置77以降) ===
            base_odds = JRDBParser.safe_float(line[78:83])  # 基準オッズ (5文字: 78-83) → "46.61"
            base_popularity = JRDBParser.safe_int(line[83:85])  # 人気順位 (2文字: 83-85) → "3 " → 3
            base_place_odds = JRDBParser.safe_float(line[86:91])  # 基準複勝オッズ (5文字: 86-91) → "8.613"
            base_place_popularity = JRDBParser.safe_int(line[89:91])  # 複勝人気 (2文字: 89-91) → "13"

            # === 特定情報マーク (位置93以降) ===
            specific_mark_circle = JRDBParser.safe_int(line[93:94]) if len(line) > 94 else None
            specific_mark_circle2 = JRDBParser.safe_int(line[94:95]) if len(line) > 95 else None
            specific_mark_triangle = JRDBParser.safe_int(line[95:96]) if len(line) > 96 else None
            specific_mark_triangle2 = JRDBParser.safe_int(line[96:97]) if len(line) > 97 else None
            specific_mark_x = JRDBParser.safe_int(line[97:98]) if len(line) > 98 else None

            # === 総合情報マーク (位置98以降) ===
            total_mark_circle = JRDBParser.safe_int(line[98:99]) if len(line) > 99 else None
            total_mark_circle2 = JRDBParser.safe_int(line[99:100]) if len(line) > 100 else None
            total_mark_triangle = JRDBParser.safe_int(line[100:101]) if len(line) > 101 else None
            total_mark_triangle2 = JRDBParser.safe_int(line[101:102]) if len(line) > 102 else None
            total_mark_x = JRDBParser.safe_int(line[102:103]) if len(line) > 103 else None

            # === 人気指数・調教指数・厩舎指数 ===
            popularity_index = JRDBParser.safe_int(line[103:106]) if len(line) > 106 else None
            training_index = JRDBParser.safe_float(line[106:111]) if len(line) > 111 else None
            stable_index = JRDBParser.safe_float(line[111:116]) if len(line) > 116 else None

            # === 調教矢印・厩舎評価 ===
            training_arrow_code = JRDBParser.safe_int(line[116:117]) if len(line) > 117 else None
            stable_eval_code = JRDBParser.safe_int(line[117:118]) if len(line) > 118 else None

            # === 騎手期待連対率 ===
            jockey_expected_win_rate = JRDBParser.safe_float(line[118:123]) if len(line) > 123 else None

            # === 激走指数 ===
            surge_index = JRDBParser.safe_int(line[123:126]) if len(line) > 126 else None

            # === 蹄・重適正・クラス ===
            hoof_code = JRDBParser.safe_int(line[126:128]) if len(line) > 128 else None
            heavy_aptitude_code = JRDBParser.safe_int(line[128:129]) if len(line) > 129 else None
            class_code = JRDBParser.safe_int(line[129:131]) if len(line) > 131 else None

            # === ブリンカー ===
            blinker = line[131:132].strip() if len(line) > 132 else ''

            # === 騎手名 (全角6文字 = 位置153-159) ===
            jockey_name = ''
            if len(line) > 159:
                jockey_name = line[153:159].strip().replace('　', '')

            # === 負担重量 (3桁、0.1kg単位: 159-162) → "550" → 55.0 ===
            weight_carried = None
            if len(line) > 162:
                weight_raw = JRDBParser.safe_int(line[159:162])
                if weight_raw:
                    weight_carried = weight_raw / 10.0

            # === 見習い区分 ===
            apprentice_class = None
            if len(line) > 163:
                apprentice_class = JRDBParser.safe_int(line[162:163])

            # === 調教師名 (全角6文字: 163-169) → "田中勝春" ===
            trainer_name = ''
            if len(line) > 169:
                trainer_name = line[163:169].strip().replace('　', '')

            # === 調教師所属 (位置169-171) → "美浦" ===
            trainer_affiliation = ''
            if len(line) > 171:
                trainer_affiliation = line[169:171].strip().replace('　', '')

            # === 前走レースキー (各16文字、位置171から開始) ===
            # フォーマット: 血統登録番号(8) + 年月日(8) = 16文字
            prev_race_key_1 = line[171:187].strip() if len(line) > 187 else ''
            prev_race_key_2 = line[187:203].strip() if len(line) > 203 else ''
            prev_race_key_3 = line[203:219].strip() if len(line) > 219 else ''
            prev_race_key_4 = line[219:235].strip() if len(line) > 235 else ''
            prev_race_key_5 = line[235:251].strip() if len(line) > 251 else ''

            # === 枠番 ===
            bracket_number = JRDBParser.safe_int(line[251:252]) if len(line) > 252 else None

            # === 各種印 ===
            overall_mark = JRDBParser.safe_int(line[286:287]) if len(line) > 287 else None
            idm_mark = JRDBParser.safe_int(line[287:288]) if len(line) > 288 else None
            info_mark = JRDBParser.safe_int(line[288:289]) if len(line) > 289 else None
            jockey_mark = JRDBParser.safe_int(line[289:290]) if len(line) > 290 else None
            stable_mark = JRDBParser.safe_int(line[290:291]) if len(line) > 291 else None
            training_mark = JRDBParser.safe_int(line[291:292]) if len(line) > 292 else None
            surge_mark = JRDBParser.safe_int(line[292:293]) if len(line) > 293 else None

            # === 芝・ダ適性 ===
            turf_aptitude = line[293:294].strip() if len(line) > 294 else ''
            dirt_aptitude = line[294:295].strip() if len(line) > 295 else ''

            # === 騎手コード・調教師コード ===
            jockey_code = line[295:300].strip() if len(line) > 300 else ''
            trainer_code = line[300:305].strip() if len(line) > 305 else ''

            # === 賞金 ===
            prize_money = JRDBParser.safe_int(line[305:310]) if len(line) > 310 else None
            earned_prize = JRDBParser.safe_int(line[310:315]) if len(line) > 315 else None
            condition_class = JRDBParser.safe_int(line[315:316]) if len(line) > 316 else None

            # === 展開予測指数 (位置326以降) ===
            # 分析結果: "-19.9-10.6-19.6 -8.0" at position 326
            ten_index = JRDBParser.safe_float(line[326:331]) if len(line) > 331 else None  # -19.9
            pace_index = JRDBParser.safe_float(line[331:336]) if len(line) > 336 else None  # -10.6
            agari_index = JRDBParser.safe_float(line[336:341]) if len(line) > 341 else None  # -19.6
            position_index = JRDBParser.safe_float(line[342:346]) if len(line) > 346 else None  # -8.0

            # === ペース予想 ===
            pace_forecast = line[346:347].strip() if len(line) > 347 else ''

            # === 道中順位・差・内外 ===
            mid_position = JRDBParser.safe_int(line[348:350]) if len(line) > 350 else None
            mid_gap = JRDBParser.safe_int(line[350:352]) if len(line) > 352 else None
            mid_inside_outside = JRDBParser.safe_int(line[352:353]) if len(line) > 353 else None

            # === 後3F順位・差・内外 ===
            last_3f_position = JRDBParser.safe_int(line[353:355]) if len(line) > 355 else None
            last_3f_gap = JRDBParser.safe_int(line[355:357]) if len(line) > 357 else None
            last_3f_inside_outside = JRDBParser.safe_int(line[357:358]) if len(line) > 358 else None

            # === ゴール順位・差・内外 ===
            goal_position = JRDBParser.safe_int(line[358:360]) if len(line) > 360 else None
            goal_gap = JRDBParser.safe_int(line[360:362]) if len(line) > 362 else None
            goal_inside_outside = JRDBParser.safe_int(line[362:363]) if len(line) > 363 else None

            # === 展開記号 ===
            development_code = line[363:364].strip() if len(line) > 364 else ''

            # === 馬体重 (位置364以降) ===
            confirmed_weight = JRDBParser.safe_int(line[373:376]) if len(line) > 376 else None
            confirmed_weight_diff = JRDBParser.safe_int(line[376:379]) if len(line) > 379 else None

            # === 性別・馬主・馬記号 ===
            sex_code = JRDBParser.safe_int(line[363:364]) if len(line) > 364 else None
            owner_name = line[379:399].strip().replace('　', '') if len(line) > 399 else ''

            return {
                'race_id': race_key,
                'horse_number': horse_number,
                'horse_id': horse_id,
                'horse_name': horse_name,
                'idm': idm,
                'jockey_index': jockey_index,
                'info_index': info_index,
                'total_index': total_index,
                'running_style': running_style,
                'distance_aptitude': distance_aptitude,
                'improvement': improvement,
                'rotation': None,
                'base_odds': base_odds,
                'base_popularity': base_popularity,
                'base_place_odds': base_place_odds,
                'base_place_popularity': base_place_popularity,
                'specific_mark_circle': specific_mark_circle,
                'specific_mark_circle2': specific_mark_circle2,
                'specific_mark_triangle': specific_mark_triangle,
                'specific_mark_triangle2': specific_mark_triangle2,
                'specific_mark_x': specific_mark_x,
                'total_mark_circle': total_mark_circle,
                'total_mark_circle2': total_mark_circle2,
                'total_mark_triangle': total_mark_triangle,
                'total_mark_triangle2': total_mark_triangle2,
                'total_mark_x': total_mark_x,
                'popularity_index': popularity_index,
                'training_index': training_index,
                'stable_index': stable_index,
                'training_arrow_code': training_arrow_code,
                'stable_eval_code': stable_eval_code,
                'jockey_expected_win_rate': jockey_expected_win_rate,
                'surge_index': surge_index,
                'hoof_code': hoof_code,
                'heavy_aptitude_code': heavy_aptitude_code,
                'class_code': class_code,
                'blinker': blinker if blinker else None,
                'jockey_name': jockey_name if jockey_name else None,
                'weight_carried': weight_carried,
                'apprentice_class': apprentice_class,
                'trainer_name': trainer_name if trainer_name else None,
                'trainer_affiliation': trainer_affiliation if trainer_affiliation else None,
                'prev_race_key_1': prev_race_key_1 if prev_race_key_1 else None,
                'prev_race_key_2': prev_race_key_2 if prev_race_key_2 else None,
                'prev_race_key_3': prev_race_key_3 if prev_race_key_3 else None,
                'prev_race_key_4': prev_race_key_4 if prev_race_key_4 else None,
                'prev_race_key_5': prev_race_key_5 if prev_race_key_5 else None,
                'bracket_number': bracket_number,
                'overall_mark': overall_mark,
                'idm_mark': idm_mark,
                'info_mark': info_mark,
                'jockey_mark': jockey_mark,
                'stable_mark': stable_mark,
                'training_mark': training_mark,
                'surge_mark': surge_mark,
                'turf_aptitude': turf_aptitude if turf_aptitude else None,
                'dirt_aptitude': dirt_aptitude if dirt_aptitude else None,
                'jockey_code': jockey_code if jockey_code else None,
                'trainer_code': trainer_code if trainer_code else None,
                'prize_money': prize_money,
                'earned_prize': earned_prize,
                'condition_class': condition_class,
                'ten_index': ten_index,
                'pace_index': pace_index,
                'agari_index': agari_index,
                'position_index': position_index,
                'pace_forecast': pace_forecast if pace_forecast else None,
                'mid_position': mid_position,
                'mid_gap': mid_gap,
                'mid_inside_outside': mid_inside_outside,
                'last_3f_position': last_3f_position,
                'last_3f_gap': last_3f_gap,
                'last_3f_inside_outside': last_3f_inside_outside,
                'goal_position': goal_position,
                'goal_gap': goal_gap,
                'goal_inside_outside': goal_inside_outside,
                'development_code': development_code if development_code else None,
                'distance_aptitude_2': None,
                'confirmed_weight': confirmed_weight,
                'confirmed_weight_diff': confirmed_weight_diff,
                'cancel_flag': None,
                'sex_code': sex_code,
                'owner_name': owner_name if owner_name else None,
                'owner_club_code': None,
                'horse_symbol_code': None,
                'surge_rank': None,
                'ls_index_rank': None,
                'ten_index_rank': None,
                'pace_index_rank': None,
                'agari_index_rank': None,
                'position_index_rank': None,
                'jockey_expected_win_single': None,
                'jockey_expected_top3': None,
                'transport_class': None,
                'running_form': None,
                'body_type': None,
                'body_total_1': None,
                'body_total_2': None,
                'body_total_3': None,
                'horse_note_1': None,
                'horse_note_2': None,
                'horse_note_3': None,
                'start_index': None,
                'late_start_rate': None,
                'reference_prev_race': None,
                'reference_jockey_code': None,
                'big_ticket_index': None,
                'big_ticket_mark': None,
                'demotion_flag': None,
                'surge_type': None,
                'rest_reason_code': None,
                'flags': None,
                'stable_entry_race_count': None,
                'stable_entry_date': None,
                'stable_entry_days_before': None,
                'pasture': None,
                'pasture_rank': None,
                'stable_rank': None,
                'created_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"Error parsing KYF line: {e}", exc_info=True)
            return None

    @staticmethod
    def parse_sec_line(line: str) -> Optional[Dict]:
        """
        SEC (成績データ) を解析 - 仕様書準拠

        UTF-8デコード後の文字位置に基づいて各フィールドを抽出します。
        レコード長: 約270文字 (UTF-8変換後)

        Args:
            line: 固定長レコード文字列 (UTF-8変換済み)

        Returns:
            解析結果の辞書 or None
        """
        try:
            if len(line) < 200:
                logger.warning(f"SEC line too short: {len(line)} chars")
                return None

            # === 基本情報 (ASCII部分) ===
            race_key = line[0:8].strip()           # レースキー
            horse_number = JRDBParser.safe_int(line[8:10])  # 馬番
            horse_id = line[10:18].strip()         # 血統登録番号

            # 年月日 (YYYYMMDD)
            race_date_str = line[18:26].strip()
            race_date = JRDBParser.parse_date(race_date_str)

            # === 馬名 (全角18文字 = 位置26-44) ===
            horse_name = line[26:44].strip().replace('　', '').strip()

            # === レース条件 ===
            distance = JRDBParser.safe_int(line[44:48])
            course_type_code = line[48:49]
            course_type = JRDBParser.COURSE_TYPE_MAP.get(course_type_code, None)

            direction_code = line[49:50]
            direction = 'right' if direction_code == '1' else 'left' if direction_code == '2' else 'straight'

            track_condition_code = line[51:53]
            track_condition = JRDBParser.TRACK_CONDITION_MAP.get(track_condition_code, None)

            race_type = line[53:55].strip()
            race_condition = line[55:57].strip()

            grade_code = line[61:62].strip()
            grade = None
            if grade_code == '1':
                grade = 'G1'
            elif grade_code == '2':
                grade = 'G2'
            elif grade_code == '3':
                grade = 'G3'
            elif grade_code == '5':
                grade = 'Listed'

            # === レース名 (全角25文字 = 位置62-87) ===
            race_name = line[62:87].strip().replace('　', '').strip()

            # 頭数
            num_horses = JRDBParser.safe_int(line[87:89])

            # === 馬成績 ===
            finish_position = JRDBParser.safe_int(line[93:95])
            abnormal_code = JRDBParser.safe_int(line[95:96])

            # タイム (0.1秒単位) → 秒に変換
            time_raw = JRDBParser.safe_int(line[96:100])
            finish_time = time_raw / 10.0 if time_raw else None

            # 斤量 (0.1kg単位) → kgに変換
            weight_raw = JRDBParser.safe_int(line[100:103])
            weight_carried = weight_raw / 10.0 if weight_raw else None

            # === 騎手名・調教師名 (全角6文字ずつ) ===
            jockey_name = line[103:109].strip().replace('　', '')
            trainer_name = line[109:115].strip().replace('　', '')

            # === オッズ・人気 ===
            win_odds = JRDBParser.safe_float(line[115:121])
            win_popularity = JRDBParser.safe_int(line[121:123])

            # === JRDB指数 ===
            idm = JRDBParser.safe_float(line[123:126])
            raw_score = JRDBParser.safe_float(line[126:129])
            track_bias = JRDBParser.safe_float(line[129:132])
            pace = JRDBParser.safe_float(line[132:135])
            late_start = JRDBParser.safe_float(line[135:138])
            position_fault = JRDBParser.safe_float(line[138:141])
            disadvantage = JRDBParser.safe_float(line[141:144])
            front_disadvantage = JRDBParser.safe_float(line[144:147])
            mid_disadvantage = JRDBParser.safe_float(line[147:150])
            back_disadvantage = JRDBParser.safe_float(line[150:153])
            race_score = JRDBParser.safe_float(line[153:156])

            # === コース取り・上昇度・クラス ===
            course_position = JRDBParser.safe_int(line[156:157])
            improvement_code = JRDBParser.safe_int(line[157:158])
            class_code = JRDBParser.safe_int(line[158:160])
            body_code = JRDBParser.safe_int(line[160:161])
            condition_code = JRDBParser.safe_int(line[161:162])

            # === ペース ===
            race_pace = line[162:163].strip() if len(line) > 163 else ''
            horse_pace = line[163:164].strip() if len(line) > 164 else ''

            # === 指数群 ===
            ten_index = JRDBParser.safe_float(line[164:169]) if len(line) > 169 else None
            agari_index = JRDBParser.safe_float(line[169:174]) if len(line) > 174 else None
            pace_index = JRDBParser.safe_float(line[174:179]) if len(line) > 179 else None
            race_pace_index = JRDBParser.safe_float(line[179:184]) if len(line) > 184 else None

            # === 1(2)着馬名 (全角6文字) ===
            winner_name = line[184:190].strip().replace('　', '') if len(line) > 190 else ''

            # 1着タイム差 (0.1秒単位)
            winner_diff_raw = JRDBParser.safe_int(line[190:193]) if len(line) > 193 else None
            winner_time_diff = winner_diff_raw / 10.0 if winner_diff_raw else None

            # 前3F/後3Fタイム (0.1秒単位)
            front_3f_raw = JRDBParser.safe_int(line[193:196]) if len(line) > 196 else None
            front_3f_time = front_3f_raw / 10.0 if front_3f_raw else None

            last_3f_raw = JRDBParser.safe_int(line[196:199]) if len(line) > 199 else None
            last_3f_time = last_3f_raw / 10.0 if last_3f_raw else None

            # === 備考 (全角12文字 = 位置199-211) ===
            remarks = line[199:211].strip().replace('　', '').strip() if len(line) > 211 else ''

            # === 馬体重関連フィールド (絶対位置で取得) ===
            # SEC仕様: 馬体重(333-335), 増減(336-338), 天候(339), コース(340), 脚質(341)
            # UTF-8位置: 261-264, 264-267, 267, 268, 269
            horse_weight = None
            horse_weight_diff = None
            weather_code = None
            course_code = None
            race_running_style = None

            if len(line) >= 270:
                # 馬体重 (位置261-264)
                horse_weight = JRDBParser.safe_int(line[261:264])

                # 馬体重増減 (位置264-267、符号付き)
                weight_diff_str = line[264:267].strip()
                if weight_diff_str:
                    # 符号を処理 (+10, -5, など)
                    weight_diff_str = weight_diff_str.replace('+', '')
                    horse_weight_diff = JRDBParser.safe_int(weight_diff_str)

                # 天候コード (位置267)
                weather_code = JRDBParser.safe_int(line[267:268])

                # コース (位置268)
                course_code = line[268:269].strip() if len(line) > 268 and line[268:269].strip() else None

                # レース脚質 (位置269)
                race_running_style = line[269:270].strip() if len(line) > 269 and line[269:270].strip() else None

            # === 中間フィールド (位置が複雑なため概算) ===
            # 複勝オッズ・10時オッズ・コーナー順位など
            place_odds = None
            odds_10am_win = None
            odds_10am_place = None
            corner_position_1 = None
            corner_position_2 = None
            corner_position_3 = None
            corner_position_4 = None
            front_3f_lead_diff = None
            last_3f_lead_diff = None
            jockey_code = None
            trainer_code = None

            # 中間フィールドの抽出を試みる (位置211以降)
            if len(line) >= 260:
                # 10時オッズ付近を探す
                odds_10am_win = JRDBParser.safe_float(line[219:225]) if len(line) > 225 else None
                odds_10am_place = JRDBParser.safe_float(line[225:231]) if len(line) > 231 else None

            return {
                'race_id': race_key,
                'horse_number': horse_number,
                'horse_id': horse_id,
                'race_date': race_date,
                'horse_name': horse_name if horse_name else None,
                'distance': distance,
                'course_type': course_type,
                'direction': direction,
                'track_condition': track_condition,
                'race_type': race_type if race_type else None,
                'race_condition': race_condition if race_condition else None,
                'grade': grade,
                'race_name': race_name if race_name else None,
                'num_horses': num_horses,
                'finish_position': finish_position,
                'abnormal_code': abnormal_code,
                'finish_time': finish_time,
                'weight_carried': weight_carried,
                'jockey_name': jockey_name if jockey_name else None,
                'trainer_name': trainer_name if trainer_name else None,
                'win_odds': win_odds,
                'win_popularity': win_popularity,
                'idm': idm,
                'raw_score': raw_score,
                'track_bias': track_bias,
                'pace': pace,
                'late_start': late_start,
                'position_fault': position_fault,
                'disadvantage': disadvantage,
                'front_disadvantage': front_disadvantage,
                'mid_disadvantage': mid_disadvantage,
                'back_disadvantage': back_disadvantage,
                'race_score': race_score,
                'course_position': course_position,
                'improvement_code': improvement_code,
                'class_code': class_code,
                'body_code': body_code,
                'condition_code': condition_code,
                'race_pace': race_pace if race_pace else None,
                'horse_pace': horse_pace if horse_pace else None,
                'ten_index': ten_index,
                'agari_index': agari_index,
                'pace_index': pace_index,
                'race_pace_index': race_pace_index,
                'winner_name': winner_name if winner_name else None,
                'winner_time_diff': winner_time_diff,
                'front_3f_time': front_3f_time,
                'last_3f_time': last_3f_time,
                'remarks': remarks if remarks else None,
                'place_odds': place_odds,
                'odds_10am_win': odds_10am_win,
                'odds_10am_place': odds_10am_place,
                'corner_position_1': corner_position_1,
                'corner_position_2': corner_position_2,
                'corner_position_3': corner_position_3,
                'corner_position_4': corner_position_4,
                'front_3f_lead_diff': front_3f_lead_diff,
                'last_3f_lead_diff': last_3f_lead_diff,
                'jockey_code': jockey_code,
                'trainer_code': trainer_code,
                'horse_weight': horse_weight,
                'horse_weight_diff': horse_weight_diff,
                'weather_code': weather_code,
                'course_code': course_code,
                'race_running_style': race_running_style,
                'created_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"Error parsing SEC line: {e}", exc_info=True)
            return None

    # 性別コードマッピング
    SEX_CODE_MAP = {
        '1': '牡',
        '2': '牝',
        '3': 'セン'
    }

    @staticmethod
    def parse_ukc_line(line: str) -> Optional[Dict]:
        """
        UKC (馬基本データ) を解析

        UTF-8デコード後の文字位置に基づいて各フィールドを抽出します。

        Args:
            line: 固定長レコード文字列 (UTF-8変換済み)

        Returns:
            解析結果の辞書 or None
        """
        try:
            if len(line) < 160:
                logger.warning(f"UKC line too short: {len(line)} chars")
                return None

            # === 基本情報 ===
            horse_id = line[0:8].strip()
            horse_name = line[8:26].strip().replace('　', '').strip()

            # 性別・毛色・馬記号
            sex_code = JRDBParser.safe_int(line[26:27])
            sex = JRDBParser.SEX_CODE_MAP.get(line[26:27], None)
            coat_color_code = JRDBParser.safe_int(line[27:29])
            horse_symbol_code = JRDBParser.safe_int(line[29:31])

            # === 血統情報 ===
            sire_name = line[31:49].strip().replace('　', '').strip()
            dam_name = line[49:67].strip().replace('　', '').strip()
            broodmare_sire_name = line[67:85].strip().replace('　', '').strip()

            # === 生年月日 ===
            birth_date_str = line[85:93].strip()
            birth_date = JRDBParser.parse_date(birth_date_str)

            # === 血統キー用生年 ===
            sire_birth_year = JRDBParser.safe_int(line[93:97])
            dam_birth_year = JRDBParser.safe_int(line[97:101])
            broodmare_sire_birth_year = JRDBParser.safe_int(line[101:105])

            # === 馬主・生産者 ===
            owner_name = line[105:125].strip().replace('　', '').strip() if len(line) > 125 else ''
            owner_club_code = JRDBParser.safe_int(line[125:127]) if len(line) > 127 else None
            breeder_name = line[127:147].strip().replace('　', '').strip() if len(line) > 147 else ''
            birthplace = line[147:151].strip().replace('　', '').strip() if len(line) > 151 else ''

            # === その他 ===
            deletion_flag = JRDBParser.safe_int(line[151:152]) if len(line) > 152 else None

            data_date_str = line[152:160].strip() if len(line) > 160 else ''
            data_date = JRDBParser.parse_date(data_date_str)

            sire_line_code = line[160:164].strip() if len(line) > 164 else ''
            broodmare_sire_line_code = line[164:168].strip() if len(line) > 168 else ''

            return {
                'horse_id': horse_id,
                'horse_name': horse_name if horse_name else None,
                'sex_code': sex_code,
                'sex': sex,
                'coat_color_code': coat_color_code,
                'horse_symbol_code': horse_symbol_code,
                'sire_name': sire_name if sire_name else None,
                'dam_name': dam_name if dam_name else None,
                'broodmare_sire_name': broodmare_sire_name if broodmare_sire_name else None,
                'birth_date': birth_date,
                'sire_birth_year': sire_birth_year,
                'dam_birth_year': dam_birth_year,
                'broodmare_sire_birth_year': broodmare_sire_birth_year,
                'owner_name': owner_name if owner_name else None,
                'owner_club_code': owner_club_code,
                'breeder_name': breeder_name if breeder_name else None,
                'birthplace': birthplace if birthplace else None,
                'deletion_flag': deletion_flag,
                'data_date': data_date,
                'sire_line_code': sire_line_code if sire_line_code else None,
                'broodmare_sire_line_code': broodmare_sire_line_code if broodmare_sire_line_code else None,
                'created_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"Error parsing UKC line: {e}", exc_info=True)
            return None

    @staticmethod
    def parse_zz9x4(s: str) -> dict:
        """
        ZZ9*4形式 (3桁×4グループ = 12文字) をパース

        Args:
            s: 12文字の文字列 (1着数/2着数/3着数/着外数)

        Returns:
            {'win': int, 'place': int, 'show': int, 'out': int}
        """
        if len(s) < 12:
            return {'win': None, 'place': None, 'show': None, 'out': None}
        return {
            'win': JRDBParser.safe_int(s[0:3]),
            'place': JRDBParser.safe_int(s[3:6]),
            'show': JRDBParser.safe_int(s[6:9]),
            'out': JRDBParser.safe_int(s[9:12])
        }

    @staticmethod
    def parse_kka_line(line: str) -> Optional[Dict]:
        """
        KKA (競走馬拡張データ) を解析

        レコード長: 324バイト (仕様書)、UTF-8変換後は約306文字
        各成績はZZ9*4形式 (3桁×4 = 12文字: 1着/2着/3着/着外)

        Args:
            line: 固定長レコード文字列 (UTF-8変換済み)

        Returns:
            解析結果の辞書 or None
        """
        try:
            if len(line) < 280:
                logger.warning(f"KKA line too short: {len(line)} chars")
                return None

            # === レースキー + 馬番 ===
            race_key = line[0:8].strip()
            horse_number = JRDBParser.safe_int(line[8:10])

            # ファイル名から日付を取得 (レースキーから推測)
            # レースキー: 場コード(2) + 年(2) + 回(1) + 日(1) + R(2)
            year_code = race_key[2:4]
            year = int(year_code) + 2000 if int(year_code) <= 50 else int(year_code) + 1900
            # 日付はファイル名から取得するため、ここでは設定しない
            race_date = None

            # === 着度数 (基本成績) ===
            jra = JRDBParser.parse_zz9x4(line[10:22])
            exchange = JRDBParser.parse_zz9x4(line[22:34])
            other = JRDBParser.parse_zz9x4(line[34:46])

            # === 競走馬該当条件別着度数集計 ===
            surface = JRDBParser.parse_zz9x4(line[46:58])           # 芝ダ障害別
            surface_dist = JRDBParser.parse_zz9x4(line[58:70])      # 芝ダ障害別距離
            track_dist = JRDBParser.parse_zz9x4(line[70:82])        # トラック距離
            rotation = JRDBParser.parse_zz9x4(line[82:94])          # ローテ
            direction = JRDBParser.parse_zz9x4(line[94:106])        # 回り
            jockey = JRDBParser.parse_zz9x4(line[106:118])          # 騎手
            good = JRDBParser.parse_zz9x4(line[118:130])            # 良
            slightly_heavy = JRDBParser.parse_zz9x4(line[130:142])  # 稍重
            heavy = JRDBParser.parse_zz9x4(line[142:154])           # 重不良
            slow_pace = JRDBParser.parse_zz9x4(line[154:166])       # Sペース
            medium_pace = JRDBParser.parse_zz9x4(line[166:178])     # Mペース
            high_pace = JRDBParser.parse_zz9x4(line[178:190])       # Hペース
            season = JRDBParser.parse_zz9x4(line[190:202])          # 季節
            bracket = JRDBParser.parse_zz9x4(line[202:214])         # 枠

            # === 該当条件別着度数 ===
            jockey_dist = JRDBParser.parse_zz9x4(line[214:226])            # 騎手距離
            jockey_track_dist = JRDBParser.parse_zz9x4(line[226:238])      # 騎手トラック距離
            jockey_trainer = JRDBParser.parse_zz9x4(line[238:250])         # 騎手調教師
            jockey_owner = JRDBParser.parse_zz9x4(line[250:262])           # 騎手馬主
            jockey_blinker = JRDBParser.parse_zz9x4(line[262:274])         # 騎手ブリンカ
            trainer_owner = JRDBParser.parse_zz9x4(line[274:286])          # 調教師馬主

            # === 血統データ ===
            sire_turf_place_rate = JRDBParser.safe_int(line[286:289])
            sire_dirt_place_rate = JRDBParser.safe_int(line[289:292])
            sire_avg_place_distance = JRDBParser.safe_int(line[292:296])
            broodmare_sire_turf_place_rate = JRDBParser.safe_int(line[296:299])
            broodmare_sire_dirt_place_rate = JRDBParser.safe_int(line[299:302])
            broodmare_sire_avg_place_distance = JRDBParser.safe_int(line[302:306])

            return {
                'race_id': race_key,
                'horse_number': horse_number,
                'race_date': race_date,

                # JRA成績
                'jra_win': jra['win'],
                'jra_place': jra['place'],
                'jra_show': jra['show'],
                'jra_out': jra['out'],

                # 交流成績
                'exchange_win': exchange['win'],
                'exchange_place': exchange['place'],
                'exchange_show': exchange['show'],
                'exchange_out': exchange['out'],

                # 他成績
                'other_win': other['win'],
                'other_place': other['place'],
                'other_show': other['show'],
                'other_out': other['out'],

                # 芝ダ障害別成績
                'surface_win': surface['win'],
                'surface_place': surface['place'],
                'surface_show': surface['show'],
                'surface_out': surface['out'],

                # 芝ダ障害別距離成績
                'surface_dist_win': surface_dist['win'],
                'surface_dist_place': surface_dist['place'],
                'surface_dist_show': surface_dist['show'],
                'surface_dist_out': surface_dist['out'],

                # トラック距離成績
                'track_dist_win': track_dist['win'],
                'track_dist_place': track_dist['place'],
                'track_dist_show': track_dist['show'],
                'track_dist_out': track_dist['out'],

                # ローテ成績
                'rotation_win': rotation['win'],
                'rotation_place': rotation['place'],
                'rotation_show': rotation['show'],
                'rotation_out': rotation['out'],

                # 回り成績
                'direction_win': direction['win'],
                'direction_place': direction['place'],
                'direction_show': direction['show'],
                'direction_out': direction['out'],

                # 騎手成績
                'jockey_win': jockey['win'],
                'jockey_place': jockey['place'],
                'jockey_show': jockey['show'],
                'jockey_out': jockey['out'],

                # 良馬場成績
                'good_win': good['win'],
                'good_place': good['place'],
                'good_show': good['show'],
                'good_out': good['out'],

                # 稍重成績
                'slightly_heavy_win': slightly_heavy['win'],
                'slightly_heavy_place': slightly_heavy['place'],
                'slightly_heavy_show': slightly_heavy['show'],
                'slightly_heavy_out': slightly_heavy['out'],

                # 重不良成績
                'heavy_win': heavy['win'],
                'heavy_place': heavy['place'],
                'heavy_show': heavy['show'],
                'heavy_out': heavy['out'],

                # Sペース成績
                'slow_pace_win': slow_pace['win'],
                'slow_pace_place': slow_pace['place'],
                'slow_pace_show': slow_pace['show'],
                'slow_pace_out': slow_pace['out'],

                # Mペース成績
                'medium_pace_win': medium_pace['win'],
                'medium_pace_place': medium_pace['place'],
                'medium_pace_show': medium_pace['show'],
                'medium_pace_out': medium_pace['out'],

                # Hペース成績
                'high_pace_win': high_pace['win'],
                'high_pace_place': high_pace['place'],
                'high_pace_show': high_pace['show'],
                'high_pace_out': high_pace['out'],

                # 季節成績
                'season_win': season['win'],
                'season_place': season['place'],
                'season_show': season['show'],
                'season_out': season['out'],

                # 枠成績
                'bracket_win': bracket['win'],
                'bracket_place': bracket['place'],
                'bracket_show': bracket['show'],
                'bracket_out': bracket['out'],

                # 騎手距離成績
                'jockey_dist_win': jockey_dist['win'],
                'jockey_dist_place': jockey_dist['place'],
                'jockey_dist_show': jockey_dist['show'],
                'jockey_dist_out': jockey_dist['out'],

                # 騎手トラック距離成績
                'jockey_track_dist_win': jockey_track_dist['win'],
                'jockey_track_dist_place': jockey_track_dist['place'],
                'jockey_track_dist_show': jockey_track_dist['show'],
                'jockey_track_dist_out': jockey_track_dist['out'],

                # 騎手調教師成績
                'jockey_trainer_win': jockey_trainer['win'],
                'jockey_trainer_place': jockey_trainer['place'],
                'jockey_trainer_show': jockey_trainer['show'],
                'jockey_trainer_out': jockey_trainer['out'],

                # 騎手馬主成績
                'jockey_owner_win': jockey_owner['win'],
                'jockey_owner_place': jockey_owner['place'],
                'jockey_owner_show': jockey_owner['show'],
                'jockey_owner_out': jockey_owner['out'],

                # 騎手ブリンカ成績
                'jockey_blinker_win': jockey_blinker['win'],
                'jockey_blinker_place': jockey_blinker['place'],
                'jockey_blinker_show': jockey_blinker['show'],
                'jockey_blinker_out': jockey_blinker['out'],

                # 調教師馬主成績
                'trainer_owner_win': trainer_owner['win'],
                'trainer_owner_place': trainer_owner['place'],
                'trainer_owner_show': trainer_owner['show'],
                'trainer_owner_out': trainer_owner['out'],

                # 血統データ
                'sire_turf_place_rate': sire_turf_place_rate,
                'sire_dirt_place_rate': sire_dirt_place_rate,
                'sire_avg_place_distance': sire_avg_place_distance,
                'broodmare_sire_turf_place_rate': broodmare_sire_turf_place_rate,
                'broodmare_sire_dirt_place_rate': broodmare_sire_dirt_place_rate,
                'broodmare_sire_avg_place_distance': broodmare_sire_avg_place_distance,

                'created_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"Error parsing KKA line: {e}", exc_info=True)
            return None

    @staticmethod
    def parse_kaa_line(line: str) -> Optional[Dict]:
        """
        KAA (開催データ) を解析

        馬場予想や天候予想等の開催に対するデータ。
        レコード長: 56バイト (仕様書)、UTF-8変換後は約46文字

        Args:
            line: 固定長レコード文字列 (UTF-8変換済み)

        Returns:
            解析結果の辞書 or None
        """
        try:
            if len(line) < 40:
                logger.warning(f"KAA line too short: {len(line)} chars")
                return None

            # === 開催キー ===
            venue_code = line[0:2].strip()
            year_code = line[2:4].strip()
            kai = JRDBParser.safe_int(line[4:5])
            day_hex = line[5:6].strip()

            # 開催ID生成 (場コード + 年 + 回 + 日)
            venue_id = f"{venue_code}{year_code}{kai}{day_hex}"

            # 年の補完
            year = JRDBParser.safe_int(year_code)
            if year is not None:
                year = year + 2000 if year <= 50 else year + 1900

            # === 基本情報 ===
            race_date = JRDBParser.parse_date(line[6:14])
            region_code = JRDBParser.safe_int(line[14:15])
            day_of_week = line[15:16].strip()
            venue_name = line[16:18].strip().replace('　', '')

            # === 天候・芝馬場 ===
            weather_code = JRDBParser.safe_int(line[18:19])
            turf_condition_code = JRDBParser.safe_int(line[19:21])
            turf_condition_inner = JRDBParser.safe_int(line[21:22])
            turf_condition_middle = JRDBParser.safe_int(line[22:23])
            turf_condition_outer = JRDBParser.safe_int(line[23:24])
            turf_bias = JRDBParser.safe_int(line[24:27])

            # === 直線馬場差 ===
            straight_bias_innermost = JRDBParser.safe_int(line[27:29])
            straight_bias_inner = JRDBParser.safe_int(line[29:31])
            straight_bias_middle = JRDBParser.safe_int(line[31:33])
            straight_bias_outer = JRDBParser.safe_int(line[33:35])
            straight_bias_outermost = JRDBParser.safe_int(line[35:37])

            # === ダート馬場 ===
            dirt_condition_code = JRDBParser.safe_int(line[37:39])
            dirt_condition_inner = JRDBParser.safe_int(line[39:40])
            dirt_condition_middle = JRDBParser.safe_int(line[40:41])
            dirt_condition_outer = JRDBParser.safe_int(line[41:42])
            dirt_bias = JRDBParser.safe_int(line[42:45])

            # === データ区分 ===
            data_category = JRDBParser.safe_int(line[45:46]) if len(line) > 45 else None

            return {
                'venue_id': venue_id,
                'race_date': race_date,
                'venue_code': venue_code,
                'venue_name': venue_name if venue_name else JRDBParser.VENUE_CODE_MAP.get(venue_code, ''),
                'year': year,
                'kai': kai,
                'day': day_hex,
                'region_code': region_code,
                'day_of_week': day_of_week if day_of_week else None,

                'weather_code': weather_code,

                'turf_condition_code': turf_condition_code,
                'turf_condition_inner': turf_condition_inner,
                'turf_condition_middle': turf_condition_middle,
                'turf_condition_outer': turf_condition_outer,
                'turf_bias': turf_bias,

                'straight_bias_innermost': straight_bias_innermost,
                'straight_bias_inner': straight_bias_inner,
                'straight_bias_middle': straight_bias_middle,
                'straight_bias_outer': straight_bias_outer,
                'straight_bias_outermost': straight_bias_outermost,

                'dirt_condition_code': dirt_condition_code,
                'dirt_condition_inner': dirt_condition_inner,
                'dirt_condition_middle': dirt_condition_middle,
                'dirt_condition_outer': dirt_condition_outer,
                'dirt_bias': dirt_bias,

                'data_category': data_category,

                'created_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"Error parsing KAA line: {e}", exc_info=True)
            return None

    @staticmethod
    def parse_file(file_content: str, data_type: str) -> List[Dict]:
        """
        ファイル全体を解析

        Args:
            file_content: ファイルの内容 (文字列)
            data_type: データタイプ (BAA, KYF, SEC など)

        Returns:
            解析結果のリスト
        """
        results = []
        lines = file_content.strip().split('\n')

        parser_map = {
            'BAA': JRDBParser.parse_baa_line,
            'BAB': JRDBParser.parse_baa_line,
            'BAC': JRDBParser.parse_baa_line,
            'KYF': JRDBParser.parse_kyf_line,
            'KYG': JRDBParser.parse_kyf_line,
            'KYH': JRDBParser.parse_kyf_line,
            'SEC': JRDBParser.parse_sec_line,
            'UKC': JRDBParser.parse_ukc_line,
            'KKA': JRDBParser.parse_kka_line,
            'KAA': JRDBParser.parse_kaa_line,
        }

        parser_func = parser_map.get(data_type.upper())
        if not parser_func:
            logger.warning(f"No parser for data type: {data_type}")
            return results

        for i, line in enumerate(lines):
            if not line or line.strip() == '':
                continue

            try:
                parsed = parser_func(line)
                if parsed:
                    results.append(parsed)
            except Exception as e:
                logger.error(f"Error parsing line {i+1}: {e}", exc_info=True)

        logger.info(f"Parsed {len(results)} records from {data_type} file")
        return results
