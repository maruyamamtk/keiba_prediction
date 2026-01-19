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
            if len(line) < 100:
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
        KYF (競走馬データ) を解析

        Args:
            line: 固定長レコード文字列

        Returns:
            解析結果の辞書 or None
        """
        try:
            if len(line) < 500:
                logger.warning(f"KYF line too short: {len(line)} bytes")
                return None

            # レースキー
            race_key = line[0:8].strip()

            # 馬番
            horse_number = JRDBParser.safe_int(line[8:10])

            # 血統登録番号 (馬ID)
            horse_id = line[10:18].strip()

            # 馬名
            horse_name = line[18:54].strip()

            # IDM・指数
            idm = JRDBParser.safe_float(line[54:59])
            jockey_index = JRDBParser.safe_float(line[59:64])
            info_index = JRDBParser.safe_float(line[64:69])
            total_index = JRDBParser.safe_float(line[69:74])

            # 脚質
            pace_type = line[74:75].strip()

            # 距離適性
            distance_aptitude = line[75:76].strip()

            # 基準オッズ
            base_odds = JRDBParser.safe_float(line[101:106])

            # 人気順位
            popularity = JRDBParser.safe_int(line[106:108])

            # 枠番
            bracket_number = JRDBParser.safe_int(line[115:116])

            # 斤量
            weight = JRDBParser.safe_int(line[137:140])

            # 騎手コード・名前
            jockey_code = line[165:170].strip()
            jockey_name = line[170:182].strip()

            # 調教師コード・名前
            trainer_code = line[182:187].strip()
            trainer_name = line[187:199].strip()

            return {
                'race_id': race_key,
                'horse_id': horse_id,
                'horse_name': horse_name,
                'bracket_number': bracket_number,
                'horse_number': horse_number,
                'finish_position': None,  # KYFでは未定
                'finish_time': None,
                'last_3f_time': None,
                'passing_order': None,
                'odds': base_odds,
                'popularity': popularity,
                'weight': weight,
                'jockey_id': jockey_code,
                'jockey_name': jockey_name,
                'trainer_id': trainer_code,
                'trainer_name': trainer_name,
                'horse_weight': None,  # KYFでは未定
                'horse_weight_diff': None,
                'idm': idm,
                'jockey_index': jockey_index,
                'info_index': info_index,
                'total_index': total_index,
                'race_pace': pace_type,
                'horse_pace': distance_aptitude,
                'created_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"Error parsing KYF line: {e}", exc_info=True)
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
