"""
バックテストシミュレーター

学習済みモデルの予測スコア（複勝率）と過去のオッズ・払戻データを使用して
投資シミュレーションを行う。

投資ルール:
- 期待回収率フィルタ: 予測複勝率 × オッズ > 閾値 の馬のみ購入
- 賭け金計算: Fractional Kelly 基準（budget_per_race を上限とする）
- 1レースあたり上限: budget_per_race (円) の固定予算
- 最低賭け金: 100円単位に切り捨て
"""



import logging
from dataclasses import asdict, dataclass


import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_VENUE_NAME_MAP: dict[str, str] = {
    "01": "札幌",
    "02": "函館",
    "03": "福島",
    "04": "新潟",
    "05": "東京",
    "06": "中山",
    "07": "中京",
    "08": "京都",
    "09": "阪神",
    "10": "小倉",
}


def kelly_criterion(win_prob: float, odds: float) -> float:
    """
    Kelly 基準: 最適賭け金比率を計算する

    f* = (p × (odds - 1) - (1 - p)) / (odds - 1)

    Args:
        win_prob: 的中確率 (0〜1)
        odds: オッズ (1.0 以上)

    Returns:
        資金に対する賭け金の割合 (0〜1、負の場合は 0)
    """
    if odds <= 1.0:
        return 0.0
    kelly = (win_prob * (odds - 1.0) - (1.0 - win_prob)) / (odds - 1.0)
    return max(0.0, kelly)


def fractional_kelly(win_prob: float, odds: float, fraction: float = 0.25) -> float:
    """
    Fractional Kelly: リスク調整済み Kelly 基準

    Args:
        win_prob: 的中確率 (0〜1)
        odds: オッズ (1.0 以上)
        fraction: Kelly 値に掛ける係数 (デフォルト: 0.25)

    Returns:
        資金に対する賭け金の割合 (0〜1)
    """
    return kelly_criterion(win_prob, odds) * fraction


@dataclass
class BetRecord:
    """個別の賭けの記録"""

    race_id: str
    race_date: object
    horse_id: str
    horse_number: int
    horse_name: str | None
    win_place_prob: float
    odds: float
    expected_return: float
    kelly_frac: float
    bet_amount: float
    finish_position: int | None
    is_hit: bool
    payout_per_100: int | None
    return_amount: float
    profit: float
    capital_after: float


class BacktestSimulator:
    """
    バックテストシミュレーター

    複勝（place bet）を対象に投資シミュレーションを行う。
    """

    def __init__(
        self,
        initial_capital: float = 100_000.0,
        kelly_fraction: float = 0.25,
        expected_return_threshold: float = 1.2,
        budget_per_race: float = 3000.0,
        min_bet_amount: float = 100.0,
        odds_column: str = "odds_yesterday",
        show_race_summary: bool = True,
        # 後方互換性のための旧パラメータ（無視される）
        max_bet_ratio: float | None = None,
    ):
        """
        初期化

        Args:
            initial_capital: 初期資金 (円)
            kelly_fraction: Fractional Kelly の係数
            expected_return_threshold: 期待回収率フィルタ閾値
                (予測複勝率 × オッズ > 閾値 の馬のみ購入)
            budget_per_race: 1レースあたりの固定予算 (円)。Kelly で計算した賭け金の上限
            min_bet_amount: 最低賭け金 (円)
            odds_column: 意思決定に使用するオッズカラム名
            show_race_summary: True のときレース単位のサマリーログを出力する
        """
        self.initial_capital = initial_capital
        self.kelly_fraction = kelly_fraction
        self.expected_return_threshold = expected_return_threshold
        self.budget_per_race = budget_per_race
        self.min_bet_amount = min_bet_amount
        self.odds_column = odds_column
        self.show_race_summary = show_race_summary

    def run(
        self,
        predictions_df: pd.DataFrame,
        payouts_df: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """
        バックテストを実行する

        Args:
            predictions_df: 予測結果 DataFrame
                必須カラム:
                  - race_id, race_date, horse_id, horse_number
                  - win_place_prob (予測複勝率: 0〜1)
                  - finish_position (実際の着順)
                  - odds_column で指定したカラム (オッズ)
            payouts_df: 払戻情報 DataFrame (raw.payouts の place データ)
                カラム: race_id, horse_number_1, payout_amount, bet_type
                None の場合は odds_column からリターンを推定する

        Returns:
            賭けの記録 DataFrame (BetRecord フィールド)
            賭け対象がない場合は空の DataFrame を返す
        """
        if self.odds_column not in predictions_df.columns:
            logger.warning(
                f"odds_column '{self.odds_column}' が見つかりません。"
                f"利用可能なカラム: {list(predictions_df.columns)}"
            )
            return pd.DataFrame()

        # 払戻マップを構築: (race_id, horse_number) -> payout_amount
        payout_map: dict[tuple, int] = {}
        if payouts_df is not None and len(payouts_df) > 0:
            place_df = payouts_df[payouts_df["bet_type"] == "place"]
            for _, row in place_df.iterrows():
                key = (str(row["race_id"]), int(row["horse_number_1"]))
                payout_map[key] = int(row["payout_amount"])

        capital = float(self.initial_capital)
        records: list[BetRecord] = []

        # 日付・レース順にソート
        df = predictions_df.sort_values(
            ["race_date", "race_id", "horse_number"]
        ).copy()
        odds_series = pd.to_numeric(df[self.odds_column], errors="coerce")
        df = df.assign(_odds=odds_series)

        has_venue_code = "venue_code" in df.columns
        has_race_number = "race_number" in df.columns
        has_horse_name = "horse_name" in df.columns

        for race_id, race_df in df.groupby("race_id", sort=False):
            race_date = race_df["race_date"].iloc[0]

            # 競馬場名・レース番号ラベルを構築
            if has_venue_code:
                venue_code = str(race_df["venue_code"].iloc[0])
                venue_name = _VENUE_NAME_MAP.get(venue_code, venue_code)
            else:
                venue_name = "不明"
            if has_race_number:
                race_num_str = f"{int(race_df['race_number'].iloc[0])}R"
            else:
                race_num_str = "?R"
            race_label = f"{race_date} {venue_name} {race_num_str}"

            # レース着順の完全性チェック:
            # 有効な着順（1以上の整数）を持つ馬が1頭もいない場合は
            # 着順データが集計できていないレースとしてスキップする
            if "finish_position" in race_df.columns:
                valid_finish = pd.to_numeric(
                    race_df["finish_position"], errors="coerce"
                )
                if (valid_finish >= 1).sum() == 0:
                    logger.info(
                        f"[スキップ] {race_label}"
                        " | 有効な着順データがないためシミュレーション対象外"
                    )
                    continue

            race_records: list[BetRecord] = []

            for idx, row in race_df.iterrows():
                odds = row["_odds"]
                if pd.isna(odds) or odds <= 0:
                    continue

                win_place_prob = float(row["win_place_prob"])
                odds = float(odds)
                expected_return = win_place_prob * odds

                # 期待回収率フィルタ
                if expected_return <= self.expected_return_threshold:
                    continue

                # Fractional Kelly で賭け金計算（budget_per_race を上限とする）
                kf = fractional_kelly(win_place_prob, odds, self.kelly_fraction)
                bet_amount = capital * kf
                bet_amount = min(bet_amount, self.budget_per_race)
                # 100円単位に切り捨て
                bet_amount = np.floor(bet_amount / 100.0) * 100.0
                bet_amount = max(self.min_bet_amount, bet_amount)

                if bet_amount > capital:
                    continue

                # 着順の確認
                finish_raw = row.get("finish_position", None)
                if finish_raw is None or pd.isna(finish_raw):
                    finish_position = None
                    is_hit = False
                elif int(finish_raw) == 0:
                    # 着順0 = 出走取消・競走中止などの無効データ → 賭け対象外
                    continue
                else:
                    finish_position = int(finish_raw)
                    is_hit = (1 <= finish_position <= 3)

                # 払戻金の計算
                horse_number = int(row["horse_number"])
                payout_per_100 = payout_map.get((str(race_id), horse_number), None)
                horse_name: str | None = (
                    str(row["horse_name"])
                    if has_horse_name and pd.notna(row.get("horse_name"))
                    else None
                )

                if is_hit:
                    if payout_per_100 is not None:
                        return_amount = bet_amount * (payout_per_100 / 100.0)
                    else:
                        # 払戻データなし: オッズで推定
                        return_amount = bet_amount * odds
                else:
                    return_amount = 0.0

                profit = return_amount - bet_amount
                capital += profit

                record = BetRecord(
                    race_id=str(race_id),
                    race_date=race_date,
                    horse_id=str(row["horse_id"]),
                    horse_number=horse_number,
                    horse_name=horse_name,
                    win_place_prob=win_place_prob,
                    odds=odds,
                    expected_return=expected_return,
                    kelly_frac=kf,
                    bet_amount=bet_amount,
                    finish_position=finish_position,
                    is_hit=is_hit,
                    payout_per_100=payout_per_100,
                    return_amount=return_amount,
                    profit=profit,
                    capital_after=capital,
                )
                race_records.append(record)
                records.append(record)

                # 馬単位のログ
                hit_mark = "◎" if is_hit else "×"
                finish_str = f"{finish_position}着" if finish_position is not None else "不明"
                horse_name_str = f" {horse_name}" if horse_name else ""
                logger.info(
                    f"  {hit_mark} {race_label} 馬番{horse_number:2d}{horse_name_str}"
                    f" | 複勝率:{win_place_prob:.3f} オッズ:{odds:.1f}"
                    f" | 賭け:{bet_amount:,.0f}円 → 払戻:{return_amount:,.0f}円"
                    f" | 損益:{profit:+,.0f}円 [{finish_str}]"
                )

            # レース単位のサマリーログ
            if race_records and self.show_race_summary:
                race_bets = sum(r.bet_amount for r in race_records)
                race_returns = sum(r.return_amount for r in race_records)
                race_profit = race_returns - race_bets
                n_hits = sum(1 for r in race_records if r.is_hit)
                logger.info(
                    f"[レースサマリー] {race_label}"
                    f" | {len(race_records)}頭購入 {n_hits}頭的中"
                    f" | 賭け計:{race_bets:,.0f}円 払戻計:{race_returns:,.0f}円"
                    f" | レース損益:{race_profit:+,.0f}円 残高:{capital:,.0f}円"
                )

        if not records:
            logger.warning("賭け対象となるレコードが見つかりませんでした")
            return pd.DataFrame()

        return pd.DataFrame([asdict(r) for r in records])
