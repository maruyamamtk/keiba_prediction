"""
バックテストシミュレーター

学習済みモデルの予測スコア（複勝率）と過去のオッズ・払戻データを使用して
投資シミュレーションを行う。

投資ルール:
- 期待回収率フィルタ: 予測複勝率 × オッズ > 閾値 の馬のみ購入
- 賭け金計算: Fractional Kelly 基準
- 1レースあたり上限: 総資金の max_bet_ratio まで
- 最低賭け金: 100円単位に切り捨て
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


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
    win_place_prob: float
    odds: float
    expected_return: float
    kelly_frac: float
    bet_amount: float
    finish_position: Optional[int]
    is_hit: bool
    payout_per_100: Optional[int]
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
        max_bet_ratio: float = 0.05,
        min_bet_amount: float = 100.0,
        odds_column: str = "odds_yesterday",
    ):
        """
        初期化

        Args:
            initial_capital: 初期資金 (円)
            kelly_fraction: Fractional Kelly の係数
            expected_return_threshold: 期待回収率フィルタ閾値
                (予測複勝率 × オッズ > 閾値 の馬のみ購入)
            max_bet_ratio: 1レースあたりの最大賭け金比率 (例: 0.05 = 5%)
            min_bet_amount: 最低賭け金 (円)
            odds_column: 意思決定に使用するオッズカラム名
        """
        self.initial_capital = initial_capital
        self.kelly_fraction = kelly_fraction
        self.expected_return_threshold = expected_return_threshold
        self.max_bet_ratio = max_bet_ratio
        self.min_bet_amount = min_bet_amount
        self.odds_column = odds_column

    def run(
        self,
        predictions_df: pd.DataFrame,
        payouts_df: Optional[pd.DataFrame] = None,
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

        for race_id, race_df in df.groupby("race_id", sort=False):
            race_date = race_df["race_date"].iloc[0]

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

                # Fractional Kelly で賭け金計算
                kf = fractional_kelly(win_place_prob, odds, self.kelly_fraction)
                bet_amount = capital * kf
                bet_amount = min(bet_amount, capital * self.max_bet_ratio)
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
                else:
                    finish_position = int(finish_raw)
                    is_hit = (1 <= finish_position <= 3)

                # 払戻金の計算
                horse_number = int(row["horse_number"])
                payout_per_100 = payout_map.get((str(race_id), horse_number), None)

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

                records.append(BetRecord(
                    race_id=str(race_id),
                    race_date=race_date,
                    horse_id=str(row["horse_id"]),
                    horse_number=horse_number,
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
                ))

        if not records:
            logger.warning("賭け対象となるレコードが見つかりませんでした")
            return pd.DataFrame()

        return pd.DataFrame([asdict(r) for r in records])
