"""キャリブレーション（Issue #414）のテスト"""

import numpy as np
import pandas as pd
import pytest

from src.models.calibration import (
    compute_calibration_metrics,
    fit_calibration_temperature,
    normalize_win_place_prob,
)


def _make_race(scores, race_id="R1"):
    return pd.DataFrame(
        {
            "race_id": [race_id] * len(scores),
            "pred_score": [float(s) for s in scores],
            "horse_number": list(range(1, len(scores) + 1)),
        }
    )


class TestFitCalibrationTemperature:
    """fit_calibration_temperature のテスト"""

    def _synthetic_dataset(self, n_races=400, field=12, true_temp=2.0, seed=0):
        """既知の温度 true_temp で生成した複勝結果を持つデータセットを作る。

        raw score から「真の温度」で複勝確率を作り、その確率で複勝圏内を
        ベルヌーイ抽出する。フィットはこの true_temp 近傍を復元するはず。
        """
        rng = np.random.default_rng(seed)
        rows = []
        for r in range(n_races):
            scores = rng.normal(size=field)
            df = _make_race(scores, race_id=f"R{r}")
            probs = normalize_win_place_prob(df, temperature=true_temp)["win_place_prob"].values
            outcomes = rng.binomial(1, np.clip(probs, 0, 1))
            for hn, (s, o) in enumerate(zip(scores, outcomes), start=1):
                rows.append({"race_id": f"R{r}", "pred_score": float(s), "is_place": int(o)})
        return pd.DataFrame(rows)

    def test_recovers_true_temperature(self):
        """既知温度で生成したデータから、その温度近傍を復元すること"""
        df = self._synthetic_dataset(true_temp=2.0)
        t = fit_calibration_temperature(df)
        assert 1.4 < t < 2.8  # true_temp=2.0 近傍

    def test_returns_one_when_no_labels(self):
        """ラベルが無い/単一クラスのとき temperature=1.0 を返すこと"""
        df = pd.DataFrame(
            {"race_id": ["R1"] * 4, "pred_score": [1.0, 2.0, 3.0, 4.0], "is_place": [0, 0, 0, 0]}
        )
        assert fit_calibration_temperature(df) == 1.0

    def test_custom_score_column(self):
        """pred_score 以外の列名を指定しても動作すること"""
        df = self._synthetic_dataset(true_temp=1.5).rename(columns={"pred_score": "raw"})
        t = fit_calibration_temperature(df, score_col="raw")
        assert 0.3 <= t <= 5.0

    def test_lower_brier_than_uncalibrated(self):
        """フィット温度（デフォルト=Brier目的）の Brier が温度1.0以下になること"""
        df = self._synthetic_dataset(true_temp=2.5)
        t = fit_calibration_temperature(df)
        labels = df["is_place"].values.astype(float)
        p1 = normalize_win_place_prob(df, temperature=1.0)["win_place_prob"].values
        pt = normalize_win_place_prob(df, temperature=t)["win_place_prob"].values
        assert ((pt - labels) ** 2).mean() <= ((p1 - labels) ** 2).mean() + 1e-9

    def test_logloss_objective_lowers_logloss(self):
        """objective='log_loss' 指定時は log-loss が温度1.0以下になること"""
        df = self._synthetic_dataset(true_temp=2.5)
        t = fit_calibration_temperature(df, objective="log_loss")
        labels = df["is_place"].values.astype(float)
        p1 = normalize_win_place_prob(df, temperature=1.0)["win_place_prob"].values
        pt = normalize_win_place_prob(df, temperature=t)["win_place_prob"].values

        def ll(p):
            p = np.clip(p, 1e-12, 1 - 1e-12)
            return -(labels * np.log(p) + (1 - labels) * np.log(1 - p)).mean()

        assert ll(pt) <= ll(p1) + 1e-9


class TestComputeCalibrationMetrics:
    """compute_calibration_metrics のテスト"""

    def test_perfect_calibration_low_ece(self):
        """予測=実測のとき ECE ≈ 0 になること"""
        rng = np.random.default_rng(1)
        probs = rng.uniform(0, 1, size=20000)
        labels = rng.binomial(1, probs)
        m = compute_calibration_metrics(probs, labels)
        assert m["ece"] < 0.02

    def test_brier_and_logloss_known_values(self):
        """Brier / log-loss が手計算値と一致すること"""
        probs = np.array([0.5, 0.5])
        labels = np.array([1, 0])
        m = compute_calibration_metrics(probs, labels)
        assert m["brier"] == pytest.approx(0.25)
        assert m["log_loss"] == pytest.approx(-np.log(0.5))

    def test_reliability_columns(self):
        """reliability DataFrame が期待する列を持つこと"""
        probs = np.array([0.02, 0.2, 0.9])
        labels = np.array([0, 0, 1])
        m = compute_calibration_metrics(probs, labels)
        assert list(m["reliability"].columns) == ["bin", "n", "pred_mean", "actual_rate", "gap"]
        assert m["reliability"]["n"].sum() == 3

    def test_overconfident_has_positive_ece(self):
        """過信（高確率で外す）ケースで ECE が大きくなること"""
        probs = np.full(1000, 0.95)
        labels = np.zeros(1000)  # 全部外れ
        m = compute_calibration_metrics(probs, labels)
        assert m["ece"] > 0.9


class TestNormalizeTemperatureBehavior:
    """温度がキャリブレーションとして機能することの確認（既存挙動の補完）"""

    def test_temperature_preserves_ranking(self):
        """温度を変えても馬の順位（pred_score 降順）が保たれること（ランク不変）"""
        df = _make_race([3.0, 2.0, 1.0, 0.5, 0.0])
        for t in [0.5, 1.0, 2.0, 4.0]:
            res = normalize_win_place_prob(df, temperature=t)
            ordered = res.sort_values("pred_score", ascending=False)["win_place_prob"].values
            assert np.all(np.diff(ordered) <= 1e-12)

    def test_temperature_preserves_sum_constraint(self):
        """温度を変えてもレース内合計=min(3, n) が保たれること"""
        df = _make_race([3.0, 2.0, 1.0, 0.5, 0.0])
        for t in [0.5, 1.0, 2.0, 4.0]:
            res = normalize_win_place_prob(df, temperature=t)
            assert res["win_place_prob"].sum() == pytest.approx(3.0)
