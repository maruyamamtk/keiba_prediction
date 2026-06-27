"""キャリブレーション（Issue #414）のテスト"""

import numpy as np
import pandas as pd
import pytest

from src.models.calibration import (
    apply_isotonic_calibration,
    apply_model_calibration,
    compute_calibration_metrics,
    fit_calibration_isotonic,
    fit_calibration_temperature,
    normalize_win_place_prob,
)


class _FakeRanker:
    """apply_model_calibration の duck-typing 用ダミーモデル。"""

    def __init__(self, isotonic=None, temperature=None):
        self.calibration_isotonic = isotonic
        self.calibration_temperature = temperature


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


class TestFitCalibrationIsotonic:
    """fit_calibration_isotonic / apply_isotonic_calibration のテスト（Issue #416）"""

    def _overconfident_dataset(self, n_races=600, field=12, true_temp=2.2, seed=3):
        """過信（高確率帯で外す）形状のデータセットを作る。

        win_place_prob を真の温度 true_temp（>1）で生成した確率から複勝結果を抽出する。
        校正前（T=1.0）の予測は過信気味になり、校正で補正できる余地が生まれる。
        """
        rng = np.random.default_rng(seed)
        rows = []
        for r in range(n_races):
            scores = rng.normal(size=field)
            df = _make_race(scores, race_id=f"R{r}")
            probs = normalize_win_place_prob(df, temperature=true_temp)["win_place_prob"].values
            outcomes = rng.binomial(1, np.clip(probs, 0, 1))
            for s, o in zip(scores, outcomes):
                rows.append({"race_id": f"R{r}", "pred_score": float(s), "is_place": int(o)})
        return pd.DataFrame(rows)

    def test_returns_json_serializable_calibrator(self):
        """校正器が JSON 保存可能な dict（閾値配列）であること"""
        import json

        df = self._overconfident_dataset()
        calib = fit_calibration_isotonic(df)
        assert calib["method"] == "isotonic"
        assert len(calib["x_thresholds"]) == len(calib["y_thresholds"])
        assert len(calib["x_thresholds"]) >= 2
        # 例外なく JSON シリアライズできること
        json.loads(json.dumps(calib))

    def test_roundtrip_through_json(self):
        """meta.json 経由の保存→読込（roundtrip）後も同じ確率を生むこと"""
        import json

        df = self._overconfident_dataset()
        calib = fit_calibration_isotonic(df)
        calib_restored = json.loads(json.dumps(calib))
        p1 = apply_isotonic_calibration(df, calib)["win_place_prob"].values
        p2 = apply_isotonic_calibration(df, calib_restored)["win_place_prob"].values
        assert np.allclose(p1, p2)

    def test_returns_identity_when_no_labels(self):
        """ラベルが単一クラスのとき恒等写像を返すこと"""
        df = pd.DataFrame(
            {"race_id": ["R1"] * 4, "pred_score": [1.0, 2.0, 3.0, 4.0], "is_place": [0, 0, 0, 0]}
        )
        calib = fit_calibration_isotonic(df)
        assert calib["x_thresholds"] == [0.0, 1.0]
        assert calib["y_thresholds"] == [0.0, 1.0]

    def test_preserves_within_race_sum(self):
        """適用後もレース内合計 = min(3, n) が保たれること"""
        df = self._overconfident_dataset()
        calib = fit_calibration_isotonic(df)
        out = apply_isotonic_calibration(df, calib)
        sums = out.groupby("race_id")["win_place_prob"].sum()
        assert np.allclose(sums.values, 3.0, atol=1e-6)

    def test_preserves_ranking(self):
        """単調写像のためレース内順位（pred_score 降順）が保たれること"""
        df = self._overconfident_dataset()
        calib = fit_calibration_isotonic(df)
        out = apply_isotonic_calibration(df, calib)
        for _, g in out.groupby("race_id"):
            ordered = g.sort_values("pred_score", ascending=False)["win_place_prob"].values
            assert np.all(np.diff(ordered) <= 1e-9)

    def test_each_prob_within_unit_interval(self):
        """各馬の校正後確率が [0, 1] に収まること"""
        df = self._overconfident_dataset()
        calib = fit_calibration_isotonic(df)
        out = apply_isotonic_calibration(df, calib)
        assert out["win_place_prob"].min() >= -1e-9
        assert out["win_place_prob"].max() <= 1.0 + 1e-9

    def test_improves_ece_on_holdout(self):
        """races を 50/50 分割し、held-out で ECE が校正前より改善すること"""
        df = self._overconfident_dataset(n_races=1000)
        races = sorted(df["race_id"].unique())
        mid = len(races) // 2
        fit_races, eval_races = set(races[:mid]), set(races[mid:])
        fit_df = df[df["race_id"].isin(fit_races)].copy()
        eval_df = df[df["race_id"].isin(eval_races)].copy()

        calib = fit_calibration_isotonic(fit_df)
        labels = eval_df["is_place"].values
        p_before = normalize_win_place_prob(eval_df, temperature=1.0)["win_place_prob"].values
        p_after = apply_isotonic_calibration(eval_df, calib)["win_place_prob"].values

        ece_before = compute_calibration_metrics(p_before, labels)["ece"]
        ece_after = compute_calibration_metrics(p_after, labels)["ece"]
        assert ece_after <= ece_before + 1e-9


class TestApplyModelCalibration:
    """apply_model_calibration（共通校正適用・Issue #417）のテスト"""

    def test_isotonic_takes_precedence(self):
        """アイソトニック校正器があればそれを使い、温度より優先されること"""
        df = _make_race([3.0, 2.0, 1.0, 0.5, 0.0])
        calib = {
            "method": "isotonic",
            "x_thresholds": [0.0, 0.5, 1.0],
            "y_thresholds": [0.0, 0.3, 1.0],
        }
        ranker = _FakeRanker(isotonic=calib, temperature=2.0)
        expected = apply_isotonic_calibration(df, calib)["win_place_prob"].values
        actual = apply_model_calibration(df, ranker)["win_place_prob"].values
        assert np.allclose(actual, expected)

    def test_temperature_used_when_no_isotonic(self):
        """アイソトニックが無く温度があれば温度を適用すること"""
        df = _make_race([3.0, 2.0, 1.0, 0.5, 0.0])
        ranker = _FakeRanker(isotonic=None, temperature=1.5)
        expected = normalize_win_place_prob(df, temperature=1.5)["win_place_prob"].values
        actual = apply_model_calibration(df, ranker)["win_place_prob"].values
        assert np.allclose(actual, expected)

    def test_fallback_to_uncalibrated(self):
        """校正器が一切無い旧モデルは T=1.0（未校正）にフォールバックすること"""
        df = _make_race([3.0, 2.0, 1.0, 0.5, 0.0])
        ranker = _FakeRanker(isotonic=None, temperature=None)
        expected = normalize_win_place_prob(df, temperature=1.0)["win_place_prob"].values
        actual = apply_model_calibration(df, ranker)["win_place_prob"].values
        assert np.allclose(actual, expected)

    def test_missing_attributes_fallback(self):
        """校正属性を持たないオブジェクトでも T=1.0 にフォールバックすること"""
        df = _make_race([3.0, 2.0, 1.0])

        class _Bare:
            pass

        expected = normalize_win_place_prob(df, temperature=1.0)["win_place_prob"].values
        actual = apply_model_calibration(df, _Bare())["win_place_prob"].values
        assert np.allclose(actual, expected)


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
