"""win_place_prob（複勝率）のキャリブレーション（Issue #414）

LambdaRank は順位の正しさ（NDCG/AUC）のみを最適化し、raw score の絶対スケール
（分散・キャリブレーション）は制御しない。本モジュールは raw score を実際の
複勝圏内率に一致させるための確率変換と、その温度パラメータを out-of-sample で
フィットする処理を提供する。

主な公開関数:
- normalize_win_place_prob: レース内 z-score 標準化 + 温度付き softmax + water-fill
  で win_place_prob を算出する（本番予測パスが使用）。
- fit_calibration_isotonic: 検証データ（out-of-sample）上で win_place_prob(T=1.0) →
  is_place のアイソトニック回帰をフィットする（Issue #416・本番の既定校正手法）。
- apply_isotonic_calibration: アイソトニック校正器を適用し、レース内 sum=k に
  再正規化して win_place_prob を上書きする（Issue #416）。
- fit_calibration_temperature: 検証データ（out-of-sample）上で Brier スコア（既定）を
  最小化する温度を求める（温度スケーリング / Guo et al. 2017・後方互換のため残置）。
- compute_calibration_metrics: 信頼性曲線・ECE・Brier・log-loss を算出する（診断用）。
"""

import logging

import numpy as np
import pandas as pd
from scipy.optimize import minimize_scalar
from sklearn.isotonic import IsotonicRegression

logger = logging.getLogger(__name__)

# 温度フィットの探索範囲。1.0 未満=より鋭く、1.0 超=より平滑（過信抑制）。
DEFAULT_TEMPERATURE_BOUNDS = (0.3, 5.0)
_EPS = 1e-12


def _water_fill(p: np.ndarray, probs: np.ndarray) -> np.ndarray:
    """水充填アルゴリズム: 1.0 を超えた分を未達馬に softmax 確率比で再配分する。

    Args:
        p: ベース重み（合計 ≈ 1.0、softmax 確率または比例配分）
        probs: 初期配分 (p * k)。インプレース修正して返す。

    Returns:
        clip 済みの配分ベクトル（sum ≈ min(k, n)）
    """
    n = len(probs)
    for _ in range(n):
        mask_over = probs > 1.0
        if not mask_over.any():
            break
        excess = (probs[mask_over] - 1.0).sum()
        probs[mask_over] = 1.0
        mask_under = probs < 1.0
        if not mask_under.any():
            break
        p_under_sum = p[mask_under].sum()
        if p_under_sum < 1e-12:
            probs[mask_under] += excess / mask_under.sum()
        else:
            probs[mask_under] += excess * p[mask_under] / p_under_sum
    return np.clip(probs, 0.0, 1.0)


def normalize_win_place_prob(
    df: pd.DataFrame, temperature: float = 1.0, n_places: int = 3
) -> pd.DataFrame:
    """レース内z-score標準化＋温度付きソフトマックス＋水充填でwin_place_probを計算する

    LambdaRank は順位の正しさ（AUC/NDCG）のみを最適化し、raw score の絶対スケール
    （分散・キャリブレーション）は制御しない。そのため再学習でハイパーパラメータが
    変わるたびにスコア分散が変動し、固定値の temperature が機能しなくなる（Issue #390）。

    本実装ではソフトマックス前にレース内でスコアを z-score 標準化する。
    z = (score - mean) / std とすることで softmax の実効温度が `std × temperature` に
    依存しなくなり、モデルの raw score スケールに不変な複勝率が得られる。これにより
    再学習でスケールが変わっても複勝率の分散が安定する（Issue #397）。

    temperature は out-of-sample でフィットしたキャリブレーション温度を与えることで、
    予測複勝率を実際の複勝圏内率に一致させる（Issue #414）。temperature>1.0 で
    分布が平滑化され、本命馬の過信が抑制される。

    水充填アルゴリズム（_water_fill）により prob=1.0 クリップ問題は解消済み。

    Args:
        df: race_id と pred_score カラムを持つ DataFrame
        temperature: 標準化後スコアに対するソフトマックス温度（大きいほど均一化、デフォルト1.0）
        n_places: 複勝対象着順数（デフォルト3）

    Returns:
        win_place_prob カラムを上書きした DataFrame（元の DataFrame は変更しない）
    """
    df = df.copy()

    def _zscore_water_fill(scores: pd.Series) -> pd.Series:
        vals = scores.values.astype(float)
        n = len(vals)
        k = float(min(n_places, n))
        std = vals.std()  # 母標準偏差（ddof=0）
        if std < 1e-12:
            # 全馬同一スコア（std=0）→ 均等配分にフォールバック
            p = np.full(n, 1.0 / n)
        else:
            # レース内 z-score 標準化（mean は softmax 正規化で相殺されるが明示する）
            z = (vals - vals.mean()) / std
            shifted = z - z.max()  # 数値安定化
            exp_s = np.exp(shifted / temperature)
            p = exp_s / exp_s.sum()
        return pd.Series(_water_fill(p, p * k), index=scores.index)

    df["win_place_prob"] = df.groupby("race_id")["pred_score"].transform(
        _zscore_water_fill
    )
    return df


def _log_loss(probs: np.ndarray, labels: np.ndarray) -> float:
    """二値 log-loss（NLL）を計算する。"""
    p = np.clip(probs, _EPS, 1.0 - _EPS)
    return float(-(labels * np.log(p) + (1 - labels) * np.log(1 - p)).mean())


def _brier(probs: np.ndarray, labels: np.ndarray) -> float:
    """Brier スコア（平均二乗誤差）を計算する。"""
    return float(((probs - labels) ** 2).mean())


def fit_calibration_temperature(
    df: pd.DataFrame,
    label_col: str = "is_place",
    score_col: str = "pred_score",
    n_places: int = 3,
    bounds: tuple[float, float] = DEFAULT_TEMPERATURE_BOUNDS,
    objective: str = "brier",
) -> float:
    """out-of-sample データ上で校正スコアを最小化する温度を求める（温度スケーリング）。

    win_place_prob(temperature) を各レースで算出し、実際の複勝圏内フラグ
    （label_col, 1=複勝圏内）に対する校正スコアを最小化する単一温度を返す。
    単調変換のため NDCG@3 / Recall@3 などのランク指標は不変。

    目的関数はデフォルトで Brier スコアを使う。本システムの win_place_prob は
    レース内合計=k・各馬≤1 という water-fill 制約下にあり、log-loss を目的にすると
    「高確率帯（本命馬）の過信」を過剰に軟化させて T が大きくなりすぎ、reliability
    （ECE）と Brier をむしろ悪化させる（実測: log-loss最適 T=1.43 で ECE 0.023→0.042）。
    Brier は reliability+resolution に分解できる proper scoring rule で、ECE と最適温度が
    一致する（実測: Brier/ECE とも T≈1.1 で最小）ため、信頼性の校正に適している（Issue #414）。

    Args:
        df: race_id・score_col・label_col を持つ DataFrame
        label_col: 複勝圏内フラグ列名（1=複勝圏内, 0=圏外）
        score_col: モデルの raw score 列名（normalize 内では pred_score を参照するため
                   pred_score 以外を渡した場合は内部でリネームする）
        n_places: 複勝対象着順数
        bounds: 温度の探索範囲
        objective: 最小化する校正スコア（"brier"（デフォルト） or "log_loss"）

    Returns:
        フィットした温度。データ不足時は 1.0 を返す。
    """
    work = df[["race_id", score_col, label_col]].copy()
    if score_col != "pred_score":
        work = work.rename(columns={score_col: "pred_score"})
    work = work[work[label_col].notna()].copy()
    if len(work) == 0 or work[label_col].nunique() < 2:
        logger.warning("校正温度フィット不可（ラベル不足）→ temperature=1.0 を使用")
        return 1.0

    labels = work[label_col].values.astype(float)
    score_fn = _brier if objective == "brier" else _log_loss

    def loss(temperature: float) -> float:
        probs = normalize_win_place_prob(
            work, temperature=temperature, n_places=n_places
        )["win_place_prob"].values
        return score_fn(probs, labels)

    result = minimize_scalar(loss, bounds=bounds, method="bounded")
    temperature = float(result.x)
    logger.info(
        "校正温度フィット完了: temperature=%.4f "
        "(%s %.4f → %.4f, n=%d)",
        temperature,
        objective,
        loss(1.0),
        result.fun,
        len(work),
    )
    return temperature


def fit_calibration_isotonic(
    df: pd.DataFrame,
    label_col: str = "is_place",
    score_col: str = "pred_score",
    n_places: int = 3,
) -> dict:
    """out-of-sample データ上でアイソトニック回帰の校正器をフィットする（Issue #416）。

    各レースで win_place_prob(T=1.0) を算出し、その値（説明変数）から実際の複勝圏内
    フラグ（label_col, 1=複勝圏内）への単調増加写像を IsotonicRegression で学習する。

    本システムのミスキャリブレーションは「過信(over-confidence)」かつ非対称（高確率帯
    ほど深刻）であり、単一の大域パラメータである温度では補正できず中帯を過補正して
    ECE がむしろ悪化する。アイソトニック回帰は任意の単調歪みを区間ごとに補正できるため
    高確率帯（投資対象）の過信を解消しつつ ECE/Brier/LogLoss を全て改善する（Issue #416）。

    win_place_prob(T=1.0) → is_place の単調写像のためレース内順位は保存され、
    NDCG@3 / Recall@3 は不変。

    Args:
        df: race_id・score_col・label_col を持つ DataFrame
        label_col: 複勝圏内フラグ列名（1=複勝圏内, 0=圏外）
        score_col: モデルの raw score 列名（pred_score 以外は内部でリネーム）
        n_places: 複勝対象着順数

    Returns:
        JSON 保存可能な校正器 dict
        {"method": "isotonic", "x_thresholds": [...], "y_thresholds": [...]}。
        データ不足時は恒等写像（{"x_thresholds": [0, 1], "y_thresholds": [0, 1]}）を返す。
    """
    work = df[["race_id", score_col, label_col]].copy()
    if score_col != "pred_score":
        work = work.rename(columns={score_col: "pred_score"})
    work = work[work[label_col].notna()].copy()

    identity = {"method": "isotonic", "x_thresholds": [0.0, 1.0], "y_thresholds": [0.0, 1.0]}
    if len(work) == 0 or work[label_col].nunique() < 2:
        logger.warning("アイソトニック校正フィット不可（ラベル不足）→ 恒等写像を使用")
        return identity

    probs = normalize_win_place_prob(work, temperature=1.0, n_places=n_places)[
        "win_place_prob"
    ].values
    labels = work[label_col].values.astype(float)

    iso = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
    iso.fit(probs, labels)

    calibrator = {
        "method": "isotonic",
        "x_thresholds": [float(x) for x in iso.X_thresholds_],
        "y_thresholds": [float(y) for y in iso.y_thresholds_],
    }
    logger.info(
        "アイソトニック校正フィット完了: 閾値%d点 (n=%d)",
        len(calibrator["x_thresholds"]),
        len(work),
    )
    return calibrator


def apply_isotonic_calibration(
    df: pd.DataFrame, calibrator: dict, n_places: int = 3
) -> pd.DataFrame:
    """アイソトニック校正器を適用し、レース内 sum=k に再正規化する（Issue #416）。

    各馬の win_place_prob(T=1.0) に校正器（区分線形補間）を適用したうえで、レース内で
    _water_fill により合計 = min(n_places, n) に再正規化して win_place_prob を上書きする。
    校正写像は単調増加のためレース内順位は保存される（NDCG@3 / Recall@3 不変）。

    Args:
        df: race_id と pred_score カラムを持つ DataFrame
        calibrator: fit_calibration_isotonic が返す校正器 dict
        n_places: 複勝対象着順数

    Returns:
        win_place_prob カラムを上書きした DataFrame（元の DataFrame は変更しない）
    """
    df = normalize_win_place_prob(df, temperature=1.0, n_places=n_places)
    x_thresholds = np.asarray(calibrator["x_thresholds"], dtype=float)
    y_thresholds = np.asarray(calibrator["y_thresholds"], dtype=float)

    def _calibrate(probs: pd.Series) -> pd.Series:
        # np.interp は範囲外を端点値にクリップ（IsotonicRegression out_of_bounds="clip" と等価）
        calibrated = np.interp(probs.values.astype(float), x_thresholds, y_thresholds)
        n = len(calibrated)
        k = float(min(n_places, n))
        total = calibrated.sum()
        if total < 1e-12:
            base = np.full(n, 1.0 / n)
            return pd.Series(_water_fill(base, base * k), index=probs.index)
        # 校正値を比率重みとして sum=k に再正規化（_water_fill で各馬≤1 を保証）
        base = calibrated / total
        return pd.Series(_water_fill(base, base * k), index=probs.index)

    df["win_place_prob"] = df.groupby("race_id")["win_place_prob"].transform(_calibrate)
    return df


def apply_model_calibration(df: pd.DataFrame, ranker, n_places: int = 3) -> pd.DataFrame:
    """モデルに保存された校正器を優先順位順に適用して win_place_prob を算出する。

    優先順位は アイソトニック（Issue #416）→ 温度（Issue #414）→ 未校正 T=1.0（旧モデル）。
    本番予測パス（predict.py）とバックテスト/戦略最適化パス（run_backtest.py）の双方が
    この共通関数を使うことで、両者の win_place_prob を必ず一致させる（Issue #417）。
    校正器はいずれも単調写像のためレース内順位は保存される（NDCG@3 / Recall@3 不変）。

    Args:
        df: race_id と pred_score カラムを持つ DataFrame
        ranker: calibration_isotonic / calibration_temperature 属性を持つモデル
                （未保持の場合は duck-typing で None 扱い → T=1.0 フォールバック）
        n_places: 複勝対象着順数

    Returns:
        win_place_prob カラムを上書きした DataFrame
    """
    calib_isotonic = getattr(ranker, "calibration_isotonic", None)
    if calib_isotonic:
        logger.info("win_place_prob 算出: アイソトニック校正を適用")
        return apply_isotonic_calibration(df, calib_isotonic, n_places=n_places)

    calib_temp = getattr(ranker, "calibration_temperature", None) or 1.0
    logger.info("win_place_prob 算出: calibration_temperature=%.4f", calib_temp)
    return normalize_win_place_prob(df, temperature=calib_temp, n_places=n_places)


def compute_calibration_metrics(
    probs: np.ndarray,
    labels: np.ndarray,
    bins: list[float] | None = None,
) -> dict:
    """信頼性曲線・ECE・Brier・log-loss を算出する（診断用）。

    Args:
        probs: 予測複勝率（win_place_prob）
        labels: 複勝圏内フラグ（1=圏内, 0=圏外）
        bins: ビン境界。None の場合はデフォルトの不均等ビンを使用。

    Returns:
        {"brier", "log_loss", "ece", "mean_pred", "mean_actual", "n",
         "reliability": DataFrame(bin, n, pred_mean, actual_rate, gap)}
    """
    probs = np.asarray(probs, dtype=float)
    labels = np.asarray(labels, dtype=float)
    if bins is None:
        bins = [0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.8, 1.0001]

    brier = float(((probs - labels) ** 2).mean())
    logloss = _log_loss(probs, labels)

    bin_idx = np.digitize(probs, bins, right=False) - 1
    bin_idx = np.clip(bin_idx, 0, len(bins) - 2)
    rows = []
    ece = 0.0
    total = len(probs)
    for b in range(len(bins) - 1):
        mask = bin_idx == b
        n = int(mask.sum())
        if n == 0:
            continue
        pred_mean = float(probs[mask].mean())
        actual_rate = float(labels[mask].mean())
        gap = actual_rate - pred_mean
        ece += (n / total) * abs(gap)
        rows.append(
            {
                "bin": f"{bins[b] * 100:.0f}-{min(bins[b + 1], 1.0) * 100:.0f}%",
                "n": n,
                "pred_mean": pred_mean,
                "actual_rate": actual_rate,
                "gap": gap,
            }
        )

    return {
        "brier": brier,
        "log_loss": logloss,
        "ece": float(ece),
        "mean_pred": float(probs.mean()),
        "mean_actual": float(labels.mean()),
        "n": total,
        "reliability": pd.DataFrame(rows),
    }
