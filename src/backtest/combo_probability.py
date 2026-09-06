"""
Harville型逐次確率モデルによる組み合わせ確率計算

複勝率（3着以内確率）どうしの独立積（prob_i * prob_j）は、複勝圏の「椅子」が
レースにつき3つしかないという排他性を無視するため、理論値からズレる
（ワイド全組み合わせの和が3にならない、三連複が1にならない）。

本モジュールは、校正済み複勝率から「1着確率」を逆算し（IPF: 反復比例フィッティング）、
Harvilleモデル（着順を1着→2着→3着と逐次的に決める確率モデル）でワイド・三連複の
同時確率を計算する。
"""

from __future__ import annotations

from functools import lru_cache
from itertools import permutations

import numpy as np


def harville_place_probs(q: np.ndarray, gamma: float = 1.0) -> np.ndarray:
    """1着確率ベクトル q から各馬の複勝率（3着以内確率）を計算する

    Args:
        q: 1着確率ベクトル（sum=1を想定）
        gamma: Henery補正の指数。1.0で素のHarville。1未満で本命の優位性を割り引く

    Returns:
        各馬の複勝率ベクトル（n>=3ならsum=3になる）
    """
    n = len(q)
    qg = q ** gamma
    total = qg.sum()
    p1 = qg / total
    p2 = np.zeros(n)
    p3 = np.zeros(n)
    for a in range(n):
        rem_a = total - qg[a]
        if rem_a <= 0:
            continue
        p_a = qg[a] / total
        for i in range(n):
            if i == a:
                continue
            p2[i] += p_a * qg[i] / rem_a
            rem_ab = rem_a - qg[i]
            if rem_ab <= 0:
                continue
            p_ai = p_a * qg[i] / rem_a
            for k in range(n):
                if k == a or k == i:
                    continue
                p3[k] += p_ai * qg[k] / rem_ab
    return p1 + p2 + p3


def pair_joint_probability(q: np.ndarray, i: int, j: int, gamma: float = 1.0) -> float:
    """P(馬i, 馬jが両方3着以内) を計算する

    3着目の枠は i, j 以外の任意の馬が占めうるため、他の全馬それぞれについて
    {i, j, 他馬} の3頭の着順6通りを足し上げる（{i, j}だけの2頭の項を別途
    足すと3着目の確率質量を二重計上するので加えない）。

    n=2（i, jの2頭しかいない）の場合は3着目が存在しないため特別扱いする。
    この場合は2頭しかいないレースなので両馬とも必ず3着以内となり、確率は1.0。
    """
    n = len(q)
    if n == 2:
        return 1.0
    qg = q ** gamma
    total = qg.sum()
    prob = 0.0
    for k in range(n):
        if k == i or k == j:
            continue
        prob += _triple_perm_sum(qg, total, i, j, k)
    return min(max(prob, 0.0), 1.0)


def triple_joint_probability(
    q: np.ndarray, i: int, j: int, k: int, gamma: float = 1.0
) -> float:
    """P(馬i, j, kがちょうど3着以内を占める) を計算する（三連複の的中確率）"""
    qg = q ** gamma
    total = qg.sum()
    prob = _triple_perm_sum(qg, total, i, j, k)
    return min(max(prob, 0.0), 1.0)


def _triple_perm_sum(qg: np.ndarray, total: float, i: int, j: int, k: int) -> float:
    """3頭 (i, j, k) が1〜3着を占める全6通りの確率を足し上げる"""
    prob = 0.0
    for perm in permutations((i, j, k)):
        remaining = total
        pr = 1.0
        for pos in perm:
            pr *= qg[pos] / remaining
            remaining -= qg[pos]
        prob += pr
    return prob


def invert_win_probabilities(
    place_probs: np.ndarray,
    gamma: float = 1.0,
    max_iters: int = 100,
    tol: float = 1e-10,
) -> np.ndarray:
    """校正済み複勝率から、Harville式でその複勝率を再現する1着確率を逆算する

    IPF（反復比例フィッティング）: 現在のqから逆算されるHarville複勝率と
    目標複勝率の比率でqを補正し、正規化する、を収束まで繰り返す。

    非現実的な入力（sum(place_probs) != 3など）でも例外・NaN・発散を起こさない
    よう、各反復でqを正の値にクリップし、反復回数を上限で打ち切る。

    Args:
        place_probs: 校正済み複勝率ベクトル（sum≈3を想定、n>=3）
        gamma: Henery補正の指数
        max_iters: 反復回数の上限
        tol: 収束判定の閾値（qの変化量の最大値）

    Returns:
        1着確率ベクトル q（sum=1）
    """
    p_target = np.clip(np.asarray(place_probs, dtype=float), 1e-9, None)
    q = p_target / p_target.sum()
    for _ in range(max_iters):
        p_est = np.clip(harville_place_probs(q, gamma=gamma), 1e-9, None)
        q_new = np.clip(q * (p_target / p_est), 1e-12, None)
        total = q_new.sum()
        if not np.isfinite(total) or total <= 0:
            break
        q_new = q_new / total
        diff = np.max(np.abs(q_new - q))
        q = q_new
        if diff < tol:
            break
    return q


@lru_cache(maxsize=8192)
def _invert_win_probabilities_cached(place_probs_key: tuple[float, ...], gamma: float) -> tuple[float, ...]:
    """invert_win_probabilities のメモ化版（内部用）

    同一レースの複勝率ベクトルは、Optunaのハイパーパラメータ探索中は何度も
    同じ内容で呼ばれる（探索対象のパラメータはEVフィルタの閾値等であり、
    複勝率やgammaはトライアル間で変わらない）。逆算は O(n^2)×反復回数の
    計算コストがかかるため、内容（複勝率ベクトル+gamma）をキーにキャッシュする。
    """
    q = invert_win_probabilities(np.asarray(place_probs_key), gamma=gamma)
    return tuple(q.tolist())
