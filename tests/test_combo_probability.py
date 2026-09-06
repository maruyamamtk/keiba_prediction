"""
Harville型逐次確率モデル（combo_probability）のテスト
"""

from __future__ import annotations

from itertools import permutations

import numpy as np
import pytest

from src.backtest.combo_probability import (
    harville_place_probs,
    invert_win_probabilities,
    pair_joint_probability,
    triple_joint_probability,
)


def _brute_force_set_prob(q: np.ndarray, target: tuple[int, ...], gamma: float = 1.0) -> float:
    """target集合がちょうど上位len(target)着を占める確率をブルートフォースで計算する"""
    qg = q**gamma
    total_all = qg.sum()
    prob = 0.0
    for perm in permutations(target):
        remaining = total_all
        pr = 1.0
        for pos in perm:
            pr *= qg[pos] / remaining
            remaining -= qg[pos]
        prob += pr
    return prob


def _brute_force_pair_joint(q: np.ndarray, i: int, j: int, gamma: float = 1.0) -> float:
    n = len(q)
    total = 0.0
    for k in range(n):
        if k in (i, j):
            continue
        total += _brute_force_set_prob(q, (i, j, k), gamma=gamma)
    return total


class TestHarvillePlaceProbs:
    def test_sum_equals_three_for_n_ge_3(self):
        rng = np.random.default_rng(0)
        for n in (3, 5, 8, 18):
            q = rng.dirichlet(np.ones(n))
            place = harville_place_probs(q)
            assert place.sum() == pytest.approx(3.0, abs=1e-9)

    def test_matches_brute_force_marginal(self):
        rng = np.random.default_rng(1)
        q = rng.dirichlet(np.ones(6) * 1.2)
        place = harville_place_probs(q)
        for i in range(len(q)):
            others = [k for k in range(len(q)) if k != i]
            brute = 0.0
            # P(iが3着以内) = 全ての2頭の組み合わせについて{i,a,b}がtop3を占める確率の和
            for a_idx in range(len(others)):
                for b_idx in range(a_idx + 1, len(others)):
                    brute += _brute_force_set_prob(
                        q, (i, others[a_idx], others[b_idx])
                    )
            assert place[i] == pytest.approx(brute, abs=1e-9)

    def test_stronger_horse_has_higher_place_prob(self):
        q = np.array([0.5, 0.3, 0.1, 0.05, 0.05])
        place = harville_place_probs(q)
        assert np.all(np.diff(place) <= 0)  # 降順


class TestPairJointProbability:
    def test_matches_brute_force(self):
        rng = np.random.default_rng(2)
        q = rng.dirichlet(np.ones(7) * 1.1)
        for i, j in [(0, 1), (0, 3), (5, 6), (2, 4)]:
            fast = pair_joint_probability(q, i, j)
            brute = _brute_force_pair_joint(q, i, j)
            assert fast == pytest.approx(brute, abs=1e-9)

    def test_result_bounded_in_unit_interval(self):
        """回帰テスト: 3着目の枠を二重計上すると1を超える不正な値になっていたバグの再発防止"""
        rng = np.random.default_rng(3)
        for _ in range(50):
            n = rng.integers(3, 12)
            q = rng.dirichlet(np.ones(n) * rng.uniform(0.1, 2.0))
            i, j = rng.choice(n, size=2, replace=False)
            prob = pair_joint_probability(q, int(i), int(j))
            assert 0.0 <= prob <= 1.0

    def test_joint_le_min_marginal(self):
        """P(i,jが両方3着以内) は min(P(iが3着以内), P(jが3着以内)) を超えない"""
        rng = np.random.default_rng(4)
        q = rng.dirichlet(np.ones(8))
        place = harville_place_probs(q)
        for i, j in [(0, 1), (2, 5), (6, 7)]:
            joint = pair_joint_probability(q, i, j)
            assert joint <= min(place[i], place[j]) + 1e-9

    def test_two_horse_race_both_always_place(self):
        """2頭立て（3着目が存在しない）では両馬とも必ず3着以内となり確率は1.0"""
        q = np.array([0.7, 0.3])
        assert pair_joint_probability(q, 0, 1) == pytest.approx(1.0)

    def test_dominant_pair_not_overestimated_by_independence(self):
        """強い2頭の同時確率は、独立積よりも実際は低くなる（椅子の取り合いのため）"""
        q = np.array([0.40, 0.25, 0.15, 0.08, 0.06, 0.03, 0.02, 0.01])
        place = harville_place_probs(q)
        naive = place[0] * place[1]
        joint = pair_joint_probability(q, 0, 1)
        assert joint < naive


class TestTripleJointProbability:
    def test_matches_brute_force(self):
        rng = np.random.default_rng(5)
        q = rng.dirichlet(np.ones(7) * 1.1)
        for i, j, k in [(0, 1, 2), (0, 3, 5), (4, 5, 6)]:
            fast = triple_joint_probability(q, i, j, k)
            brute = _brute_force_set_prob(q, (i, j, k))
            assert fast == pytest.approx(brute, abs=1e-9)

    def test_result_bounded_in_unit_interval(self):
        rng = np.random.default_rng(6)
        for _ in range(50):
            n = rng.integers(3, 12)
            q = rng.dirichlet(np.ones(n) * rng.uniform(0.1, 2.0))
            i, j, k = rng.choice(n, size=3, replace=False)
            prob = triple_joint_probability(q, int(i), int(j), int(k))
            assert 0.0 <= prob <= 1.0

    def test_sum_over_all_triples_equals_one(self):
        from itertools import combinations

        q = np.array([0.5, 0.3, 0.1, 0.06, 0.04])
        total = sum(
            triple_joint_probability(q, i, j, k)
            for i, j, k in combinations(range(len(q)), 3)
        )
        assert total == pytest.approx(1.0, abs=1e-9)


class TestInvertWinProbabilities:
    def test_round_trip_recovers_place_probs(self):
        rng = np.random.default_rng(7)
        for n in (3, 5, 8, 13, 18):
            q_true = rng.dirichlet(np.ones(n) * 0.9)
            place_target = harville_place_probs(q_true)
            q_est = invert_win_probabilities(place_target)
            place_est = harville_place_probs(q_est)
            assert np.max(np.abs(place_est - place_target)) < 1e-4

    def test_matches_real_race_example(self):
        """本セッションで検証済みの実レース例（札幌12R・13頭立て）との整合確認"""
        calibrated_place = np.array(
            [0.855, 0.609, 0.486, 0.237, 0.186, 0.172, 0.106, 0.096, 0.068, 0.065, 0.062, 0.029, 0.029]
        )
        q = invert_win_probabilities(calibrated_place)
        # 1番人気(ミルボナー)の逆算勝率は約41.7%であることを確認済み
        assert q[0] == pytest.approx(0.4168, abs=1e-3)
        place_est = harville_place_probs(q)
        assert np.max(np.abs(place_est - calibrated_place)) < 1e-6

    def test_output_is_valid_probability_vector(self):
        q = invert_win_probabilities(np.array([0.9, 0.8, 0.7, 0.4, 0.2]))
        assert q.sum() == pytest.approx(1.0, abs=1e-6)
        assert np.all(q >= 0)

    def test_handles_unrealistic_input_without_crashing(self):
        """sum!=3の非現実的な入力でも例外・NaN・発散なく有効な確率ベクトルを返す"""
        for probs in (
            [0.4, 0.35, 0.3, 0.1, 0.05],
            [0.5, 0.2, 0.15],
            [1 / 8] * 8,
        ):
            q = invert_win_probabilities(np.array(probs))
            assert np.all(np.isfinite(q))
            assert q.sum() == pytest.approx(1.0, abs=1e-6)
            assert np.all(q >= 0)

    def test_three_horse_race_boundary(self):
        """3頭立て（全馬が確実に複勝圏内）でも破綻しない"""
        q = invert_win_probabilities(np.array([1.0, 1.0, 1.0]))
        assert np.all(np.isfinite(q))
        place = harville_place_probs(q)
        assert np.max(np.abs(place - 1.0)) < 1e-3


class TestHeneryGamma:
    def test_gamma_below_one_flattens_distribution(self):
        """gamma<1で本命の複勝率が下がり下位人気が上がる（Heneryモデルの効果）"""
        q = np.array([0.40, 0.25, 0.15, 0.08, 0.06, 0.03, 0.02, 0.01])
        place_1 = harville_place_probs(q, gamma=1.0)
        place_08 = harville_place_probs(q, gamma=0.8)
        assert place_08[0] < place_1[0]
        assert place_08[-1] > place_1[-1]
        assert place_08.sum() == pytest.approx(3.0, abs=1e-9)


class TestInvertWinProbabilitiesCached:
    def test_matches_uncached_result(self):
        from src.backtest.combo_probability import _invert_win_probabilities_cached

        probs = (0.868, 0.739, 0.552, 0.327, 0.252, 0.130, 0.088, 0.044)
        q_direct = invert_win_probabilities(np.array(probs))
        q_cached = np.asarray(_invert_win_probabilities_cached(probs, 1.0))
        assert np.allclose(q_direct, q_cached)

    def test_repeated_calls_hit_cache(self):
        from src.backtest.combo_probability import _invert_win_probabilities_cached

        _invert_win_probabilities_cached.cache_clear()
        probs = (0.868, 0.739, 0.552, 0.327, 0.252, 0.130, 0.088, 0.044)
        _invert_win_probabilities_cached(probs, 1.0)
        _invert_win_probabilities_cached(probs, 1.0)
        info = _invert_win_probabilities_cached.cache_info()
        assert info.hits >= 1
