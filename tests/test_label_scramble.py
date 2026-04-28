from __future__ import annotations

from collections import Counter

import pytest

from silver.backtest import (
    LabelScrambleInputError,
    LabelScrambleSample,
    run_label_scramble,
)


def test_label_scramble_result_is_byte_stable_for_same_seed_and_inputs() -> None:
    samples = _signal_samples()

    first = run_label_scramble(tuple(reversed(samples)), seed=31, trial_count=12)
    second = run_label_scramble(samples, seed=31, trial_count=12)

    assert first == second
    assert first.to_json() == second.to_json()
    assert first.seed == 31
    assert first.trial_count == 12
    assert first.sample_count == len(samples)
    assert first.group_count == 2
    assert first.observed_score == pytest.approx(1.0)
    assert len(first.scramble_scores) == 12
    assert first.p_value == pytest.approx(first.observed_rank / 13)


def test_label_scramble_does_not_mutate_samples_or_scramble_identities() -> None:
    samples = _signal_samples()
    original = tuple(samples)
    scored_inputs: list[tuple[LabelScrambleSample, ...]] = []

    def capture_and_score(scored_samples: tuple[LabelScrambleSample, ...]) -> float:
        scored_inputs.append(scored_samples)
        return sum(
            sample.feature_value * sample.label_value for sample in scored_samples
        )

    run_label_scramble(
        samples,
        seed=7,
        trial_count=5,
        scoring_function=capture_and_score,
    )

    assert samples == original
    expected_identity = _identity(_normalized(samples))
    expected_group_labels = _group_labels(samples)
    assert len(scored_inputs) == 6

    for scored_samples in scored_inputs:
        assert _identity(scored_samples) == expected_identity
        assert _group_labels(scored_samples) == expected_group_labels


def test_ineligible_samples_are_rejected_before_scrambling() -> None:
    samples = (
        LabelScrambleSample(
            sample_id="AAA-2024-01-02",
            feature_value=1.0,
            label_value=0.01,
            group_key="2024-01-02",
            eligible=False,
        ),
    )

    with pytest.raises(LabelScrambleInputError, match="already eligible samples"):
        run_label_scramble(samples, seed=1, trial_count=1)


def test_flat_label_null_has_conservative_rank() -> None:
    samples = tuple(
        LabelScrambleSample(
            sample_id=sample.sample_id,
            feature_value=sample.feature_value,
            label_value=0.0,
            group_key=sample.group_key,
        )
        for sample in _signal_samples()
    )

    result = run_label_scramble(samples, seed=99, trial_count=9)

    assert result.observed_score == 0.0
    assert result.scramble_scores == (0.0,) * 9
    assert result.observed_rank == 10
    assert result.p_value == 1.0


def _signal_samples() -> tuple[LabelScrambleSample, ...]:
    return (
        LabelScrambleSample("AAA-2024-01-02", 1.0, 0.01, "2024-01-02"),
        LabelScrambleSample("BBB-2024-01-02", 2.0, 0.02, "2024-01-02"),
        LabelScrambleSample("CCC-2024-01-02", 3.0, 0.03, "2024-01-02"),
        LabelScrambleSample("AAA-2024-01-03", 1.0, 0.01, "2024-01-03"),
        LabelScrambleSample("BBB-2024-01-03", 2.0, 0.02, "2024-01-03"),
        LabelScrambleSample("CCC-2024-01-03", 3.0, 0.03, "2024-01-03"),
    )


def _normalized(
    samples: tuple[LabelScrambleSample, ...],
) -> tuple[LabelScrambleSample, ...]:
    return tuple(sorted(samples, key=lambda sample: (sample.group_key, sample.sample_id)))


def _identity(
    samples: tuple[LabelScrambleSample, ...],
) -> tuple[tuple[str, float, str, bool], ...]:
    return tuple(
        (
            sample.sample_id,
            sample.feature_value,
            sample.group_key,
            sample.eligible,
        )
        for sample in samples
    )


def _group_labels(
    samples: tuple[LabelScrambleSample, ...],
) -> dict[str, Counter[float]]:
    groups: dict[str, Counter[float]] = {}
    for sample in samples:
        groups.setdefault(sample.group_key, Counter())[sample.label_value] += 1
    return groups
