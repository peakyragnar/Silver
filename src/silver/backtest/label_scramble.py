"""Pure label-scramble robustness tests for backtest-ready samples."""

from __future__ import annotations

import json
import math
import random
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Literal


LabelScrambleAlternative = Literal["greater", "less", "two_sided"]
LabelScrambleScoringFunction = Callable[[tuple["LabelScrambleSample", ...]], float]


class LabelScrambleInputError(ValueError):
    """Raised when label-scramble inputs are malformed or not PIT-eligible."""


@dataclass(frozen=True, slots=True)
class LabelScrambleSample:
    """One already eligible feature/label row for a scramble falsifier.

    The utility deliberately accepts only backtest-ready samples. It performs
    no database, calendar, or source-data lookup, so point-in-time filtering
    must happen before constructing these rows.
    """

    sample_id: str
    feature_value: float
    label_value: float
    group_key: str = "all"
    eligible: bool = True


@dataclass(frozen=True, slots=True)
class LabelScrambleResult:
    """Deterministic summary of observed and randomized-label scores."""

    observed_score: float
    scramble_scores: tuple[float, ...]
    observed_rank: int
    p_value: float
    trial_count: int
    seed: int
    alternative: LabelScrambleAlternative
    sample_count: int
    group_count: int
    score_name: str

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-stable primitive representation of the result."""

        return {
            "alternative": self.alternative,
            "group_count": self.group_count,
            "observed_rank": self.observed_rank,
            "observed_score": self.observed_score,
            "p_value": self.p_value,
            "sample_count": self.sample_count,
            "score_name": self.score_name,
            "scramble_scores": list(self.scramble_scores),
            "seed": self.seed,
            "trial_count": self.trial_count,
        }

    def to_json(self) -> str:
        """Return byte-stable JSON for reproducibility metadata."""

        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))


def run_label_scramble(
    samples: Sequence[LabelScrambleSample],
    *,
    seed: int,
    trial_count: int,
    scoring_function: LabelScrambleScoringFunction | None = None,
    alternative: LabelScrambleAlternative = "greater",
) -> LabelScrambleResult:
    """Run grouped label-scramble trials over already eligible samples.

    Each trial shuffles ``label_value`` only within ``group_key``. Sample
    identities, group membership, eligibility flags, and feature values remain
    unchanged. The default score is Spearman-style rank correlation between
    feature values and labels; callers may supply any deterministic pure
    scoring function over the trial samples.
    """

    _validate_seed(seed)
    _validate_trial_count(trial_count)
    _validate_alternative(alternative)
    normalized_samples = _normalize_samples(samples)
    groups = _group_indices(normalized_samples)
    score_fn = scoring_function or rank_correlation_score
    score_name = _score_name(scoring_function)

    observed_score = _score(normalized_samples, score_fn)
    rng = random.Random(seed)
    scramble_scores = tuple(
        _score(_scramble_once(normalized_samples, groups, rng), score_fn)
        for _ in range(trial_count)
    )
    observed_rank = 1 + sum(
        1
        for scramble_score in scramble_scores
        if _is_extreme(scramble_score, observed_score, alternative)
    )

    return LabelScrambleResult(
        observed_score=observed_score,
        scramble_scores=scramble_scores,
        observed_rank=observed_rank,
        p_value=observed_rank / (trial_count + 1),
        trial_count=trial_count,
        seed=seed,
        alternative=alternative,
        sample_count=len(normalized_samples),
        group_count=len(groups),
        score_name=score_name,
    )


def rank_correlation_score(samples: tuple[LabelScrambleSample, ...]) -> float:
    """Return Spearman-style rank correlation for feature values and labels."""

    return _rank_correlation(
        tuple(sample.feature_value for sample in samples),
        tuple(sample.label_value for sample in samples),
    )


def _normalize_samples(
    samples: Sequence[LabelScrambleSample],
) -> tuple[LabelScrambleSample, ...]:
    if isinstance(samples, (str, bytes)) or not isinstance(samples, Sequence):
        raise LabelScrambleInputError("samples must be a sequence")

    normalized: list[LabelScrambleSample] = []
    seen_ids: set[str] = set()
    for index, sample in enumerate(tuple(samples), start=1):
        if not isinstance(sample, LabelScrambleSample):
            raise LabelScrambleInputError(
                f"samples[{index}] must be a LabelScrambleSample"
            )
        if sample.eligible is not True:
            raise LabelScrambleInputError(
                "label scramble requires already eligible samples"
            )
        sample_id = _require_text(sample.sample_id, f"samples[{index}].sample_id")
        if sample_id in seen_ids:
            raise LabelScrambleInputError(f"duplicate sample_id: {sample_id}")
        seen_ids.add(sample_id)
        group_key = _require_text(sample.group_key, f"samples[{index}].group_key")
        normalized.append(
            LabelScrambleSample(
                sample_id=sample_id,
                feature_value=_finite_float(
                    sample.feature_value,
                    f"samples[{index}].feature_value",
                ),
                label_value=_finite_float(
                    sample.label_value,
                    f"samples[{index}].label_value",
                ),
                group_key=group_key,
                eligible=True,
            )
        )

    if len(normalized) < 2:
        raise LabelScrambleInputError("at least two samples are required")

    return tuple(
        sorted(normalized, key=lambda sample: (sample.group_key, sample.sample_id))
    )


def _group_indices(
    samples: tuple[LabelScrambleSample, ...],
) -> tuple[tuple[str, tuple[int, ...]], ...]:
    grouped: dict[str, list[int]] = {}
    for index, sample in enumerate(samples):
        grouped.setdefault(sample.group_key, []).append(index)
    return tuple(
        (group_key, tuple(indices)) for group_key, indices in sorted(grouped.items())
    )


def _scramble_once(
    samples: tuple[LabelScrambleSample, ...],
    groups: tuple[tuple[str, tuple[int, ...]], ...],
    rng: random.Random,
) -> tuple[LabelScrambleSample, ...]:
    scrambled_labels = [sample.label_value for sample in samples]
    for _, indices in groups:
        labels = [samples[index].label_value for index in indices]
        rng.shuffle(labels)
        for index, label in zip(indices, labels, strict=True):
            scrambled_labels[index] = label

    return tuple(
        LabelScrambleSample(
            sample_id=sample.sample_id,
            feature_value=sample.feature_value,
            label_value=scrambled_labels[index],
            group_key=sample.group_key,
            eligible=sample.eligible,
        )
        for index, sample in enumerate(samples)
    )


def _score(
    samples: tuple[LabelScrambleSample, ...],
    scoring_function: LabelScrambleScoringFunction,
) -> float:
    score = scoring_function(samples)
    if isinstance(score, bool) or not isinstance(score, int | float):
        raise LabelScrambleInputError("scoring_function must return a finite number")
    score_float = float(score)
    if not math.isfinite(score_float):
        raise LabelScrambleInputError("scoring_function must return a finite number")
    return score_float


def _score_name(
    scoring_function: LabelScrambleScoringFunction | None,
) -> str:
    if scoring_function is None:
        return "rank_correlation"
    name = getattr(scoring_function, "__name__", "")
    if not name or name == "<lambda>":
        return "custom"
    return name


def _is_extreme(
    scramble_score: float,
    observed_score: float,
    alternative: LabelScrambleAlternative,
) -> bool:
    if alternative == "greater":
        return scramble_score >= observed_score
    if alternative == "less":
        return scramble_score <= observed_score
    return abs(scramble_score) >= abs(observed_score)


def _rank_correlation(xs: Sequence[float], ys: Sequence[float]) -> float:
    if len(xs) != len(ys):
        raise LabelScrambleInputError("rank correlation inputs must have equal length")
    if len(xs) < 2:
        return 0.0
    pearson = _pearson(_average_ranks(xs), _average_ranks(ys))
    return 0.0 if pearson is None else pearson


def _average_ranks(values: Sequence[float]) -> tuple[float, ...]:
    indexed = sorted((value, index) for index, value in enumerate(values))
    ranks = [0.0] * len(indexed)
    start = 0
    while start < len(indexed):
        end = start + 1
        while end < len(indexed) and indexed[end][0] == indexed[start][0]:
            end += 1
        average_rank = (start + 1 + end) / 2.0
        for _, original_index in indexed[start:end]:
            ranks[original_index] = average_rank
        start = end
    return tuple(ranks)


def _pearson(xs: Sequence[float], ys: Sequence[float]) -> float | None:
    mean_x = _mean(xs)
    mean_y = _mean(ys)
    x_deltas = [value - mean_x for value in xs]
    y_deltas = [value - mean_y for value in ys]
    denominator = math.sqrt(
        sum(value * value for value in x_deltas)
        * sum(value * value for value in y_deltas)
    )
    if denominator == 0:
        return None
    return sum(x * y for x, y in zip(x_deltas, y_deltas, strict=True)) / denominator


def _mean(values: Sequence[float]) -> float:
    if not values:
        raise LabelScrambleInputError("cannot calculate mean of empty sequence")
    return sum(values) / len(values)


def _validate_seed(seed: int) -> None:
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise LabelScrambleInputError("seed must be an integer")


def _validate_trial_count(trial_count: int) -> None:
    if isinstance(trial_count, bool) or not isinstance(trial_count, int):
        raise LabelScrambleInputError("trial_count must be a positive integer")
    if trial_count < 1:
        raise LabelScrambleInputError("trial_count must be a positive integer")


def _validate_alternative(alternative: str) -> None:
    if alternative not in ("greater", "less", "two_sided"):
        raise LabelScrambleInputError(
            "alternative must be one of greater, less, or two_sided"
        )


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise LabelScrambleInputError(f"{field_name} must be non-empty text")
    return value


def _finite_float(value: object, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise LabelScrambleInputError(f"{field_name} must be finite")
    as_float = float(value)
    if not math.isfinite(as_float):
        raise LabelScrambleInputError(f"{field_name} must be finite")
    return as_float
