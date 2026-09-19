import math
from collections.abc import Iterable

from flowsense.domain import InvalidObservationError


def validate_finite_observations(
    subject_id: str,
    values: Iterable[float],
) -> None:
    if any(not math.isfinite(value) for value in values):
        raise InvalidObservationError(subject_id)
