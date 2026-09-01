from enum import StrEnum


class Severity(StrEnum):
    NORMAL = "NORMAL"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class ImpactClassification(StrEnum):
    NORMAL = "NORMAL"
    OWN_DRIFT = "OWN_DRIFT"
    INHERITED_DELAY = "INHERITED_DELAY"
    COMBINED = "COMBINED"


class MappedTaskAggregation(StrEnum):
    MAX = "MAX"
    MEAN = "MEAN"
    SUM = "SUM"


class ChangeDirection(StrEnum):
    INCREASE = "INCREASE"
    DECREASE = "DECREASE"


class TrendDirection(StrEnum):
    INCREASING = "INCREASING"
    DECREASING = "DECREASING"


SEVERITY_SCORE: dict[Severity, int] = {
    Severity.NORMAL: 0,
    Severity.MEDIUM: 1,
    Severity.HIGH: 2,
    Severity.CRITICAL: 3,
}
