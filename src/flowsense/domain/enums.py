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


class ChangeDirection(StrEnum):
    INCREASE = "INCREASE"
    DECREASE = "DECREASE"


SEVERITY_SCORE: dict[Severity, int] = {
    Severity.NORMAL: 0,
    Severity.MEDIUM: 1,
    Severity.HIGH: 2,
    Severity.CRITICAL: 3,
}
