class FlowSenseError(Exception):
    """Base exception for expected FlowSense failures."""


class InsufficientHistoryError(FlowSenseError, ValueError):
    def __init__(
        self,
        subject_id: str,
        required: int,
        actual: int,
    ) -> None:
        self.subject_id = subject_id
        self.required = required
        self.actual = actual
        super().__init__(
            f"{subject_id} için drift hesaplamak için en az {required} run gerekli."
        )


class InvalidTaskTimingError(FlowSenseError, ValueError):
    """Raised when task timestamps cannot produce a valid handoff timing."""
