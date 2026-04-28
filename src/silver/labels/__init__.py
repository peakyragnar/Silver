"""Forward-return label calculation helpers."""

from silver.labels.forward_returns import (
    ForwardReturnLabel,
    ForwardReturnLabelBatch,
    ForwardReturnLabelInputError,
    SkippedForwardReturnLabel,
    SkipReason,
    calculate_forward_return_labels,
)
from silver.labels.materialize import (
    ForwardLabelMaterializationResult,
    build_forward_label_records,
)
from silver.labels.repository import (
    ForwardLabelPersistenceError,
    ForwardLabelPriceObservation,
    ForwardLabelRecord,
    ForwardLabelRepository,
    ForwardLabelWriteResult,
)

__all__ = [
    "ForwardReturnLabel",
    "ForwardReturnLabelBatch",
    "ForwardReturnLabelInputError",
    "ForwardLabelMaterializationResult",
    "ForwardLabelPersistenceError",
    "ForwardLabelPriceObservation",
    "ForwardLabelRecord",
    "ForwardLabelRepository",
    "ForwardLabelWriteResult",
    "SkippedForwardReturnLabel",
    "SkipReason",
    "build_forward_label_records",
    "calculate_forward_return_labels",
]
