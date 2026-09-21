from datetime import timedelta

import numpy as np


DELIVERED_RATE = 0.99
REVIEW_RATE = 0.95
DELAYED_RATE = 0.08

ITEM_COUNT_VALUES = np.array([1, 2, 3, 4, 5], dtype=np.int64)
ITEM_COUNT_PROBS = np.array([0.900, 0.080, 0.013, 0.005, 0.002])

PAYMENT_TYPE_VALUES = np.array(
    ["credit_card", "boleto", "voucher", "debit_card"], dtype=object
)
PAYMENT_TYPE_PROBS = np.array([0.740, 0.190, 0.055, 0.015])

REVIEW_SCORE_VALUES = np.array([1, 2, 3, 4, 5], dtype=np.int64)
REVIEW_SCORE_PROBS = np.array([0.115, 0.032, 0.082, 0.193, 0.578])


def _choice(rng, values, probabilities=None):
    value = rng.choice(values, p=probabilities)
    return value.item() if hasattr(value, "item") else value


def sample_item_count(rng):
    return int(_choice(rng, ITEM_COUNT_VALUES, ITEM_COUNT_PROBS))


def sample_payment_type(rng):
    return str(_choice(rng, PAYMENT_TYPE_VALUES, PAYMENT_TYPE_PROBS))


def sample_installments(rng, payment_type, credit_card_installments):
    if payment_type != "credit_card":
        return 1
    return int(_choice(rng, credit_card_installments))


def sample_review_score(rng):
    return int(_choice(rng, REVIEW_SCORE_VALUES, REVIEW_SCORE_PROBS))


def sample_empirical_delay(rng, positive_duration_seconds):
    return timedelta(seconds=int(_choice(rng, positive_duration_seconds)))


def sample_review_delay(rng):
    return timedelta(seconds=int(rng.integers(1, 24 * 60 * 60 + 1)))


def sample_answer_delay(rng):
    return timedelta(seconds=int(rng.integers(1, 168 * 60 * 60 + 1)))


def sample_cancel_delay(rng):
    return timedelta(seconds=int(rng.integers(1, 48 * 60 * 60 + 1)))


def sample_estimated_delivery(rng, purchase, delivered, is_delayed):
    if delivered is None:
        return purchase + timedelta(
            seconds=int(rng.integers(5 * 86400, 30 * 86400 + 1))
        )
    if is_delayed:
        return delivered - timedelta(
            seconds=int(rng.integers(1, 3 * 86400 + 1))
        )
    return delivered + timedelta(
        seconds=int(rng.integers(1, 14 * 86400 + 1))
    )


for _probabilities in (
    ITEM_COUNT_PROBS,
    PAYMENT_TYPE_PROBS,
    REVIEW_SCORE_PROBS,
):
    if not np.isclose(_probabilities.sum(), 1.0):
        raise ValueError("Distribution probabilities must sum to 1.0")
