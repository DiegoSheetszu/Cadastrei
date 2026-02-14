from decimal import Decimal


def to_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float, Decimal)):
        return value != 0

    text = str(value).strip().lower()
    return text in {"1", "true", "t", "sim", "s", "y", "yes"}
