
def format_cpf(value) -> str | None:
    if value is None:
        return None

    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if not digits:
        return None

    if len(digits) < 11:
        digits = digits.zfill(11)
    if len(digits) == 11:
        return f"{digits[:3]}.{digits[3:6]}.{digits[6:9]}-{digits[9:]}"

    return digits
