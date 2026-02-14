
def format_cnpj(value) -> str | None:
    if value is None:
        return None

    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if not digits:
        return None

    if len(digits) < 14:
        digits = digits.zfill(14)
    if len(digits) == 14:
        return f"{digits[:2]}.{digits[2:5]}.{digits[5:8]}/{digits[8:12]}-{digits[12:]}"

    return digits
