
def map_genero(tipsex) -> str:
    value = (str(tipsex).strip().upper() if tipsex is not None else "")
    if value == "F":
        return "feminino"
    if value == "M":
        return "masculino"
    return "masculino"
