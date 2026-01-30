from typing import Tuple
def normalize_value_unit(value, unit: str) -> Tuple[float | None, str]:
    if value is None:
        return None, unit
    try:
        v = float(value)
    except Exception:
        return None, unit

    unit_norm = unit
    if unit and isinstance(unit, str) and unit.upper().startswith("USD"):
        suffix = unit.upper()[3:]
        if suffix == "M" or suffix in ("MM", "MN"):
            v *= 1_000_000
        elif suffix in ("B", "BN"):
            v *= 1_000_000_000
        elif suffix in ("TH", "THS", "THOUSANDS"):
            v *= 1_000
        unit_norm = "USD"
    elif unit and unit.lower() in ("shares", "shrs"):
        unit_norm = "shares"

    return v, unit_norm


