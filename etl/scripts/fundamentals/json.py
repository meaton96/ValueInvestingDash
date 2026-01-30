from typing import List, Tuple
import orjson as jsonlib
from etl.scripts.utilities.normalize import normalize_value_unit

def extract_rows_from_json(cik: int, buf_or_obj, source_file: str, TAG_MAP) -> List[Tuple]:
    """
    Return rows matching FUND_COLS from one companyfacts JSON, limited to TAG_MAP.
    """
    if isinstance(buf_or_obj, (bytes, bytearray)):
        try:
            j = jsonlib.loads(buf_or_obj)
        except Exception:
            return []
    elif isinstance(buf_or_obj, dict):
        j = buf_or_obj
    else:
        return []

    facts = j.get("facts", {})
    if not isinstance(facts, dict):
        return []

    # crude filing_date derivation: try "entity.commonStockSharesOutstanding" frames' 'end' or fall back to submissions logic later
    filing_date_guess = None
    # if available in companyfacts root, use "entity.commonStockSharesOutstanding" erg... not consistent.
    # We'll default to None and allow NULL filtering later if needed; better: you can pass in a mapping from submissions.

    us_gaap = facts.get("us-gaap", {})
    if not isinstance(us_gaap, dict):
        return []

    rows: List[Tuple] = []

    for canon, candidates in TAG_MAP.items():
        tag_payload = None
        tag_found = None
        for t in candidates:
            tp = us_gaap.get(t)
            if isinstance(tp, dict):
                tag_payload = tp
                tag_found = t
                break
        if not tag_payload:
            continue

        units = tag_payload.get("units", {})
        if not isinstance(units, dict):
            continue

        for unit, entries in units.items():
            if not isinstance(entries, list):
                continue
            for e in entries:
                if not isinstance(e, dict):
                    continue
                val_raw = e.get("val")
                accn = e.get("accn")
                fy = e.get("fy")
                fp = e.get("fp")
                frame = e.get("frame")
                # many entries also have 'end' (ISO date). Use that as filing_date proxy.
                end_date = e.get("end")
                filing_date = None
                if end_date:
                    try:
                        filing_date = end_date[:10]  # YYYY-MM-DD
                    except Exception:
                        filing_date = None

                if not accn:
                    continue

                val, unit_norm = normalize_value_unit(val_raw, unit if isinstance(unit, str) else str(unit))
                if val is None:
                    continue

                rows.append((
                    cik, accn, fy, fp, tag_found, val, unit_norm, frame,
                    filing_date,  # may be None; DB column is DATE, NULL allowed in staging, not in raw
                    source_file
                ))
    return rows