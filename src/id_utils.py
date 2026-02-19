from __future__ import annotations

import pandas as pd


ID_EMPTY_TOKENS = {"", "nan", "none", "<na>", "nat"}


def normalize_id(
    values,
    *,
    lower: bool = True,
    remove_non_alnum_except_dash_underscore: bool = True,
) -> pd.Series:
    """Normalize string-like identifiers for robust joins.

    Steps:
    - cast to pandas string
    - strip whitespace
    - lower (optional)
    - remove trailing .0 artifact from numeric coercions
    - optional removal of non [a-z0-9_-]
    """
    s = pd.Series(values, copy=True)
    s = s.astype("string")
    s = s.str.strip()
    s = s.str.replace(r"\.0+$", "", regex=True)

    if lower:
        s = s.str.lower()

    if remove_non_alnum_except_dash_underscore:
        s = s.str.replace(r"[^a-z0-9_-]", "", regex=True)

    s = s.replace({tok: pd.NA for tok in ID_EMPTY_TOKENS})
    return s
