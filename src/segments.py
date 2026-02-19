from __future__ import annotations

from typing import Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from .id_utils import normalize_id

SEGMENT_KEYS: Tuple[str, ...] = (
    "new_users_0_7d",
    "active_0_7d",
    "engaged_no_redeem",
    "redeemers",
    "recently_lapsed_8_14d",
    "dormant_15_30d",
)

SEGMENT_METRIC_SUFFIXES: Tuple[str, ...] = (
    "users",
    "activity_completion_rate",
    "redeem_rate",
    "dormant_share",
    "transactions",
    "gmv_net",
)

NEG_DAY = -1_000_000


def _to_datetime(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", utc=True)


def _safe_num(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
    if col not in df.columns:
        return pd.Series(default, index=df.index)
    return pd.to_numeric(df[col], errors="coerce").fillna(default)


def _day_int(ts: pd.Series, base_day: pd.Timestamp) -> pd.Series:
    return ((ts.dt.floor("D") - base_day) / pd.Timedelta(days=1)).astype("Int64")


def _build_window_dates(
    brand_tables: Mapping[str, pd.DataFrame],
    window_sizes: Sequence[int],
    freq: str,
) -> pd.DatetimeIndex:
    dt_values: List[pd.Timestamp] = []
    candidates = [
        ("activity_transaction", "activity_datetime"),
        ("purchase", "create_datetime"),
        ("purchase", "paid_datetime"),
        ("user_view", "join_datetime"),
        ("user_visitor", "visit_datetime"),
    ]

    for table_name, col in candidates:
        df = brand_tables.get(table_name, pd.DataFrame())
        if df.empty or col not in df.columns:
            continue
        dt = _to_datetime(df[col]).dropna()
        if not dt.empty:
            dt_values.append(dt.min())
            dt_values.append(dt.max())

    if not dt_values:
        return pd.DatetimeIndex([], tz="UTC")

    global_min = min(dt_values).floor("D")
    global_max = max(dt_values).floor("D")
    start = global_min + pd.Timedelta(days=max(window_sizes))
    if start > global_max:
        start = global_min

    return pd.date_range(start=start, end=global_max, freq=freq, tz="UTC")


def _build_presence_map(df: pd.DataFrame, day_col: str, user_col: str) -> Dict[int, np.ndarray]:
    if df.empty:
        return {}
    grouped = df.groupby(day_col, sort=True)[user_col].apply(lambda s: s.drop_duplicates().to_numpy(dtype=np.int32))
    return {int(day): arr for day, arr in grouped.items()}


def _build_value_map(df: pd.DataFrame, day_col: str, user_col: str, value_col: str) -> Dict[int, Tuple[np.ndarray, np.ndarray]]:
    if df.empty:
        return {}
    g = df.groupby([day_col, user_col], as_index=False)[value_col].sum()
    out: Dict[int, Tuple[np.ndarray, np.ndarray]] = {}
    for day, sub in g.groupby(day_col, sort=True):
        out[int(day)] = (
            sub[user_col].to_numpy(dtype=np.int32),
            sub[value_col].to_numpy(dtype=np.float32),
        )
    return out


def _assign_segment_codes(
    day: int,
    seen_mask: np.ndarray,
    rec_days: np.ndarray,
    first_seen: np.ndarray,
    activity_count: np.ndarray,
    redeem_count: np.ndarray,
) -> np.ndarray:
    seg_code = np.full(len(seen_mask), -1, dtype=np.int8)

    new_mask = seen_mask & (first_seen >= day - 6) & (first_seen <= day)
    seg_code[new_mask] = 0

    redeemer_mask = seen_mask & (redeem_count >= 2)
    seg_code[(seg_code < 0) & redeemer_mask] = 3

    engaged_mask = seen_mask & (activity_count >= 3) & (redeem_count <= 0) & (rec_days <= 7)
    seg_code[(seg_code < 0) & engaged_mask] = 2

    active_mask = seen_mask & (rec_days <= 7)
    seg_code[(seg_code < 0) & active_mask] = 1

    lapsed_mask = seen_mask & (rec_days >= 8) & (rec_days <= 14)
    seg_code[(seg_code < 0) & lapsed_mask] = 4

    dormant_mask = seen_mask & (rec_days >= 15) & (rec_days <= 30)
    seg_code[(seg_code < 0) & dormant_mask] = 5

    return seg_code


def compute_segment_kpis(
    tables: Mapping[str, pd.DataFrame],
    window_sizes: Sequence[int] = (7, 30, 60, 90),
    snapshot_freq: str = "7D",
) -> pd.DataFrame:
    brands = sorted(
        set(
            pd.concat(
                [
                    t["brand_id"]
                    for t in tables.values()
                    if isinstance(t, pd.DataFrame) and not t.empty and "brand_id" in t.columns
                ],
                ignore_index=True,
            ).dropna().astype(str)
            if tables
            else []
        )
    )

    rows: List[dict] = []

    for brand_id in brands:
        brand_tables: Dict[str, pd.DataFrame] = {}
        for table_name, df in tables.items():
            if not isinstance(df, pd.DataFrame) or df.empty or "brand_id" not in df.columns:
                brand_tables[table_name] = pd.DataFrame()
                continue
            brand_tables[table_name] = df.loc[df["brand_id"] == brand_id].copy()

        window_ends = _build_window_dates(brand_tables, window_sizes=window_sizes, freq=snapshot_freq)
        if len(window_ends) == 0:
            continue

        activity = brand_tables.get("activity_transaction", pd.DataFrame())
        purchase = brand_tables.get("purchase", pd.DataFrame())
        user_view = brand_tables.get("user_view", pd.DataFrame())
        user_visitor = brand_tables.get("user_visitor", pd.DataFrame())

        frames: List[pd.DataFrame] = []
        if not activity.empty and {"user_id", "activity_datetime"}.issubset(activity.columns):
            a = activity[["user_id", "activity_datetime"]].copy()
            a["date"] = _to_datetime(a["activity_datetime"]).dt.floor("D")
            a["user_norm"] = normalize_id(a["user_id"])
            frames.append(a[["user_norm", "date"]])

        if not user_view.empty and {"user_id", "join_datetime"}.issubset(user_view.columns):
            v = user_view[["user_id", "join_datetime"]].copy()
            v["date"] = _to_datetime(v["join_datetime"]).dt.floor("D")
            v["user_norm"] = normalize_id(v["user_id"])
            frames.append(v[["user_norm", "date"]])

        if not user_visitor.empty and {"user_id", "visit_datetime"}.issubset(user_visitor.columns):
            uv = user_visitor[["user_id", "visit_datetime"]].copy()
            uv["date"] = _to_datetime(uv["visit_datetime"]).dt.floor("D")
            uv["user_norm"] = normalize_id(uv["user_id"])
            frames.append(uv[["user_norm", "date"]])

        presence = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["user_norm", "date"])
        presence = presence.dropna(subset=["user_norm", "date"]).drop_duplicates()

        a_metric = pd.DataFrame(columns=["user_norm", "date", "activity_count", "completed_count", "redeem_count"])
        if not activity.empty and {"user_id", "activity_datetime"}.issubset(activity.columns):
            a = activity.copy()
            a["date"] = _to_datetime(a["activity_datetime"]).dt.floor("D")
            a["user_norm"] = normalize_id(a["user_id"])
            a = a.dropna(subset=["user_norm", "date"])
            a["is_completed_i"] = a.get("is_completed", False).fillna(False).astype(bool).astype(float)
            typ = a.get("activity_type", pd.Series("", index=a.index)).astype(str).str.lower()
            name = a.get("activity_name", pd.Series("", index=a.index)).astype(str).str.lower()
            redeem = typ.str.contains("redeem|consume|burn|use", regex=True, na=False) | name.str.contains(
                "redeem|consume|burn|use", regex=True, na=False
            )
            a["redeem_i"] = redeem.astype(float)

            a_metric = (
                a.groupby(["date", "user_norm"], as_index=False)
                .agg(activity_count=("user_norm", "size"), completed_count=("is_completed_i", "sum"), redeem_count=("redeem_i", "sum"))
            )

        p_metric = pd.DataFrame(columns=["user_norm", "date", "txn_count", "gmv_net"])
        if not purchase.empty and {"user_id", "create_datetime"}.issubset(purchase.columns):
            p = purchase.copy()
            p["date"] = _to_datetime(p["create_datetime"]).dt.floor("D")
            p["user_norm"] = normalize_id(p["user_id"])
            p = p.dropna(subset=["user_norm", "date"])
            if not p.empty:
                p["txn_count"] = 1.0
                p["gmv_net"] = _safe_num(p, "net_amount", default=0.0)
                p_metric = p.groupby(["date", "user_norm"], as_index=False).agg(txn_count=("txn_count", "sum"), gmv_net=("gmv_net", "sum"))

        users = pd.concat(
            [
                presence.get("user_norm", pd.Series(dtype="string")),
                a_metric.get("user_norm", pd.Series(dtype="string")),
                p_metric.get("user_norm", pd.Series(dtype="string")),
            ],
            ignore_index=True,
        ).dropna()
        users = users.astype(str)
        if users.empty:
            continue

        user_codes, user_uniques = pd.factorize(users, sort=True)
        user_map = pd.Series(np.arange(len(user_uniques)), index=user_uniques)
        n_users = len(user_uniques)

        min_dates = [window_ends.min()]
        for df in (presence, a_metric, p_metric):
            if not df.empty and "date" in df.columns:
                min_dates.append(df["date"].min())
        base_day = min([d.floor("D") for d in min_dates if pd.notna(d)])

        first_seen = np.full(n_users, 10**9, dtype=np.int32)

        def _encode(df: pd.DataFrame, val_cols: Sequence[str]) -> pd.DataFrame:
            if df.empty:
                return pd.DataFrame(columns=["day", "user_code", *val_cols])
            x = df.copy()
            x["user_norm"] = x["user_norm"].astype(str)
            x = x[x["user_norm"].isin(user_map.index)]
            if x.empty:
                return pd.DataFrame(columns=["day", "user_code", *val_cols])
            x["user_code"] = x["user_norm"].map(user_map).astype(np.int32)
            x["day"] = _day_int(pd.to_datetime(x["date"], utc=True), base_day).astype(int)
            return x[["day", "user_code", *val_cols]]

        p_presence_enc = _encode(p_metric[["date", "user_norm"]].drop_duplicates() if not p_metric.empty else pd.DataFrame(), [])
        a_presence_enc = _encode(presence, [])

        for enc in (p_presence_enc, a_presence_enc):
            if enc.empty:
                continue
            min_by_user = enc.groupby("user_code")["day"].min()
            idx = min_by_user.index.to_numpy(dtype=np.int32)
            vals = min_by_user.to_numpy(dtype=np.int32)
            first_seen[idx] = np.minimum(first_seen[idx], vals)

        first_seen[first_seen >= 10**9] = NEG_DAY

        act_enc = _encode(a_metric[["date", "user_norm", "activity_count", "completed_count", "redeem_count"]], ["activity_count", "completed_count", "redeem_count"])
        p_enc = _encode(p_metric[["date", "user_norm", "txn_count", "gmv_net"]], ["txn_count", "gmv_net"])

        activity_presence_map = _build_presence_map(a_presence_enc, "day", "user_code")
        purchase_presence_map = _build_presence_map(p_presence_enc, "day", "user_code")

        act_map = _build_value_map(act_enc, "day", "user_code", "activity_count")
        comp_map = _build_value_map(act_enc, "day", "user_code", "completed_count")
        red_map = _build_value_map(act_enc, "day", "user_code", "redeem_count")
        txn_map = _build_value_map(p_enc, "day", "user_code", "txn_count")
        gmv_map = _build_value_map(p_enc, "day", "user_code", "gmv_net")

        snapshot_days = ((window_ends.floor("D") - base_day) / pd.Timedelta(days=1)).astype(int).to_numpy()
        max_day = int(snapshot_days.max())
        snapshot_map = {int(day): i for i, day in enumerate(snapshot_days)}

        last_activity = np.full(n_users, NEG_DAY, dtype=np.int32)
        last_purchase = np.full(n_users, NEG_DAY, dtype=np.int32)

        run_act = {w: np.zeros(n_users, dtype=np.float32) for w in window_sizes}
        run_comp = {w: np.zeros(n_users, dtype=np.float32) for w in window_sizes}
        run_red = {w: np.zeros(n_users, dtype=np.float32) for w in window_sizes}
        run_tx = {w: np.zeros(n_users, dtype=np.float32) for w in window_sizes}
        run_gmv = {w: np.zeros(n_users, dtype=np.float32) for w in window_sizes}

        for day in range(0, max_day + 1):
            a_users = activity_presence_map.get(day)
            if a_users is not None:
                last_activity[a_users] = day
            p_users = purchase_presence_map.get(day)
            if p_users is not None:
                last_purchase[p_users] = day

            for w in window_sizes:
                for src, arr in (
                    (act_map, run_act[w]),
                    (comp_map, run_comp[w]),
                    (red_map, run_red[w]),
                    (txn_map, run_tx[w]),
                    (gmv_map, run_gmv[w]),
                ):
                    add = src.get(day)
                    if add is not None:
                        np.add.at(arr, add[0], add[1])
                    rem = src.get(day - w)
                    if rem is not None:
                        np.add.at(arr, rem[0], -rem[1])

            if day not in snapshot_map:
                continue

            i = snapshot_map[day]
            rec_source = np.maximum(last_activity, last_purchase)
            seen = rec_source > NEG_DAY
            rec_days = np.where(seen, day - rec_source, 10**6)

            for w in window_sizes:
                seg_code = _assign_segment_codes(
                    day=day,
                    seen_mask=seen,
                    rec_days=rec_days,
                    first_seen=first_seen,
                    activity_count=run_act[w],
                    redeem_count=run_red[w],
                )
                total_segmented = int(np.count_nonzero(seg_code >= 0))

                rec = {
                    "brand_id": brand_id,
                    "window_end_date": window_ends[i],
                    "window_size": f"{w}d",
                    "window_size_days": float(w),
                }

                for idx, key in enumerate(SEGMENT_KEYS):
                    mask = seg_code == idx
                    n_seg = int(np.count_nonzero(mask))
                    share = float(n_seg / total_segmented) if total_segmented > 0 else 0.0

                    if n_seg > 0:
                        act_sum = float(run_act[w][mask].sum())
                        comp_sum = float(run_comp[w][mask].sum())
                        red_sum = float(run_red[w][mask].sum())
                        txn_sum = float(run_tx[w][mask].sum())
                        gmv_sum = float(run_gmv[w][mask].sum())
                        completion_rate = float(comp_sum / act_sum) if act_sum > 0 else 0.0
                        redeem_rate = float(red_sum / act_sum) if act_sum > 0 else 0.0
                        dormant_share = float(np.count_nonzero(rec_days[mask] >= 15) / n_seg)
                    else:
                        completion_rate = 0.0
                        redeem_rate = 0.0
                        dormant_share = 0.0
                        txn_sum = 0.0
                        gmv_sum = 0.0

                    rec[f"seg_{key}_users"] = float(n_seg)
                    rec[f"seg_{key}_share"] = share
                    rec[f"seg_{key}_activity_completion_rate"] = completion_rate
                    rec[f"seg_{key}_redeem_rate"] = redeem_rate
                    rec[f"seg_{key}_dormant_share"] = dormant_share
                    rec[f"seg_{key}_transactions"] = txn_sum
                    rec[f"seg_{key}_gmv_net"] = gmv_sum

                rows.append(rec)

    if not rows:
        return pd.DataFrame()

    out = pd.DataFrame(rows)
    out["window_end_date"] = pd.to_datetime(out["window_end_date"], errors="coerce", utc=True)
    out = out.sort_values(["brand_id", "window_size", "window_end_date"]).reset_index(drop=True)
    return out
