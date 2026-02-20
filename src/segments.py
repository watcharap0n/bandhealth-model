from __future__ import annotations

from typing import Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from .id_utils import normalize_id

ACTIVITY_SEGMENT_KEYS: Tuple[str, ...] = (
    "active_0_7d",
    "recently_lapsed_8_14d",
    "dormant_15_30d",
    "dormant_31_60d",
    "dormant_60d_plus",
    "new_users_0_7d",
    "redeemers",
    "non_redeemers",
)

COMMERCE_SEGMENT_KEYS: Tuple[str, ...] = (
    "buyers",
    "repeat_buyers",
    "high_aov_buyers",
    "discount_sensitive",
    "sku_affinity_top1",
)

SEGMENT_KEYS: Tuple[str, ...] = ACTIVITY_SEGMENT_KEYS + COMMERCE_SEGMENT_KEYS

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
    use_activity_enrichment: bool,
) -> pd.DatetimeIndex:
    dt_values: List[pd.Timestamp] = []
    candidates = [
        ("activity_transaction", "activity_datetime"),
        ("purchase", "create_datetime"),
        ("purchase", "paid_datetime"),
    ]
    if use_activity_enrichment:
        candidates.extend(
            [
                ("user_view", "join_datetime"),
                ("user_visitor", "visit_datetime"),
            ]
        )

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


def _collect_brands(tables: Mapping[str, pd.DataFrame]) -> List[str]:
    brands: set = set()
    for df in tables.values():
        if isinstance(df, pd.DataFrame) and not df.empty and "brand_id" in df.columns:
            brands.update(df["brand_id"].dropna().astype(str).tolist())
    return sorted(brands)


def _encode(
    df: pd.DataFrame,
    date_col: str,
    user_col: str,
    user_map: pd.Series,
    base_day: pd.Timestamp,
    val_cols: Sequence[str],
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["day", "user_code", *val_cols])

    x = df.copy()
    x[user_col] = x[user_col].astype(str)
    x = x[x[user_col].isin(user_map.index)]
    if x.empty:
        return pd.DataFrame(columns=["day", "user_code", *val_cols])
    x["user_code"] = x[user_col].map(user_map).astype(np.int32)
    x["day"] = _day_int(pd.to_datetime(x[date_col], utc=True), base_day).astype(int)
    return x[["day", "user_code", *val_cols]]


def compute_segment_kpis(
    tables: Mapping[str, pd.DataFrame],
    window_sizes: Sequence[int] = (7, 30, 60, 90),
    snapshot_freq: str = "7D",
    commerce_joinable_by_brand: Optional[Mapping[str, bool]] = None,
    activity_enrichment_joinable_by_brand: Optional[Mapping[str, bool]] = None,
) -> pd.DataFrame:
    rows: List[dict] = []

    for brand_id in _collect_brands(tables):
        brand_tables: Dict[str, pd.DataFrame] = {}
        for table_name, df in tables.items():
            if not isinstance(df, pd.DataFrame) or df.empty or "brand_id" not in df.columns:
                brand_tables[table_name] = pd.DataFrame()
            else:
                brand_tables[table_name] = df.loc[df["brand_id"] == brand_id].copy()

        commerce_joinable = True if commerce_joinable_by_brand is None else bool(commerce_joinable_by_brand.get(brand_id, False))
        use_activity_enrichment = True if activity_enrichment_joinable_by_brand is None else bool(activity_enrichment_joinable_by_brand.get(brand_id, False))

        window_ends = _build_window_dates(
            brand_tables,
            window_sizes=window_sizes,
            freq=snapshot_freq,
            use_activity_enrichment=use_activity_enrichment,
        )
        if len(window_ends) == 0:
            continue

        activity = brand_tables.get("activity_transaction", pd.DataFrame())
        user_view = brand_tables.get("user_view", pd.DataFrame())
        user_visitor = brand_tables.get("user_visitor", pd.DataFrame())
        purchase = brand_tables.get("purchase", pd.DataFrame())
        purchase_items = brand_tables.get("purchase_items", pd.DataFrame())

        presence_frames: List[pd.DataFrame] = []
        if not activity.empty and {"user_id", "activity_datetime"}.issubset(activity.columns):
            a = activity[["user_id", "activity_datetime"]].copy()
            a["user_norm"] = normalize_id(a["user_id"])
            a["date"] = _to_datetime(a["activity_datetime"]).dt.floor("D")
            presence_frames.append(a[["user_norm", "date"]])

        if use_activity_enrichment and (not user_view.empty) and {"user_id", "join_datetime"}.issubset(user_view.columns):
            v = user_view[["user_id", "join_datetime"]].copy()
            v["user_norm"] = normalize_id(v["user_id"])
            v["date"] = _to_datetime(v["join_datetime"]).dt.floor("D")
            presence_frames.append(v[["user_norm", "date"]])

        if use_activity_enrichment and (not user_visitor.empty) and {"user_id", "visit_datetime"}.issubset(user_visitor.columns):
            uv = user_visitor[["user_id", "visit_datetime"]].copy()
            uv["user_norm"] = normalize_id(uv["user_id"])
            uv["date"] = _to_datetime(uv["visit_datetime"]).dt.floor("D")
            presence_frames.append(uv[["user_norm", "date"]])

        presence = pd.concat(presence_frames, ignore_index=True) if presence_frames else pd.DataFrame(columns=["user_norm", "date"])
        presence = presence.dropna(subset=["user_norm", "date"]).drop_duplicates()

        # Activity metrics.
        a_metric = pd.DataFrame(columns=["user_norm", "date", "activity_count", "completed_count", "redeem_count"])
        if not activity.empty and {"user_id", "activity_datetime"}.issubset(activity.columns):
            a = activity.copy()
            a["user_norm"] = normalize_id(a["user_id"])
            a["date"] = _to_datetime(a["activity_datetime"]).dt.floor("D")
            a = a.dropna(subset=["user_norm", "date"])
            a["is_completed_i"] = pd.to_numeric(a.get("is_completed", 0), errors="coerce").fillna(0.0)
            typ = a.get("activity_type", pd.Series("", index=a.index)).astype(str).str.lower()
            name = a.get("activity_name", pd.Series("", index=a.index)).astype(str).str.lower()
            redeem = typ.str.contains("redeem|consume|burn|use", regex=True, na=False) | name.str.contains("redeem|consume|burn|use", regex=True, na=False)
            a["redeem_i"] = redeem.astype(float)
            a_metric = (
                a.groupby(["date", "user_norm"], as_index=False)
                .agg(
                    activity_count=("user_norm", "size"),
                    completed_count=("is_completed_i", "sum"),
                    redeem_count=("redeem_i", "sum"),
                )
            )

        # Purchase metrics (user-level).
        p_metric = pd.DataFrame(columns=["user_norm", "date", "txn_count", "gmv_net", "discount_amount", "subtotal_amount"])
        if not purchase.empty and {"user_id", "create_datetime"}.issubset(purchase.columns):
            p = purchase.copy()
            p["user_norm"] = normalize_id(p["user_id"])
            p["date"] = _to_datetime(p["create_datetime"]).dt.floor("D")
            p["transaction_id_norm"] = normalize_id(p.get("transaction_id"))
            p = p.dropna(subset=["user_norm", "date"])
            if not p.empty:
                p["txn_count"] = 1.0
                p["gmv_net"] = _safe_num(p, "net_amount", default=0.0)
                p["discount_amount"] = _safe_num(p, "discount_amount", default=0.0)
                p["subtotal_amount"] = _safe_num(p, "subtotal_amount", default=0.0)
                p_metric = (
                    p.groupby(["date", "user_norm"], as_index=False)
                    .agg(
                        txn_count=("txn_count", "sum"),
                        gmv_net=("gmv_net", "sum"),
                        discount_amount=("discount_amount", "sum"),
                        subtotal_amount=("subtotal_amount", "sum"),
                    )
                )
        else:
            p = pd.DataFrame(columns=["transaction_id_norm", "user_norm", "date"])

        top_sku_user_day = pd.DataFrame(columns=["date", "user_norm", "top_sku_hits"])
        if commerce_joinable and (not purchase_items.empty) and (not p.empty):
            pi = purchase_items.copy()
            pi["transaction_id_norm"] = normalize_id(pi.get("transaction_id"))
            pi["sku_id"] = pi.get("sku_id")
            pi["qty"] = _safe_num(pi, "quantity", default=1.0).clip(lower=0.0)
            tx_user = p[["transaction_id_norm", "user_norm", "date"]].dropna(subset=["transaction_id_norm", "user_norm", "date"])
            tx_user = tx_user.drop_duplicates(subset=["transaction_id_norm"])
            joined = pi.merge(tx_user, on="transaction_id_norm", how="inner")
            joined = joined.dropna(subset=["user_norm", "date", "sku_id"])
            if not joined.empty:
                top_sku = (
                    joined.groupby("sku_id")["qty"].sum().sort_values(ascending=False).index.astype(str).tolist()
                )
                if top_sku:
                    sku0 = top_sku[0]
                    top_sku_user_day = joined.loc[joined["sku_id"].astype(str) == str(sku0), ["date", "user_norm", "qty"]].copy()
                    top_sku_user_day["top_sku_hits"] = top_sku_user_day["qty"].astype(float)
                    top_sku_user_day = (
                        top_sku_user_day.groupby(["date", "user_norm"], as_index=False)["top_sku_hits"].sum()
                    )

        # User universe.
        user_parts = [
            presence.get("user_norm", pd.Series(dtype="string")),
            a_metric.get("user_norm", pd.Series(dtype="string")),
            p_metric.get("user_norm", pd.Series(dtype="string")),
            top_sku_user_day.get("user_norm", pd.Series(dtype="string")),
        ]
        users = pd.concat(user_parts, ignore_index=True).dropna().astype(str)
        if users.empty:
            continue

        user_uniques = pd.Index(users.unique())
        user_map = pd.Series(np.arange(len(user_uniques), dtype=np.int32), index=user_uniques)
        n_users = len(user_uniques)

        min_dates = [window_ends.min()]
        for df, c in ((presence, "date"), (a_metric, "date"), (p_metric, "date"), (top_sku_user_day, "date")):
            if not df.empty and c in df.columns:
                min_dates.append(df[c].min())
        base_day = min([d.floor("D") for d in min_dates if pd.notna(d)])

        first_seen = np.full(n_users, 10**9, dtype=np.int32)

        presence_enc = _encode(presence, "date", "user_norm", user_map, base_day, [])
        if not presence_enc.empty:
            first = presence_enc.groupby("user_code")["day"].min()
            idx = first.index.to_numpy(dtype=np.int32)
            first_seen[idx] = np.minimum(first_seen[idx], first.to_numpy(dtype=np.int32))
        first_seen[first_seen >= 10**9] = NEG_DAY

        act_enc = _encode(a_metric, "date", "user_norm", user_map, base_day, ["activity_count", "completed_count", "redeem_count"])
        pur_enc = _encode(p_metric, "date", "user_norm", user_map, base_day, ["txn_count", "gmv_net", "discount_amount", "subtotal_amount"])
        top_sku_enc = _encode(top_sku_user_day, "date", "user_norm", user_map, base_day, ["top_sku_hits"])

        presence_map = _build_presence_map(presence_enc, "day", "user_code")
        act_map = _build_value_map(act_enc, "day", "user_code", "activity_count")
        comp_map = _build_value_map(act_enc, "day", "user_code", "completed_count")
        red_map = _build_value_map(act_enc, "day", "user_code", "redeem_count")
        txn_map = _build_value_map(pur_enc, "day", "user_code", "txn_count")
        gmv_map = _build_value_map(pur_enc, "day", "user_code", "gmv_net")
        disc_map = _build_value_map(pur_enc, "day", "user_code", "discount_amount")
        subtotal_map = _build_value_map(pur_enc, "day", "user_code", "subtotal_amount")
        top_sku_map = _build_value_map(top_sku_enc, "day", "user_code", "top_sku_hits")

        snapshot_days = ((window_ends.floor("D") - base_day) / pd.Timedelta(days=1)).astype(int).to_numpy()
        max_day = int(snapshot_days.max())
        snapshot_map = {int(day): i for i, day in enumerate(snapshot_days)}

        last_seen = np.full(n_users, NEG_DAY, dtype=np.int32)

        run_act = {w: np.zeros(n_users, dtype=np.float32) for w in window_sizes}
        run_comp = {w: np.zeros(n_users, dtype=np.float32) for w in window_sizes}
        run_red = {w: np.zeros(n_users, dtype=np.float32) for w in window_sizes}
        run_tx = {w: np.zeros(n_users, dtype=np.float32) for w in window_sizes}
        run_gmv = {w: np.zeros(n_users, dtype=np.float32) for w in window_sizes}
        run_disc = {w: np.zeros(n_users, dtype=np.float32) for w in window_sizes}
        run_subtotal = {w: np.zeros(n_users, dtype=np.float32) for w in window_sizes}
        run_top_sku = {w: np.zeros(n_users, dtype=np.float32) for w in window_sizes}

        for day in range(0, max_day + 1):
            seen_users = presence_map.get(day)
            if seen_users is not None:
                last_seen[seen_users] = day

            for w in window_sizes:
                for src, arr in (
                    (act_map, run_act[w]),
                    (comp_map, run_comp[w]),
                    (red_map, run_red[w]),
                    (txn_map, run_tx[w]),
                    (gmv_map, run_gmv[w]),
                    (disc_map, run_disc[w]),
                    (subtotal_map, run_subtotal[w]),
                    (top_sku_map, run_top_sku[w]),
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
            seen = last_seen > NEG_DAY
            rec_days = np.where(seen, day - last_seen, 10**6)

            for w in window_sizes:
                buyers_mask = run_tx[w] > 0
                population_mask = seen | buyers_mask
                pop_count = int(np.count_nonzero(population_mask))

                # Segment masks (non-exclusive on purpose for actionability).
                masks: Dict[str, np.ndarray] = {
                    "active_0_7d": seen & (rec_days <= 7),
                    "recently_lapsed_8_14d": seen & (rec_days >= 8) & (rec_days <= 14),
                    "dormant_15_30d": seen & (rec_days >= 15) & (rec_days <= 30),
                    "dormant_31_60d": seen & (rec_days >= 31) & (rec_days <= 60),
                    "dormant_60d_plus": seen & (rec_days > 60),
                    "new_users_0_7d": seen & (rec_days <= 7) & (first_seen >= day - 6) & (first_seen <= day),
                    "redeemers": population_mask & (run_red[w] > 0),
                    "non_redeemers": population_mask & (run_red[w] <= 0),
                }

                if commerce_joinable:
                    user_aov = np.divide(
                        run_gmv[w],
                        run_tx[w],
                        out=np.zeros_like(run_gmv[w]),
                        where=run_tx[w] > 0,
                    )
                    buyer_aov = user_aov[buyers_mask]
                    aov_q80 = float(np.quantile(buyer_aov, 0.8)) if buyer_aov.size else np.nan

                    user_discount_rate = np.divide(
                        run_disc[w],
                        run_subtotal[w],
                        out=np.zeros_like(run_disc[w]),
                        where=run_subtotal[w] > 0,
                    )
                    buyer_disc = user_discount_rate[buyers_mask & (run_subtotal[w] > 0)]
                    disc_q80 = float(np.quantile(buyer_disc, 0.8)) if buyer_disc.size else np.nan

                    masks["buyers"] = buyers_mask
                    masks["repeat_buyers"] = run_tx[w] >= 2
                    masks["high_aov_buyers"] = buyers_mask & np.isfinite(aov_q80) & (user_aov >= aov_q80)
                    masks["discount_sensitive"] = buyers_mask & np.isfinite(disc_q80) & (user_discount_rate >= disc_q80)
                    masks["sku_affinity_top1"] = run_top_sku[w] > 0
                else:
                    masks["buyers"] = np.zeros(n_users, dtype=bool)
                    masks["repeat_buyers"] = np.zeros(n_users, dtype=bool)
                    masks["high_aov_buyers"] = np.zeros(n_users, dtype=bool)
                    masks["discount_sensitive"] = np.zeros(n_users, dtype=bool)
                    masks["sku_affinity_top1"] = np.zeros(n_users, dtype=bool)

                rec = {
                    "brand_id": brand_id,
                    "window_end_date": window_ends[i],
                    "window_size": f"{w}d",
                    "window_size_days": float(w),
                    "commerce_joinable": float(commerce_joinable),
                    "activity_enrichment_joinable": float(use_activity_enrichment),
                }

                for seg in SEGMENT_KEYS:
                    mask = masks.get(seg, np.zeros(n_users, dtype=bool))
                    n_seg = int(np.count_nonzero(mask))
                    share = float(n_seg / pop_count) if pop_count > 0 else 0.0

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

                    rec[f"seg_{seg}_users"] = float(n_seg)
                    rec[f"seg_{seg}_share"] = share
                    rec[f"seg_{seg}_activity_completion_rate"] = completion_rate
                    rec[f"seg_{seg}_redeem_rate"] = redeem_rate
                    rec[f"seg_{seg}_dormant_share"] = dormant_share
                    rec[f"seg_{seg}_transactions"] = txn_sum
                    rec[f"seg_{seg}_gmv_net"] = gmv_sum

                rows.append(rec)

    if not rows:
        return pd.DataFrame()

    out = pd.DataFrame(rows)
    out["window_end_date"] = pd.to_datetime(out["window_end_date"], errors="coerce", utc=True)
    out = out.sort_values(["brand_id", "window_size", "window_end_date"]).reset_index(drop=True)
    return out
