from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

WINDOW_SIZES: Tuple[int, ...] = (7, 30, 60, 90)
NEG_DAY = -1_000_000


def _empty_with_index(index: pd.DatetimeIndex) -> pd.DataFrame:
    return pd.DataFrame(index=index)


def _slug(text: str) -> str:
    out = []
    for ch in str(text).lower():
        if ch.isalnum():
            out.append(ch)
        else:
            out.append("_")
    s = "".join(out)
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_") or "unknown"


def _to_datetime(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", utc=True)


def _safe_series(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
    if col not in df.columns:
        return pd.Series(default, index=df.index)
    return pd.to_numeric(df[col], errors="coerce").fillna(default)


def _rolling_at_windows_sum(series: pd.Series, window_ends: pd.DatetimeIndex, window: int) -> pd.Series:
    if len(window_ends) == 0:
        return pd.Series(dtype=float)
    if series.empty:
        return pd.Series(0.0, index=window_ends)

    start = min(series.index.min(), window_ends.min())
    end = max(series.index.max(), window_ends.max())
    full_idx = pd.date_range(start=start.normalize(), end=end.normalize(), freq="D", tz="UTC")
    aligned = series.reindex(full_idx, fill_value=0.0).sort_index()
    rolled = aligned.rolling(window=window, min_periods=1).sum()
    return rolled.reindex(window_ends, fill_value=0.0)


def _rolling_at_windows_mean(num: pd.Series, den: pd.Series, window_ends: pd.DatetimeIndex, window: int) -> pd.Series:
    num_r = _rolling_at_windows_sum(num, window_ends, window)
    den_r = _rolling_at_windows_sum(den, window_ends, window)
    return num_r / den_r.replace(0, np.nan)


def _entropy_and_top_share(mat: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    if mat.size == 0:
        return np.array([], dtype=float), np.array([], dtype=float)

    totals = mat.sum(axis=1, keepdims=True)
    with np.errstate(divide="ignore", invalid="ignore"):
        probs = np.divide(mat, totals, where=totals > 0)
        logs = np.where(probs > 0, np.log(probs), 0.0)
        entropy = -(probs * logs).sum(axis=1)
        top_share = np.where(totals.squeeze() > 0, probs.max(axis=1), 0.0)

    entropy = np.where(np.isfinite(entropy), entropy, 0.0)
    top_share = np.where(np.isfinite(top_share), top_share, 0.0)
    return entropy, top_share


def _quantile_tier(values: np.ndarray, higher_is_better: bool, n_tiers: int = 5) -> np.ndarray:
    if values.size == 0:
        return np.array([], dtype=np.int8)
    s = pd.Series(values)
    if higher_is_better:
        ranks = s.rank(method="average", pct=True, ascending=True)
    else:
        ranks = s.rank(method="average", pct=True, ascending=False)
    tiers = np.ceil(ranks * n_tiers).astype(int).clip(1, n_tiers)
    return tiers.to_numpy(dtype=np.int8)


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
        ("purchase_items", "create_datetime"),
        ("purchase_items", "paid_datetime"),
        ("user_view", "join_datetime"),
        ("user_visitor", "visit_datetime"),
        ("user_device", "lastaccess"),
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


def _build_union_events(
    activity: pd.DataFrame,
    user_view: pd.DataFrame,
    user_visitor: pd.DataFrame,
) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []

    if not activity.empty and {"user_id", "activity_datetime"}.issubset(activity.columns):
        a = activity[["user_id", "activity_datetime"]].copy()
        a["event_ts"] = _to_datetime(a["activity_datetime"])
        frames.append(a[["user_id", "event_ts"]])

    if not user_view.empty and {"user_id", "join_datetime"}.issubset(user_view.columns):
        v = user_view[["user_id", "join_datetime"]].copy()
        v["event_ts"] = _to_datetime(v["join_datetime"])
        frames.append(v[["user_id", "event_ts"]])

    if not user_visitor.empty and {"user_id", "visit_datetime"}.issubset(user_visitor.columns):
        uv = user_visitor[["user_id", "visit_datetime"]].copy()
        uv["event_ts"] = _to_datetime(uv["visit_datetime"])
        frames.append(uv[["user_id", "event_ts"]])

    if not frames:
        return pd.DataFrame(columns=["user_id", "event_ts", "event_date"])

    out = pd.concat(frames, ignore_index=True)
    out = out.dropna(subset=["user_id", "event_ts"]).copy()
    out["event_date"] = out["event_ts"].dt.floor("D")
    return out


def _compute_engagement_features(
    activity: pd.DataFrame,
    user_view: pd.DataFrame,
    user_visitor: pd.DataFrame,
    window_ends: pd.DatetimeIndex,
    window_sizes: Sequence[int],
) -> pd.DataFrame:
    out = _empty_with_index(window_ends)
    if len(window_ends) == 0:
        return out

    union_events = _build_union_events(activity, user_view, user_visitor)
    if union_events.empty:
        out["recency_days_mean"] = 0.0
        out["recency_days_median"] = 0.0
        out["recency_days_p90"] = 0.0
        for w in window_sizes:
            tag = f"{w}d"
            out[f"active_users_{tag}"] = 0.0
            out[f"new_users_{tag}"] = 0.0
            out[f"returning_users_{tag}"] = 0.0
            out[f"dormant_share_{tag}"] = 0.0
            out[f"total_events_{tag}"] = 0.0
            out[f"total_logins_{tag}"] = 0.0
            out[f"total_views_{tag}"] = 0.0
            out[f"avg_events_per_active_{tag}"] = 0.0
            out[f"session_proxy_login_to_view_{tag}"] = 0.0
        return out

    base_day = min(union_events["event_date"].min(), window_ends.min()).floor("D")
    snapshot_days = ((window_ends.floor("D") - base_day) / pd.Timedelta(days=1)).astype(int).to_numpy()
    max_snapshot_day = int(snapshot_days.max())

    # Daily aggregate series for event counts and login/view proxy counts.
    event_daily = union_events.groupby("event_date").size().astype(float)

    login_daily = pd.Series(dtype=float)
    if not activity.empty and {"activity_datetime", "activity_type"}.issubset(activity.columns):
        a = activity[["activity_datetime", "activity_type"]].copy()
        a["event_date"] = _to_datetime(a["activity_datetime"]).dt.floor("D")
        a["is_login"] = a["activity_type"].astype(str).str.lower().str.contains("login", na=False)
        login_daily = a.loc[a["is_login"], "event_date"].value_counts().sort_index().astype(float)

    view_daily = pd.Series(dtype=float)
    view_frames: List[pd.Series] = []
    if not user_visitor.empty and "visit_datetime" in user_visitor.columns:
        s = _to_datetime(user_visitor["visit_datetime"]).dt.floor("D").value_counts().sort_index()
        view_frames.append(s)
    if not user_view.empty and "join_datetime" in user_view.columns:
        s = _to_datetime(user_view["join_datetime"]).dt.floor("D").value_counts().sort_index()
        view_frames.append(s)
    if view_frames:
        view_daily = pd.concat(view_frames, axis=1).fillna(0).sum(axis=1)

    # Exact rolling active/new/returning metrics via user-day simulation.
    user_day = union_events[["user_id", "event_date"]].drop_duplicates().copy()
    codes, _ = pd.factorize(user_day["user_id"], sort=True)
    user_day["user_code"] = codes.astype(np.int32)
    user_day["day"] = _day_int(user_day["event_date"].dt.tz_convert("UTC"), base_day).astype(int)

    n_users = int(user_day["user_code"].max()) + 1 if not user_day.empty else 0
    first_seen = (
        user_day.groupby("user_code")["day"].min().reindex(range(n_users), fill_value=10**9).to_numpy(dtype=np.int32)
    )

    day_to_users = user_day.groupby("day")["user_code"].apply(lambda s: s.to_numpy(dtype=np.int32)).to_dict()
    snapshot_map = {int(day): i for i, day in enumerate(snapshot_days)}

    n_snap = len(window_ends)
    rec_mean = np.zeros(n_snap, dtype=float)
    rec_median = np.zeros(n_snap, dtype=float)
    rec_p90 = np.zeros(n_snap, dtype=float)

    active_by_w = {w: np.zeros(n_snap, dtype=float) for w in window_sizes}
    new_by_w = {w: np.zeros(n_snap, dtype=float) for w in window_sizes}
    ret_by_w = {w: np.zeros(n_snap, dtype=float) for w in window_sizes}
    dormant_by_w = {w: np.zeros(n_snap, dtype=float) for w in window_sizes}

    last_seen = np.full(n_users, NEG_DAY, dtype=np.int32)

    for day in range(0, max_snapshot_day + 1):
        users_today = day_to_users.get(day)
        if users_today is not None:
            last_seen[users_today] = day

        if day not in snapshot_map:
            continue

        i = snapshot_map[day]
        valid_mask = last_seen > NEG_DAY
        if np.any(valid_mask):
            rec = day - last_seen[valid_mask]
            first_valid = first_seen[valid_mask]
            rec_mean[i] = float(np.mean(rec))
            rec_median[i] = float(np.median(rec))
            rec_p90[i] = float(np.percentile(rec, 90))
        else:
            rec = np.array([], dtype=np.int32)
            first_valid = np.array([], dtype=np.int32)

        for w in window_sizes:
            if rec.size == 0:
                continue
            active = int(np.count_nonzero(rec < w))
            new_users = int(np.count_nonzero((first_valid >= day - w + 1) & (first_valid <= day)))
            returning = max(active - new_users, 0)
            dormant = float(np.count_nonzero(rec >= w) / rec.size)

            active_by_w[w][i] = active
            new_by_w[w][i] = new_users
            ret_by_w[w][i] = returning
            dormant_by_w[w][i] = dormant

    out["recency_days_mean"] = rec_mean
    out["recency_days_median"] = rec_median
    out["recency_days_p90"] = rec_p90

    for w in window_sizes:
        tag = f"{w}d"
        total_events = _rolling_at_windows_sum(event_daily, window_ends, w)
        total_logins = _rolling_at_windows_sum(login_daily, window_ends, w)
        total_views = _rolling_at_windows_sum(view_daily, window_ends, w)

        out[f"active_users_{tag}"] = active_by_w[w]
        out[f"new_users_{tag}"] = new_by_w[w]
        out[f"returning_users_{tag}"] = ret_by_w[w]
        out[f"dormant_share_{tag}"] = dormant_by_w[w]
        out[f"total_events_{tag}"] = total_events.values
        out[f"total_logins_{tag}"] = total_logins.values
        out[f"total_views_{tag}"] = total_views.values
        out[f"avg_events_per_active_{tag}"] = total_events.values / np.maximum(active_by_w[w], 1.0)
        out[f"session_proxy_login_to_view_{tag}"] = total_logins.values / np.maximum(total_views.values, 1.0)

    return out


def _simulate_user_count_tiers(
    daily_user_counts: pd.DataFrame,
    window_ends: pd.DatetimeIndex,
    window_sizes: Sequence[int],
    low_max: int = 1,
    med_max: int = 3,
) -> Dict[int, pd.DataFrame]:
    """Return low/med/high share among users with count>0 for each window size."""
    out: Dict[int, pd.DataFrame] = {}
    if len(window_ends) == 0:
        return out

    if daily_user_counts.empty:
        for w in window_sizes:
            out[w] = pd.DataFrame(
                {
                    "low": np.zeros(len(window_ends)),
                    "med": np.zeros(len(window_ends)),
                    "high": np.zeros(len(window_ends)),
                },
                index=window_ends,
            )
        return out

    base_day = min(_to_datetime(daily_user_counts["date"]).dt.floor("D").min(), window_ends.min())
    df = daily_user_counts.copy()
    df["date"] = _to_datetime(df["date"]).dt.floor("D")
    df = df.dropna(subset=["user_id", "date", "count"])

    codes, _ = pd.factorize(df["user_id"], sort=True)
    df["user_code"] = codes.astype(np.int32)
    df["day"] = _day_int(df["date"], base_day).astype(int)

    n_users = int(df["user_code"].max()) + 1 if not df.empty else 0
    daily_map = {}
    for day, sub in df.groupby("day", sort=True):
        grouped = sub.groupby("user_code", as_index=False)["count"].sum()
        daily_map[int(day)] = (
            grouped["user_code"].to_numpy(dtype=np.int32),
            grouped["count"].to_numpy(dtype=np.float32),
        )

    snapshot_days = ((window_ends.floor("D") - base_day) / pd.Timedelta(days=1)).astype(int).to_numpy()
    max_snapshot_day = int(snapshot_days.max())
    snapshot_map = {int(day): i for i, day in enumerate(snapshot_days)}

    for w in window_sizes:
        running = np.zeros(n_users, dtype=np.float32)
        low = np.zeros(len(window_ends), dtype=float)
        med = np.zeros(len(window_ends), dtype=float)
        high = np.zeros(len(window_ends), dtype=float)

        for day in range(0, max_snapshot_day + 1):
            add = daily_map.get(day)
            if add is not None:
                np.add.at(running, add[0], add[1])

            rem = daily_map.get(day - w)
            if rem is not None:
                np.add.at(running, rem[0], -rem[1])

            if day not in snapshot_map:
                continue

            i = snapshot_map[day]
            active = running > 0
            n_active = int(np.count_nonzero(active))
            if n_active == 0:
                continue

            vals = running[active]
            low[i] = float(np.count_nonzero(vals <= low_max) / n_active)
            med[i] = float(np.count_nonzero((vals > low_max) & (vals <= med_max)) / n_active)
            high[i] = float(np.count_nonzero(vals > med_max) / n_active)

        out[w] = pd.DataFrame({"low": low, "med": med, "high": high}, index=window_ends)

    return out


def _compute_activity_features(
    activity: pd.DataFrame,
    window_ends: pd.DatetimeIndex,
    window_sizes: Sequence[int],
    engagement_df: pd.DataFrame,
) -> pd.DataFrame:
    out = _empty_with_index(window_ends)
    if len(window_ends) == 0:
        return out

    if activity.empty or "activity_datetime" not in activity.columns:
        for w in window_sizes:
            tag = f"{w}d"
            for col in (
                "activity_events",
                "activity_completed_events",
                "activity_completion_rate",
                "activity_points_sum",
                "activity_points_per_active",
                "activity_redeem_count",
                "activity_redeem_rate",
                "activity_redeem_per_active",
                "activity_type_entropy",
                "activity_type_top_share",
                "activity_name_entropy",
                "activity_name_top_share",
                "redeem_user_low_share",
                "redeem_user_med_share",
                "redeem_user_high_share",
            ):
                out[f"{col}_{tag}"] = 0.0
        return out

    a = activity.copy()
    a["event_date"] = _to_datetime(a["activity_datetime"]).dt.floor("D")
    a = a.dropna(subset=["event_date"])
    if a.empty:
        return out

    a["is_completed"] = a.get("is_completed", False).astype(bool)
    a["points"] = _safe_series(a, "points", default=0.0)

    # Redemption proxy: explicit redeem terms OR consume/use-like activity.
    redeem_tokens = "redeem|consume|burn|use"
    type_s = a.get("activity_type", pd.Series("", index=a.index)).astype(str).str.lower()
    name_s = a.get("activity_name", pd.Series("", index=a.index)).astype(str).str.lower()
    a["is_redeem_proxy"] = type_s.str.contains(redeem_tokens, regex=True, na=False) | name_s.str.contains(
        redeem_tokens, regex=True, na=False
    )

    total_daily = a.groupby("event_date").size().astype(float)
    completed_daily = a.loc[a["is_completed"]].groupby("event_date").size().astype(float)
    points_daily = a.groupby("event_date")["points"].sum().astype(float)
    redeem_daily = a.loc[a["is_redeem_proxy"]].groupby("event_date").size().astype(float)

    # Activity type distribution (top 6).
    top_types = (
        a.get("activity_type", pd.Series("unknown", index=a.index))
        .astype(str)
        .fillna("unknown")
        .value_counts()
        .head(6)
        .index.tolist()
    )
    type_daily = (
        a.assign(activity_type=a.get("activity_type", "unknown").astype(str).fillna("unknown"))
        .query("activity_type in @top_types")
        .groupby(["event_date", "activity_type"])  # type: ignore[arg-type]
        .size()
        .unstack(fill_value=0)
        .astype(float)
    )
    completed_type_daily = (
        a.loc[a["is_completed"]]
        .assign(activity_type=a.get("activity_type", "unknown").astype(str).fillna("unknown"))
        .query("activity_type in @top_types")
        .groupby(["event_date", "activity_type"])  # type: ignore[arg-type]
        .size()
        .unstack(fill_value=0)
        .astype(float)
    )

    # Activity name distribution (top 6) for entropy/concentration.
    top_names = (
        a.get("activity_name", pd.Series("unknown", index=a.index))
        .astype(str)
        .fillna("unknown")
        .value_counts()
        .head(6)
        .index.tolist()
    )
    name_daily = (
        a.assign(activity_name=a.get("activity_name", "unknown").astype(str).fillna("unknown"))
        .query("activity_name in @top_names")
        .groupby(["event_date", "activity_name"])  # type: ignore[arg-type]
        .size()
        .unstack(fill_value=0)
        .astype(float)
    )

    redeem_user_daily = (
        a.loc[a["is_redeem_proxy"], ["event_date", "user_id"]]
        .dropna(subset=["user_id"])
        .groupby(["event_date", "user_id"])
        .size()
        .rename("count")
        .reset_index()
        .rename(columns={"event_date": "date"})
    )
    redeem_tiers = _simulate_user_count_tiers(redeem_user_daily, window_ends, window_sizes)

    for w in window_sizes:
        tag = f"{w}d"
        events_w = _rolling_at_windows_sum(total_daily, window_ends, w)
        completed_w = _rolling_at_windows_sum(completed_daily, window_ends, w)
        points_w = _rolling_at_windows_sum(points_daily, window_ends, w)
        redeem_w = _rolling_at_windows_sum(redeem_daily, window_ends, w)

        active_users = engagement_df.get(f"active_users_{tag}", pd.Series(0.0, index=window_ends)).replace(0, np.nan)

        out[f"activity_events_{tag}"] = events_w.values
        out[f"activity_completed_events_{tag}"] = completed_w.values
        out[f"activity_completion_rate_{tag}"] = (completed_w / events_w.replace(0, np.nan)).fillna(0.0).values
        out[f"activity_points_sum_{tag}"] = points_w.values
        out[f"activity_points_per_active_{tag}"] = (points_w / active_users).fillna(0.0).values
        out[f"activity_redeem_count_{tag}"] = redeem_w.values
        out[f"activity_redeem_rate_{tag}"] = (redeem_w / events_w.replace(0, np.nan)).fillna(0.0).values
        out[f"activity_redeem_per_active_{tag}"] = (redeem_w / active_users).fillna(0.0).values

        if not type_daily.empty:
            start = min(type_daily.index.min(), window_ends.min())
            end = max(type_daily.index.max(), window_ends.max())
            idx = pd.date_range(start=start, end=end, freq="D", tz="UTC")

            type_roll = type_daily.reindex(idx, fill_value=0.0).rolling(w, min_periods=1).sum().reindex(window_ends, fill_value=0.0)
            comp_roll = (
                completed_type_daily.reindex(idx, fill_value=0.0).rolling(w, min_periods=1).sum().reindex(window_ends, fill_value=0.0)
            )
            ent, top_share = _entropy_and_top_share(type_roll.to_numpy())
            out[f"activity_type_entropy_{tag}"] = ent
            out[f"activity_type_top_share_{tag}"] = top_share

            for col in type_roll.columns:
                slug = _slug(col)
                vals = type_roll[col]
                comp = comp_roll[col] if col in comp_roll.columns else 0.0
                out[f"activity_type_{slug}_count_{tag}"] = vals.values
                out[f"activity_type_{slug}_completion_rate_{tag}"] = (
                    (comp / vals.replace(0, np.nan)).fillna(0.0).values
                )
        else:
            out[f"activity_type_entropy_{tag}"] = 0.0
            out[f"activity_type_top_share_{tag}"] = 0.0

        if not name_daily.empty:
            start = min(name_daily.index.min(), window_ends.min())
            end = max(name_daily.index.max(), window_ends.max())
            idx = pd.date_range(start=start, end=end, freq="D", tz="UTC")
            name_roll = name_daily.reindex(idx, fill_value=0.0).rolling(w, min_periods=1).sum().reindex(window_ends, fill_value=0.0)
            ent_n, top_n = _entropy_and_top_share(name_roll.to_numpy())
            out[f"activity_name_entropy_{tag}"] = ent_n
            out[f"activity_name_top_share_{tag}"] = top_n
        else:
            out[f"activity_name_entropy_{tag}"] = 0.0
            out[f"activity_name_top_share_{tag}"] = 0.0

        tier_df = redeem_tiers.get(w, pd.DataFrame(index=window_ends, data={"low": 0.0, "med": 0.0, "high": 0.0}))
        out[f"redeem_user_low_share_{tag}"] = tier_df["low"].values
        out[f"redeem_user_med_share_{tag}"] = tier_df["med"].values
        out[f"redeem_user_high_share_{tag}"] = tier_df["high"].values

    return out


def _simulate_sku_mix_features(
    purchase_items: pd.DataFrame,
    window_ends: pd.DatetimeIndex,
    window_sizes: Sequence[int],
) -> pd.DataFrame:
    out = _empty_with_index(window_ends)
    if len(window_ends) == 0:
        return out

    for w in window_sizes:
        tag = f"{w}d"
        out[f"sku_unique_{tag}"] = 0.0
        out[f"sku_top_share_{tag}"] = 0.0
        out[f"sku_entropy_{tag}"] = 0.0

    if purchase_items.empty or not {"create_datetime", "sku_id"}.issubset(purchase_items.columns):
        return out

    pi = purchase_items.copy()
    pi["date"] = _to_datetime(pi["create_datetime"]).dt.floor("D")
    pi = pi.dropna(subset=["date", "sku_id"])
    if pi.empty:
        return out

    pi["qty"] = _safe_series(pi, "quantity", default=1.0)
    pi["qty"] = pi["qty"].clip(lower=0.0)

    base_day = min(pi["date"].min(), window_ends.min()).floor("D")
    grouped = pi.groupby(["date", "sku_id"], as_index=False)["qty"].sum()

    sku_codes, _ = pd.factorize(grouped["sku_id"], sort=True)
    grouped["sku_code"] = sku_codes.astype(np.int32)
    grouped["day"] = _day_int(grouped["date"], base_day).astype(int)
    n_skus = int(grouped["sku_code"].max()) + 1 if not grouped.empty else 0

    day_map = {}
    for day, sub in grouped.groupby("day", sort=True):
        day_map[int(day)] = (
            sub["sku_code"].to_numpy(dtype=np.int32),
            sub["qty"].to_numpy(dtype=np.float32),
        )

    snapshot_days = ((window_ends.floor("D") - base_day) / pd.Timedelta(days=1)).astype(int).to_numpy()
    max_snapshot_day = int(snapshot_days.max())
    snapshot_map = {int(day): i for i, day in enumerate(snapshot_days)}

    for w in window_sizes:
        running = np.zeros(n_skus, dtype=np.float32)
        unique = np.zeros(len(window_ends), dtype=float)
        top_share = np.zeros(len(window_ends), dtype=float)
        entropy = np.zeros(len(window_ends), dtype=float)

        for day in range(0, max_snapshot_day + 1):
            add = day_map.get(day)
            if add is not None:
                np.add.at(running, add[0], add[1])

            rem = day_map.get(day - w)
            if rem is not None:
                np.add.at(running, rem[0], -rem[1])

            if day not in snapshot_map:
                continue

            i = snapshot_map[day]
            nz = running[running > 0]
            total = float(nz.sum())
            unique[i] = float(nz.size)
            if total <= 0:
                continue

            p = nz / total
            top_share[i] = float(p.max())
            entropy[i] = float(-(p * np.log(p + 1e-12)).sum())

        tag = f"{w}d"
        out[f"sku_unique_{tag}"] = unique
        out[f"sku_top_share_{tag}"] = top_share
        out[f"sku_entropy_{tag}"] = entropy

    return out


def _simulate_repeat_purchase_rate(
    purchase: pd.DataFrame,
    window_ends: pd.DatetimeIndex,
    window_sizes: Sequence[int],
) -> pd.DataFrame:
    out = _empty_with_index(window_ends)
    if len(window_ends) == 0:
        return out

    for w in window_sizes:
        tag = f"{w}d"
        out[f"buyers_{tag}"] = 0.0
        out[f"repeat_purchase_rate_{tag}"] = 0.0

    if purchase.empty or not {"create_datetime", "user_id"}.issubset(purchase.columns):
        return out

    p = purchase.copy()
    p["date"] = _to_datetime(p["create_datetime"]).dt.floor("D")
    p = p.dropna(subset=["date", "user_id"])
    if p.empty:
        return out

    p["txn_count"] = 1.0
    grouped = p.groupby(["date", "user_id"], as_index=False)["txn_count"].sum()

    base_day = min(grouped["date"].min(), window_ends.min()).floor("D")
    user_codes, _ = pd.factorize(grouped["user_id"], sort=True)
    grouped["user_code"] = user_codes.astype(np.int32)
    grouped["day"] = _day_int(grouped["date"], base_day).astype(int)
    n_users = int(grouped["user_code"].max()) + 1 if not grouped.empty else 0

    day_map = {}
    for day, sub in grouped.groupby("day", sort=True):
        day_map[int(day)] = (
            sub["user_code"].to_numpy(dtype=np.int32),
            sub["txn_count"].to_numpy(dtype=np.float32),
        )

    snapshot_days = ((window_ends.floor("D") - base_day) / pd.Timedelta(days=1)).astype(int).to_numpy()
    max_snapshot_day = int(snapshot_days.max())
    snapshot_map = {int(day): i for i, day in enumerate(snapshot_days)}

    for w in window_sizes:
        running = np.zeros(n_users, dtype=np.float32)
        buyers = np.zeros(len(window_ends), dtype=float)
        repeat_rate = np.zeros(len(window_ends), dtype=float)

        for day in range(0, max_snapshot_day + 1):
            add = day_map.get(day)
            if add is not None:
                np.add.at(running, add[0], add[1])

            rem = day_map.get(day - w)
            if rem is not None:
                np.add.at(running, rem[0], -rem[1])

            if day not in snapshot_map:
                continue

            i = snapshot_map[day]
            buyer_mask = running > 0
            n_buyers = int(np.count_nonzero(buyer_mask))
            buyers[i] = n_buyers
            if n_buyers == 0:
                continue
            repeat_rate[i] = float(np.count_nonzero(running[buyer_mask] > 1) / n_buyers)

        tag = f"{w}d"
        out[f"buyers_{tag}"] = buyers
        out[f"repeat_purchase_rate_{tag}"] = repeat_rate

    return out


def _compute_commerce_features(
    purchase: pd.DataFrame,
    purchase_items: pd.DataFrame,
    window_ends: pd.DatetimeIndex,
    window_sizes: Sequence[int],
) -> pd.DataFrame:
    out = _empty_with_index(window_ends)
    if len(window_ends) == 0:
        return out

    for w in window_sizes:
        tag = f"{w}d"
        cols = [
            f"transaction_count_{tag}",
            f"transaction_success_rate_{tag}",
            f"gmv_net_{tag}",
            f"gmv_sell_{tag}",
            f"discount_rate_{tag}",
            f"aov_net_{tag}",
            f"items_per_transaction_{tag}",
            f"paid_delay_hours_{tag}",
            f"delivered_rate_{tag}",
            f"shipped_rate_{tag}",
        ]
        for c in cols:
            out[c] = 0.0

    repeat_df = _simulate_repeat_purchase_rate(purchase, window_ends, window_sizes)
    sku_df = _simulate_sku_mix_features(purchase_items, window_ends, window_sizes)
    out = pd.concat([out, repeat_df, sku_df], axis=1)

    if purchase.empty or "create_datetime" not in purchase.columns:
        return out

    p = purchase.copy()
    p["create_date"] = _to_datetime(p["create_datetime"]).dt.floor("D")
    p["paid_date"] = _to_datetime(p.get("paid_datetime", pd.NaT)).dt.floor("D")
    p = p.dropna(subset=["create_date"])
    if p.empty:
        return out

    p["net_amount"] = _safe_series(p, "net_amount", default=0.0)
    p["subtotal_amount"] = _safe_series(p, "subtotal_amount", default=0.0)
    p["discount_amount"] = _safe_series(p, "discount_amount", default=0.0)
    p["itemsold"] = _safe_series(p, "itemsold", default=0.0)
    status_s = p.get("transaction_status", pd.Series("unknown", index=p.index)).astype(str).str.lower()
    p["is_success"] = status_s.eq("success")

    txn_daily = p.groupby("create_date").size().astype(float)
    success_daily = p.loc[p["is_success"]].groupby("create_date").size().astype(float)
    net_daily = p.groupby("create_date")["net_amount"].sum().astype(float)
    subtotal_daily = p.groupby("create_date")["subtotal_amount"].sum().astype(float)
    discount_daily = p.groupby("create_date")["discount_amount"].sum().astype(float)
    itemsold_daily = p.groupby("create_date")["itemsold"].sum().astype(float)

    lag_hours = (_to_datetime(p.get("paid_datetime", pd.NaT)) - _to_datetime(p["create_datetime"])).dt.total_seconds() / 3600.0
    lag_hours = lag_hours.clip(lower=0).fillna(0.0)
    p["lag_hours"] = lag_hours
    lag_sum_daily = p.groupby("create_date")["lag_hours"].sum().astype(float)
    lag_cnt_daily = p.groupby("create_date")["lag_hours"].size().astype(float)

    tx_overlap = 0.0
    if not purchase_items.empty and {"transaction_id"}.issubset(p.columns) and {"transaction_id"}.issubset(purchase_items.columns):
        p_tx = p["transaction_id"].dropna().astype(str)
        pi_tx = purchase_items["transaction_id"].dropna().astype(str)
        if len(p_tx):
            tx_overlap = float(p_tx.isin(set(pi_tx.unique())).mean())

    use_items_for_value = tx_overlap >= 0.50

    gmv_net_daily = pd.Series(dtype=float)
    gmv_sell_daily = pd.Series(dtype=float)
    qty_daily = pd.Series(dtype=float)
    delivered_num_daily = pd.Series(dtype=float)
    delivered_den_daily = pd.Series(dtype=float)
    shipped_num_daily = pd.Series(dtype=float)
    shipped_den_daily = pd.Series(dtype=float)

    if not purchase_items.empty and "create_datetime" in purchase_items.columns:
        pi = purchase_items.copy()
        pi["create_date"] = _to_datetime(pi["create_datetime"]).dt.floor("D")
        pi = pi.dropna(subset=["create_date"])
        if not pi.empty:
            pi["quantity"] = _safe_series(pi, "quantity", default=1.0).clip(lower=0.0)
            pi["price_net"] = _safe_series(pi, "price_net", default=0.0)
            pi["price_sell"] = _safe_series(pi, "price_sell", default=0.0)
            pi["gmv_net"] = pi["quantity"] * pi["price_net"]
            pi["gmv_sell"] = pi["quantity"] * pi["price_sell"]

            gmv_net_daily = pi.groupby("create_date")["gmv_net"].sum().astype(float)
            gmv_sell_daily = pi.groupby("create_date")["gmv_sell"].sum().astype(float)
            qty_daily = pi.groupby("create_date")["quantity"].sum().astype(float)

            delivered = pi.get("delivered", False)
            delivered = delivered.fillna(False).astype(bool)
            shipped = pi.get("is_shiped", False)
            if shipped.dtype != bool:
                shipped = shipped.fillna(False).astype(bool)

            pi["delivered_i"] = delivered.astype(float)
            pi["shipped_i"] = shipped.astype(float)
            delivered_num_daily = pi.groupby("create_date")["delivered_i"].sum().astype(float)
            delivered_den_daily = pi.groupby("create_date")["delivered_i"].size().astype(float)
            shipped_num_daily = pi.groupby("create_date")["shipped_i"].sum().astype(float)
            shipped_den_daily = pi.groupby("create_date")["shipped_i"].size().astype(float)

    if not use_items_for_value or gmv_net_daily.empty:
        gmv_net_daily = net_daily.copy()
    if not use_items_for_value or gmv_sell_daily.empty:
        gmv_sell_daily = subtotal_daily.copy()
    if qty_daily.empty:
        qty_daily = itemsold_daily.copy()

    for w in window_sizes:
        tag = f"{w}d"
        tx_w = _rolling_at_windows_sum(txn_daily, window_ends, w)
        success_w = _rolling_at_windows_sum(success_daily, window_ends, w)
        net_w = _rolling_at_windows_sum(gmv_net_daily, window_ends, w)
        sell_w = _rolling_at_windows_sum(gmv_sell_daily, window_ends, w)
        qty_w = _rolling_at_windows_sum(qty_daily, window_ends, w)

        lag_w = _rolling_at_windows_mean(lag_sum_daily, lag_cnt_daily, window_ends, w)
        delivered_w = _rolling_at_windows_mean(delivered_num_daily, delivered_den_daily, window_ends, w)
        shipped_w = _rolling_at_windows_mean(shipped_num_daily, shipped_den_daily, window_ends, w)

        out[f"transaction_count_{tag}"] = tx_w.values
        out[f"transaction_success_rate_{tag}"] = (success_w / tx_w.replace(0, np.nan)).fillna(0.0).values
        out[f"gmv_net_{tag}"] = net_w.values
        out[f"gmv_sell_{tag}"] = sell_w.values
        out[f"discount_rate_{tag}"] = ((sell_w - net_w) / sell_w.replace(0, np.nan)).fillna(0.0).values
        out[f"aov_net_{tag}"] = (net_w / tx_w.replace(0, np.nan)).fillna(0.0).values
        out[f"items_per_transaction_{tag}"] = (qty_w / tx_w.replace(0, np.nan)).fillna(0.0).values
        out[f"paid_delay_hours_{tag}"] = lag_w.fillna(0.0).values
        out[f"delivered_rate_{tag}"] = delivered_w.fillna(0.0).values
        out[f"shipped_rate_{tag}"] = shipped_w.fillna(0.0).values

        # Status mix (top 4 status buckets).
        top_status = status_s.value_counts().head(4).index.tolist()
        for status in top_status:
            key = _slug(status)
            status_daily = p.loc[status_s.eq(status)].groupby("create_date").size().astype(float)
            status_w = _rolling_at_windows_sum(status_daily, window_ends, w)
            out[f"status_{key}_rate_{tag}"] = (status_w / tx_w.replace(0, np.nan)).fillna(0.0).values

    return out


def _compute_rfm_features(
    activity: pd.DataFrame,
    purchase: pd.DataFrame,
    window_ends: pd.DatetimeIndex,
) -> pd.DataFrame:
    out = _empty_with_index(window_ends)
    if len(window_ends) == 0:
        return out

    base_cols = {
        "rfm_recency_mean": 0.0,
        "rfm_frequency_mean": 0.0,
        "rfm_monetary_mean": 0.0,
        "rfm_score_mean": 0.0,
        "rfm_transition_up_share": 0.0,
        "rfm_transition_down_share": 0.0,
        "rfm_dormant_share": 0.0,
    }
    for k, v in base_cols.items():
        out[k] = v

    for dim in ("recency", "frequency", "monetary", "score"):
        for tier in range(1, 6):
            out[f"rfm_{dim}_tier_{tier}_pct"] = 0.0

    # Build user universe
    users = []
    if not activity.empty and "user_id" in activity.columns:
        users.append(activity["user_id"].dropna().astype(str))
    if not purchase.empty and "user_id" in purchase.columns:
        users.append(purchase["user_id"].dropna().astype(str))
    if not users:
        return out

    user_all = pd.concat(users, ignore_index=True).dropna()
    if user_all.empty:
        return out

    user_codes, user_uniques = pd.factorize(user_all, sort=True)
    user_map = pd.Series(np.arange(len(user_uniques)), index=user_uniques)
    n_users = len(user_uniques)

    # Activity updates
    activity_presence = {}
    completed_map = {}
    points_map = {}

    if not activity.empty and {"user_id", "activity_datetime"}.issubset(activity.columns):
        a = activity.copy()
        a["date"] = _to_datetime(a["activity_datetime"]).dt.floor("D")
        a = a.dropna(subset=["date", "user_id"])
        if not a.empty:
            base_day = min(a["date"].min(), window_ends.min()).floor("D")
        else:
            base_day = window_ends.min().floor("D")
    else:
        base_day = window_ends.min().floor("D")

    if not activity.empty and {"user_id", "activity_datetime"}.issubset(activity.columns):
        a = activity.copy()
        a["date"] = _to_datetime(a["activity_datetime"]).dt.floor("D")
        a = a.dropna(subset=["date", "user_id"])
        if not a.empty:
            a["user_key"] = a["user_id"].astype(str)
            a = a[a["user_key"].isin(user_map.index)]
            a["user_code"] = a["user_key"].map(user_map).astype(np.int32)
            a["day"] = _day_int(a["date"], base_day).astype(int)
            a["points"] = _safe_series(a, "points", default=0.0)
            a["is_completed"] = a.get("is_completed", False).astype(bool)

            # Presence by day.
            for day, sub in a.groupby("day", sort=True):
                activity_presence[int(day)] = sub["user_code"].drop_duplicates().to_numpy(dtype=np.int32)

            # Completed counts by user/day.
            comp = (
                a.loc[a["is_completed"]]
                .groupby(["day", "user_code"], as_index=False)
                .size()
                .rename(columns={"size": "count"})
            )
            for day, sub in comp.groupby("day", sort=True):
                completed_map[int(day)] = (
                    sub["user_code"].to_numpy(dtype=np.int32),
                    sub["count"].to_numpy(dtype=np.float32),
                )

            # Points by user/day.
            pts = a.groupby(["day", "user_code"], as_index=False)["points"].sum()
            for day, sub in pts.groupby("day", sort=True):
                points_map[int(day)] = (
                    sub["user_code"].to_numpy(dtype=np.int32),
                    sub["points"].to_numpy(dtype=np.float32),
                )

    # Purchase updates
    purchase_presence = {}
    purchase_txn_map = {}
    purchase_net_map = {}

    if not purchase.empty and {"user_id", "create_datetime"}.issubset(purchase.columns):
        p = purchase.copy()
        p["date"] = _to_datetime(p["create_datetime"]).dt.floor("D")
        p = p.dropna(subset=["date", "user_id"])
        if not p.empty:
            base_day = min(base_day, p["date"].min().floor("D"))
            p["user_key"] = p["user_id"].astype(str)
            p = p[p["user_key"].isin(user_map.index)]
            p["user_code"] = p["user_key"].map(user_map).astype(np.int32)
            p["day"] = _day_int(p["date"], base_day).astype(int)
            p["net_amount"] = _safe_series(p, "net_amount", default=0.0)

            for day, sub in p.groupby("day", sort=True):
                purchase_presence[int(day)] = sub["user_code"].drop_duplicates().to_numpy(dtype=np.int32)

            tx = p.groupby(["day", "user_code"], as_index=False).size().rename(columns={"size": "txn"})
            for day, sub in tx.groupby("day", sort=True):
                purchase_txn_map[int(day)] = (
                    sub["user_code"].to_numpy(dtype=np.int32),
                    sub["txn"].to_numpy(dtype=np.float32),
                )

            net = p.groupby(["day", "user_code"], as_index=False)["net_amount"].sum()
            for day, sub in net.groupby("day", sort=True):
                purchase_net_map[int(day)] = (
                    sub["user_code"].to_numpy(dtype=np.int32),
                    sub["net_amount"].to_numpy(dtype=np.float32),
                )

    snapshot_days = ((window_ends.floor("D") - base_day) / pd.Timedelta(days=1)).astype(int).to_numpy()
    max_snapshot_day = int(snapshot_days.max())
    snapshot_map = {int(day): i for i, day in enumerate(snapshot_days)}

    last_activity = np.full(n_users, NEG_DAY, dtype=np.int32)
    last_purchase = np.full(n_users, NEG_DAY, dtype=np.int32)

    freq90 = np.zeros(n_users, dtype=np.float32)
    mon90 = np.zeros(n_users, dtype=np.float32)
    points90 = np.zeros(n_users, dtype=np.float32)

    prev_score_tier = None

    for day in range(0, max_snapshot_day + 1):
        # Presence updates
        a_users = activity_presence.get(day)
        if a_users is not None:
            last_activity[a_users] = day

        p_users = purchase_presence.get(day)
        if p_users is not None:
            last_purchase[p_users] = day

        # Add today's values
        for src, arr in ((purchase_txn_map, freq90), (purchase_net_map, mon90), (completed_map, freq90), (points_map, points90)):
            add = src.get(day)
            if add is not None:
                np.add.at(arr, add[0], add[1])

        # Remove expired (90d horizon)
        expired_day = day - 90
        for src, arr in ((purchase_txn_map, freq90), (purchase_net_map, mon90), (completed_map, freq90), (points_map, points90)):
            rem = src.get(expired_day)
            if rem is not None:
                np.add.at(arr, rem[0], -rem[1])

        if day not in snapshot_map:
            continue

        i = snapshot_map[day]

        rec_source = np.where(last_purchase > NEG_DAY, last_purchase, last_activity)
        valid = rec_source > NEG_DAY
        if not np.any(valid):
            continue

        rec = day - rec_source[valid]
        freq = freq90[valid]
        monetary = mon90[valid] + points90[valid]

        rec_tier = _quantile_tier(rec.astype(float), higher_is_better=False)
        freq_tier = _quantile_tier(freq.astype(float), higher_is_better=True)
        mon_tier = _quantile_tier(monetary.astype(float), higher_is_better=True)
        score_raw = rec_tier + freq_tier + mon_tier
        score_tier = _quantile_tier(score_raw.astype(float), higher_is_better=True)

        out.iloc[i, out.columns.get_loc("rfm_recency_mean")] = float(np.mean(rec))
        out.iloc[i, out.columns.get_loc("rfm_frequency_mean")] = float(np.mean(freq))
        out.iloc[i, out.columns.get_loc("rfm_monetary_mean")] = float(np.mean(monetary))
        out.iloc[i, out.columns.get_loc("rfm_score_mean")] = float(np.mean(score_raw))
        out.iloc[i, out.columns.get_loc("rfm_dormant_share")] = float(np.count_nonzero(rec_tier <= 2) / len(rec_tier))

        if prev_score_tier is not None and len(prev_score_tier) == len(score_tier):
            out.iloc[i, out.columns.get_loc("rfm_transition_up_share")] = float(np.count_nonzero(score_tier > prev_score_tier) / len(score_tier))
            out.iloc[i, out.columns.get_loc("rfm_transition_down_share")] = float(
                np.count_nonzero(score_tier < prev_score_tier) / len(score_tier)
            )
        prev_score_tier = score_tier

        for tier in range(1, 6):
            out.iloc[i, out.columns.get_loc(f"rfm_recency_tier_{tier}_pct")] = float(np.count_nonzero(rec_tier == tier) / len(rec_tier))
            out.iloc[i, out.columns.get_loc(f"rfm_frequency_tier_{tier}_pct")] = float(
                np.count_nonzero(freq_tier == tier) / len(freq_tier)
            )
            out.iloc[i, out.columns.get_loc(f"rfm_monetary_tier_{tier}_pct")] = float(np.count_nonzero(mon_tier == tier) / len(mon_tier))
            out.iloc[i, out.columns.get_loc(f"rfm_score_tier_{tier}_pct")] = float(np.count_nonzero(score_tier == tier) / len(score_tier))

    out = out.fillna(0.0)
    return out


def _flatten_windows(snapshot_df: pd.DataFrame, brand_id: str, window_sizes: Sequence[int]) -> pd.DataFrame:
    window_suffixes = [f"_{w}d" for w in window_sizes]
    rows: List[pd.DataFrame] = []

    for w in window_sizes:
        suffix = f"_{w}d"
        d: Dict[str, pd.Series] = {
            "brand_id": pd.Series(brand_id, index=snapshot_df.index),
            "window_end_date": pd.Series(snapshot_df.index, index=snapshot_df.index),
            "window_size": pd.Series(f"{w}d", index=snapshot_df.index),
            "window_size_days": pd.Series(float(w), index=snapshot_df.index),
        }

        for col in snapshot_df.columns:
            if col.endswith(suffix):
                d[col[: -len(suffix)]] = snapshot_df[col]
            elif not any(col.endswith(ws) for ws in window_suffixes):
                d[col] = snapshot_df[col]

        rows.append(pd.DataFrame(d))

    out = pd.concat(rows, ignore_index=True)
    return out


def _add_relative_and_trend_features(feature_df: pd.DataFrame) -> pd.DataFrame:
    df = feature_df.copy().sort_values(["brand_id", "window_size", "window_end_date"]).reset_index(drop=True)

    # Relative features.
    df["new_user_share"] = df["new_users"] / df["active_users"].replace(0, np.nan)
    df["returning_user_share"] = df["returning_users"] / df["active_users"].replace(0, np.nan)
    df["gmv_per_active"] = df["gmv_net"] / df["active_users"].replace(0, np.nan)
    df["transactions_per_active"] = df["transaction_count"] / df["active_users"].replace(0, np.nan)
    df["points_per_completion"] = df["activity_points_sum"] / df["activity_completed_events"].replace(0, np.nan)
    df["reward_efficiency"] = df["activity_completed_events"] / (df["activity_points_sum"].abs() + 1.0)

    key_metrics = [
        "active_users",
        "gmv_net",
        "transaction_count",
        "activity_completion_rate",
        "activity_redeem_rate",
        "reward_efficiency",
        "dormant_share",
    ]

    group_cols = ["brand_id", "window_size"]

    for m in key_metrics:
        g = df.groupby(group_cols, observed=True)[m]
        df[f"{m}_wow_pct"] = g.pct_change(periods=1)
        df[f"{m}_mom_pct"] = g.pct_change(periods=4)

        rolling_mean = g.transform(lambda s: s.rolling(window=8, min_periods=3).mean())
        rolling_std = g.transform(lambda s: s.rolling(window=8, min_periods=3).std())
        df[f"{m}_zscore"] = (df[m] - rolling_mean) / rolling_std.replace(0, np.nan)
        df[f"{m}_volatility"] = rolling_std / rolling_mean.abs().replace(0, np.nan)

    df = df.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return df


def build_feature_table(
    tables: Mapping[str, pd.DataFrame],
    window_sizes: Sequence[int] = WINDOW_SIZES,
    snapshot_freq: str = "7D",
) -> pd.DataFrame:
    """Build brand-level features at (brand_id, window_end_date, window_size)."""
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

    all_rows: List[pd.DataFrame] = []

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
        purchase_items = brand_tables.get("purchase_items", pd.DataFrame())
        user_view = brand_tables.get("user_view", pd.DataFrame())
        user_visitor = brand_tables.get("user_visitor", pd.DataFrame())

        engagement_df = _compute_engagement_features(
            activity=activity,
            user_view=user_view,
            user_visitor=user_visitor,
            window_ends=window_ends,
            window_sizes=window_sizes,
        )

        activity_df = _compute_activity_features(
            activity=activity,
            window_ends=window_ends,
            window_sizes=window_sizes,
            engagement_df=engagement_df,
        )

        commerce_df = _compute_commerce_features(
            purchase=purchase,
            purchase_items=purchase_items,
            window_ends=window_ends,
            window_sizes=window_sizes,
        )

        rfm_df = _compute_rfm_features(
            activity=activity,
            purchase=purchase,
            window_ends=window_ends,
        )

        snapshot_df = pd.concat([engagement_df, activity_df, commerce_df, rfm_df], axis=1).fillna(0.0)
        flat_df = _flatten_windows(snapshot_df, brand_id=brand_id, window_sizes=window_sizes)
        all_rows.append(flat_df)

    if not all_rows:
        return pd.DataFrame()

    features = pd.concat(all_rows, ignore_index=True)
    features["window_end_date"] = pd.to_datetime(features["window_end_date"], errors="coerce", utc=True)

    # Guardrail columns expected downstream.
    required_defaults = {
        "active_users": 0.0,
        "new_users": 0.0,
        "returning_users": 0.0,
        "dormant_share": 0.0,
        "gmv_net": 0.0,
        "transaction_count": 0.0,
        "activity_completion_rate": 0.0,
        "activity_redeem_rate": 0.0,
        "activity_points_sum": 0.0,
        "activity_completed_events": 0.0,
    }
    for col, val in required_defaults.items():
        if col not in features.columns:
            features[col] = val

    features = _add_relative_and_trend_features(features)
    features = features.sort_values(["brand_id", "window_size", "window_end_date"]).reset_index(drop=True)
    return features


def feature_definitions(feature_df: pd.DataFrame) -> pd.DataFrame:
    """Generate a compact schema dictionary for produced features."""
    rows = []
    for c in feature_df.columns:
        if c in {"brand_id", "window_end_date", "window_size", "window_size_days"}:
            meaning = "identifier"
        elif c.endswith("_wow_pct"):
            meaning = "week-over-week percent change"
        elif c.endswith("_mom_pct"):
            meaning = "month-over-month percent change (4 weekly windows)"
        elif c.endswith("_zscore"):
            meaning = "rolling z-score vs trailing baseline"
        elif c.endswith("_volatility"):
            meaning = "rolling std/mean over trailing windows"
        elif c.startswith("rfm_"):
            meaning = "RFM-based user distribution or transition metric"
        elif "share" in c:
            meaning = "ratio/share feature"
        elif "rate" in c:
            meaning = "rate/proportion feature"
        elif "entropy" in c:
            meaning = "distribution entropy"
        else:
            meaning = "aggregated KPI metric"

        rows.append({"feature_name": c, "meaning": meaning})

    return pd.DataFrame(rows)
