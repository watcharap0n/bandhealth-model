from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from sklearn.cluster import MiniBatchKMeans
from sklearn.decomposition import PCA

LABEL_COL = "label_health_class"
TIME_COL = "window_end_date"
ROW_ID_COL = "__row_id"


@dataclass
class TrainSampleConfig:
    mode: str = "off"
    seed: int = 42
    frac: float = 0.02
    max_train_rows: int = 200_000
    max_eval_rows: int = 60_000
    recent_days: int = 180
    stratify_cols: Tuple[str, ...] = ("brand_id", LABEL_COL)
    group_col: str = "brand_id"
    min_rows_per_class: int = 200
    qa_label_share_drift_tol: float = 0.10
    qa_metric_drift_std_tol: float = 0.30


@dataclass
class SampleResult:
    sampled_df: pd.DataFrame
    sampled_train_df: pd.DataFrame
    sampled_eval_df: pd.DataFrame
    train_row_ids: List[int]
    eval_row_ids: List[int]
    qa_report: Dict
    config_used: Dict
    fallback_applied: bool


def _safe_to_datetime(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", utc=True)


def _resolve_stratify_cols(df: pd.DataFrame, requested: Sequence[str]) -> List[str]:
    alias = {"predicted_health_class": LABEL_COL}
    out: List[str] = []
    for c in requested:
        cc = str(c).strip()
        if not cc:
            continue
        cc = alias.get(cc, cc)
        if cc in df.columns and cc not in out:
            out.append(cc)

    if LABEL_COL in df.columns and LABEL_COL not in out:
        out.append(LABEL_COL)
    if not out:
        for c in ["brand_id", LABEL_COL]:
            if c in df.columns and c not in out:
                out.append(c)
    return out


def _build_strata_key(df: pd.DataFrame, cols: Sequence[str]) -> pd.Series:
    if not cols:
        return pd.Series("all", index=df.index, dtype="object")
    if len(cols) == 1:
        return df[cols[0]].astype(str)
    return df[list(cols)].astype(str).agg("|".join, axis=1)


def _time_split_mask(df: pd.DataFrame, time_col: str = TIME_COL, train_frac: float = 0.8) -> Tuple[pd.Series, pd.Timestamp]:
    ts = _safe_to_datetime(df[time_col])
    cutoff = ts.quantile(train_frac)
    return (ts <= cutoff).fillna(False), cutoff


def _proportional_allocate(
    counts: np.ndarray,
    target_n: int,
    min_each: int = 0,
    rng: Optional[np.random.Generator] = None,
) -> np.ndarray:
    n_groups = int(len(counts))
    if n_groups == 0:
        return np.zeros(0, dtype=int)
    if target_n <= 0:
        return np.zeros(n_groups, dtype=int)

    counts = counts.astype(float)
    total = float(counts.sum())
    if total <= 0:
        return np.zeros(n_groups, dtype=int)

    base = np.floor(counts / total * target_n).astype(int)
    if min_each > 0:
        min_vec = np.where(counts > 0, min_each, 0).astype(int)
        base = np.maximum(base, min_vec)

    alloc = base.copy()
    cur = int(alloc.sum())

    if cur < target_n:
        rem = target_n - cur
        frac = counts / total * target_n - np.floor(counts / total * target_n)
        order = np.argsort(-frac)
        for idx in order:
            if rem <= 0:
                break
            if counts[idx] <= alloc[idx]:
                continue
            alloc[idx] += 1
            rem -= 1
        if rem > 0:
            candidates = np.where(counts > alloc)[0]
            if len(candidates) > 0:
                if rng is None:
                    rng = np.random.default_rng(42)
                picks = rng.choice(candidates, size=rem, replace=True)
                for p in picks:
                    alloc[int(p)] += 1

    if alloc.sum() > target_n:
        over = int(alloc.sum() - target_n)
        floor_vec = np.where(counts > 0, min_each, 0).astype(int) if min_each > 0 else np.zeros(n_groups, dtype=int)
        order = np.argsort(-alloc)
        while over > 0:
            moved = False
            for idx in order:
                if over <= 0:
                    break
                if alloc[idx] > floor_vec[idx]:
                    alloc[idx] -= 1
                    over -= 1
                    moved = True
            if not moved:
                break

    alloc = np.minimum(alloc, counts.astype(int))
    return alloc.astype(int)


def _stratified_sample(
    df: pd.DataFrame,
    n_rows: int,
    stratify_cols: Sequence[str],
    seed: int,
) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    if n_rows >= len(df):
        return df.copy()
    if n_rows <= 0:
        return df.iloc[0:0].copy()

    rng = np.random.default_rng(seed)
    tmp = df.copy()
    tmp["__strata_key"] = _build_strata_key(tmp, stratify_cols)
    grouped = tmp.groupby("__strata_key", observed=True, sort=False)

    keys = list(grouped.groups.keys())
    counts = np.array([len(grouped.get_group(k)) for k in keys], dtype=int)

    min_each = 1 if n_rows >= len(keys) else 0
    alloc = _proportional_allocate(counts=counts, target_n=n_rows, min_each=min_each, rng=rng)

    picked_idx: List[int] = []
    for i, key in enumerate(keys):
        take = int(alloc[i])
        if take <= 0:
            continue
        sub_idx = grouped.get_group(key).index.to_numpy()
        take = min(take, len(sub_idx))
        chosen = rng.choice(sub_idx, size=take, replace=False)
        picked_idx.extend(chosen.tolist())

    picked = tmp.loc[picked_idx] if picked_idx else tmp.iloc[0:0]

    if len(picked) < n_rows:
        remain = tmp.drop(index=picked.index)
        need = n_rows - len(picked)
        if not remain.empty and need > 0:
            extra = remain.sample(n=min(need, len(remain)), random_state=seed)
            picked = pd.concat([picked, extra], axis=0)

    return picked.drop(columns=["__strata_key"], errors="ignore")


def _ensure_min_rows_per_class(
    sampled_df: pd.DataFrame,
    source_df: pd.DataFrame,
    label_col: str,
    min_rows_per_class: int,
    target_max_rows: int,
    seed: int,
) -> pd.DataFrame:
    if sampled_df.empty or source_df.empty or label_col not in source_df.columns:
        return sampled_df

    labels = [x for x in source_df[label_col].dropna().astype(str).unique().tolist() if x != ""]
    if not labels:
        return sampled_df

    effective_min = min(min_rows_per_class, max(1, target_max_rows // max(1, len(labels))))
    out = sampled_df.copy()

    for i, lbl in enumerate(labels):
        cur = int((out[label_col].astype(str) == lbl).sum())
        if cur >= effective_min:
            continue
        pool = source_df[source_df[label_col].astype(str) == lbl]
        if pool.empty:
            continue
        need = effective_min - cur
        extra = pool.sample(n=need, replace=len(pool) < need, random_state=seed + i + 17)
        out = pd.concat([out, extra], axis=0)

    if len(out) <= target_max_rows:
        return out

    # Keep class floors, then fill remaining capacity proportionally.
    keep_idx: List[int] = []
    rng = np.random.default_rng(seed + 99)
    out = out.copy()

    for i, lbl in enumerate(labels):
        sub = out[out[label_col].astype(str) == lbl]
        take = min(len(sub), effective_min)
        if take <= 0:
            continue
        chosen = sub.sample(n=take, random_state=seed + i + 101).index.tolist()
        keep_idx.extend(chosen)

    keep = out.loc[keep_idx].copy() if keep_idx else out.iloc[0:0].copy()
    remaining_budget = max(0, target_max_rows - len(keep))

    if remaining_budget > 0:
        rem = out.drop(index=keep.index)
        if not rem.empty:
            extra_idx = rng.choice(rem.index.to_numpy(), size=min(remaining_budget, len(rem)), replace=False)
            keep = pd.concat([keep, rem.loc[extra_idx]], axis=0)

    return keep


def _cluster_feature_cols(df: pd.DataFrame, max_cols: int = 40) -> List[str]:
    drop_prefixes = (
        "label_",
        "prob_",
        "seg_",
    )
    drop_exact = {
        "brand_id",
        "window_end_date",
        "window_size",
        "window_size_days",
        ROW_ID_COL,
    }

    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    candidate = []
    for c in numeric_cols:
        if c in drop_exact:
            continue
        if any(c.startswith(p) for p in drop_prefixes):
            continue
        candidate.append(c)

    if not candidate:
        return []

    var = df[candidate].var(numeric_only=True).fillna(0.0)
    ordered = var.sort_values(ascending=False).index.tolist()
    return ordered[:max_cols]


def _cluster_downsample(df: pd.DataFrame, target_rows: int, seed: int) -> pd.DataFrame:
    if df.empty or len(df) <= target_rows:
        return df.copy()

    cols = _cluster_feature_cols(df, max_cols=40)
    if not cols:
        return df.sample(n=target_rows, random_state=seed)

    X = df[cols].copy()
    X = X.fillna(X.median(numeric_only=True)).fillna(0.0)

    n_components = min(10, X.shape[1], max(2, X.shape[0] - 1))
    if n_components < 2:
        return df.sample(n=target_rows, random_state=seed)

    pca = PCA(n_components=n_components, random_state=seed)
    Z = pca.fit_transform(X)

    if len(df) >= 150_000:
        k = 100
    else:
        k = 50
    k = min(k, max(2, len(df) // 200))

    km = MiniBatchKMeans(n_clusters=k, random_state=seed, batch_size=4096, n_init="auto")
    cluster = km.fit_predict(Z)

    tmp = df.copy()
    tmp["__cluster"] = cluster
    grouped = tmp.groupby("__cluster", observed=True, sort=False)
    keys = list(grouped.groups.keys())
    counts = np.array([len(grouped.get_group(kc)) for kc in keys], dtype=int)

    min_each = min(50, max(1, target_rows // max(1, len(keys))))
    alloc = _proportional_allocate(counts=counts, target_n=target_rows, min_each=min_each, rng=np.random.default_rng(seed))

    parts = []
    for i, kc in enumerate(keys):
        sub = grouped.get_group(kc)
        take = int(min(alloc[i], len(sub)))
        if take <= 0:
            continue
        parts.append(sub.sample(n=take, random_state=seed + i + 13))

    if parts:
        out = pd.concat(parts, axis=0)
    else:
        out = tmp.iloc[0:0]

    if len(out) < target_rows:
        remain = tmp.drop(index=out.index)
        need = target_rows - len(out)
        if not remain.empty:
            out = pd.concat([out, remain.sample(n=min(need, len(remain)), random_state=seed + 31)], axis=0)

    out = out.drop(columns=["__cluster"], errors="ignore")
    return out


def _label_distribution(df: pd.DataFrame, label_col: str) -> Dict[str, float]:
    if df.empty or label_col not in df.columns:
        return {}
    s = df[label_col].astype(str)
    vc = s.value_counts(normalize=True)
    return {str(k): float(v) for k, v in vc.items()}


def _brand_counts(df: pd.DataFrame, brand_col: str) -> Dict[str, int]:
    if df.empty or brand_col not in df.columns:
        return {}
    vc = df[brand_col].astype(str).value_counts()
    return {str(k): int(v) for k, v in vc.items()}


def _metric_stats(df: pd.DataFrame, cols: Sequence[str]) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}
    if df.empty:
        return out
    for c in cols:
        if c not in df.columns:
            continue
        s = pd.to_numeric(df[c], errors="coerce")
        out[c] = {
            "mean": float(s.mean()) if s.notna().any() else float("nan"),
            "std": float(s.std(ddof=0)) if s.notna().any() else float("nan"),
        }
    return out


def _metric_mean_drift_std_units(
    before_stats: Mapping[str, Mapping[str, float]],
    after_stats: Mapping[str, Mapping[str, float]],
) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for col in before_stats.keys():
        b = before_stats[col]
        a = after_stats.get(col, {})
        b_mean = float(b.get("mean", np.nan))
        b_std = float(b.get("std", np.nan))
        a_mean = float(a.get("mean", np.nan))
        if np.isnan(b_mean) or np.isnan(a_mean):
            out[col] = float("nan")
            continue
        denom = b_std if (not np.isnan(b_std) and b_std > 1e-9) else np.nan
        if np.isnan(denom):
            out[col] = 0.0 if abs(a_mean - b_mean) < 1e-9 else float("inf")
        else:
            out[col] = float((a_mean - b_mean) / denom)
    return out


def _distribution_drift(before: Mapping[str, float], after: Mapping[str, float]) -> Dict[str, float]:
    keys = sorted(set(before.keys()).union(after.keys()))
    return {k: float(after.get(k, 0.0) - before.get(k, 0.0)) for k in keys}


def _core_metric_cols(df: pd.DataFrame) -> List[str]:
    preferred = [
        "active_users",
        "gmv_net",
        "transaction_count",
        "activity_completion_rate",
        "activity_redeem_rate",
        "reward_efficiency",
        "dormant_share",
        "new_user_share",
        "repeat_purchase_rate",
        "sku_top_share",
    ]
    cols = [c for c in preferred if c in df.columns]
    if len(cols) >= 5:
        return cols[:10]

    numeric = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    blacklist = {"label_health_class_int", "window_size_days", ROW_ID_COL}
    numeric = [c for c in numeric if c not in blacklist and not c.startswith("prob_")]
    if cols:
        numeric = [c for c in numeric if c not in cols]
    cols.extend(numeric[: max(0, 10 - len(cols))])
    return cols[:10]


def _time_coverage(df: pd.DataFrame, time_col: str = TIME_COL) -> Dict[str, Optional[str]]:
    if df.empty or time_col not in df.columns:
        return {"min": None, "max": None}
    ts = _safe_to_datetime(df[time_col])
    return {
        "min": None if not ts.notna().any() else str(ts.min()),
        "max": None if not ts.notna().any() else str(ts.max()),
    }


def _qa_section(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    label_col: str,
    brand_col: str,
    core_cols: Sequence[str],
    label_tol: float,
    metric_tol: float,
) -> Dict:
    before_label = _label_distribution(before_df, label_col)
    after_label = _label_distribution(after_df, label_col)
    label_drift = _distribution_drift(before_label, after_label)
    label_drift_max = float(max([abs(v) for v in label_drift.values()], default=0.0))

    before_stats = _metric_stats(before_df, core_cols)
    after_stats = _metric_stats(after_df, core_cols)
    metric_drift = _metric_mean_drift_std_units(before_stats, after_stats)
    finite_metric = [abs(v) for v in metric_drift.values() if np.isfinite(v)]
    metric_drift_max = float(max(finite_metric, default=0.0))

    label_pass = label_drift_max <= label_tol
    metric_pass = metric_drift_max <= metric_tol

    return {
        "rows_before": int(len(before_df)),
        "rows_after": int(len(after_df)),
        "brand_counts_before": _brand_counts(before_df, brand_col),
        "brand_counts_after": _brand_counts(after_df, brand_col),
        "label_distribution_before": before_label,
        "label_distribution_after": after_label,
        "label_distribution_drift": label_drift,
        "metric_stats_before": before_stats,
        "metric_stats_after": after_stats,
        "metric_mean_drift_std_units": metric_drift,
        "time_coverage_before": _time_coverage(before_df),
        "time_coverage_after": _time_coverage(after_df),
        "checks": {
            "label_share_drift_max_abs": label_drift_max,
            "metric_mean_drift_max_abs_std_units": metric_drift_max,
            "label_share_pass": bool(label_pass),
            "metric_mean_pass": bool(metric_pass),
            "pass": bool(label_pass and metric_pass),
        },
    }


def _quick_sample(
    train_df: pd.DataFrame,
    eval_df: pd.DataFrame,
    config: TrainSampleConfig,
    stratify_cols: Sequence[str],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    train_target = int(min(config.max_train_rows, max(1, int(len(train_df) * config.frac))))
    eval_target = int(min(config.max_eval_rows, max(1, int(len(eval_df) * config.frac))))

    if len(eval_df) >= 5000:
        eval_target = max(eval_target, min(5000, config.max_eval_rows))

    train_sample = _stratified_sample(train_df, n_rows=train_target, stratify_cols=stratify_cols, seed=config.seed)
    train_sample = _ensure_min_rows_per_class(
        sampled_df=train_sample,
        source_df=train_df,
        label_col=LABEL_COL,
        min_rows_per_class=config.min_rows_per_class,
        target_max_rows=config.max_train_rows,
        seed=config.seed,
    )
    if len(train_sample) > config.max_train_rows:
        train_sample = _stratified_sample(train_sample, n_rows=config.max_train_rows, stratify_cols=stratify_cols, seed=config.seed + 7)

    eval_sample = _stratified_sample(eval_df, n_rows=eval_target, stratify_cols=stratify_cols, seed=config.seed + 11)
    if len(eval_sample) > config.max_eval_rows:
        eval_sample = _stratified_sample(eval_sample, n_rows=config.max_eval_rows, stratify_cols=stratify_cols, seed=config.seed + 17)

    return train_sample, eval_sample


def _smart_sample(
    train_df: pd.DataFrame,
    eval_df: pd.DataFrame,
    config: TrainSampleConfig,
    stratify_cols: Sequence[str],
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
    stage_info: Dict[str, int | str] = {}

    train_work = train_df.copy()
    stage_info["stage_a_train_rows_before"] = int(len(train_work))

    if config.recent_days and config.recent_days > 0 and TIME_COL in train_work.columns:
        ts = _safe_to_datetime(train_work[TIME_COL])
        max_ts = ts.max()
        if pd.notna(max_ts):
            min_ts = max_ts - pd.Timedelta(days=int(config.recent_days))
            keep = ts >= min_ts
            recent = train_work.loc[keep.fillna(False)].copy()
            if not recent.empty:
                train_work = recent
    stage_info["stage_a_train_rows_after_recent_filter"] = int(len(train_work))

    if len(train_work) > config.max_train_rows:
        stage_b = _cluster_downsample(train_work, target_rows=config.max_train_rows, seed=config.seed)
    else:
        stage_b = train_work
    stage_info["stage_b_train_rows_after_cluster"] = int(len(stage_b))

    train_sample = _stratified_sample(
        stage_b,
        n_rows=min(config.max_train_rows, len(stage_b)),
        stratify_cols=stratify_cols,
        seed=config.seed + 23,
    )
    train_sample = _ensure_min_rows_per_class(
        sampled_df=train_sample,
        source_df=stage_b,
        label_col=LABEL_COL,
        min_rows_per_class=config.min_rows_per_class,
        target_max_rows=config.max_train_rows,
        seed=config.seed + 29,
    )
    if len(train_sample) > config.max_train_rows:
        train_sample = _stratified_sample(train_sample, n_rows=config.max_train_rows, stratify_cols=stratify_cols, seed=config.seed + 31)

    eval_target = min(config.max_eval_rows, len(eval_df))
    eval_sample = _stratified_sample(eval_df, n_rows=eval_target, stratify_cols=stratify_cols, seed=config.seed + 41)

    return train_sample, eval_sample, stage_info


def _build_sample(
    labeled_df: pd.DataFrame,
    config: TrainSampleConfig,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
    df = labeled_df.copy()
    df = df.dropna(subset=[LABEL_COL, "window_end_date", "brand_id", "window_size"]).copy()

    if ROW_ID_COL not in df.columns:
        df[ROW_ID_COL] = np.arange(len(df), dtype=int)

    train_mask, cutoff = _time_split_mask(df, time_col=TIME_COL, train_frac=0.8)
    train_df = df.loc[train_mask].copy()
    eval_df = df.loc[~train_mask].copy()

    if eval_df.empty and len(df) > 1:
        split_idx = int(len(df) * 0.8)
        train_df = df.iloc[:split_idx].copy()
        eval_df = df.iloc[split_idx:].copy()

    strat_cols = _resolve_stratify_cols(df, config.stratify_cols)

    stage_meta: Dict = {
        "time_split_cutoff": str(cutoff),
        "train_rows_before_sampling": int(len(train_df)),
        "eval_rows_before_sampling": int(len(eval_df)),
    }

    if config.mode == "quick":
        tr, ev = _quick_sample(train_df, eval_df, config=config, stratify_cols=strat_cols)
    elif config.mode == "smart":
        tr, ev, extra = _smart_sample(train_df, eval_df, config=config, stratify_cols=strat_cols)
        stage_meta.update(extra)
    else:
        tr, ev = train_df.copy(), eval_df.copy()

    tr = tr.drop_duplicates(subset=[ROW_ID_COL]).copy()
    ev = ev.drop_duplicates(subset=[ROW_ID_COL]).copy()

    # Ensure train/eval disjoint by row_id.
    overlap_ids = set(tr[ROW_ID_COL].astype(int).tolist()).intersection(set(ev[ROW_ID_COL].astype(int).tolist()))
    if overlap_ids:
        ev = ev[~ev[ROW_ID_COL].astype(int).isin(overlap_ids)].copy()

    return tr, ev, stage_meta


def _build_qa_report(
    source_train_df: pd.DataFrame,
    source_eval_df: pd.DataFrame,
    sampled_train_df: pd.DataFrame,
    sampled_eval_df: pd.DataFrame,
    config: TrainSampleConfig,
    stage_meta: Mapping,
) -> Dict:
    core_cols = _core_metric_cols(pd.concat([source_train_df, source_eval_df], axis=0))

    train_section = _qa_section(
        before_df=source_train_df,
        after_df=sampled_train_df,
        label_col=LABEL_COL,
        brand_col=config.group_col if config.group_col in source_train_df.columns else "brand_id",
        core_cols=core_cols,
        label_tol=config.qa_label_share_drift_tol,
        metric_tol=config.qa_metric_drift_std_tol,
    )
    eval_section = _qa_section(
        before_df=source_eval_df,
        after_df=sampled_eval_df,
        label_col=LABEL_COL,
        brand_col=config.group_col if config.group_col in source_eval_df.columns else "brand_id",
        core_cols=core_cols,
        label_tol=config.qa_label_share_drift_tol,
        metric_tol=config.qa_metric_drift_std_tol,
    )

    overall_pass = bool(train_section["checks"]["pass"] and eval_section["checks"]["pass"])

    return {
        "config": asdict(config),
        "stage_meta": dict(stage_meta),
        "core_metrics_used": core_cols,
        "train": train_section,
        "eval": eval_section,
        "representative_pass": overall_pass,
    }


def _relaxed_config(config: TrainSampleConfig) -> TrainSampleConfig:
    if config.mode == "quick":
        return TrainSampleConfig(
            mode=config.mode,
            seed=config.seed,
            frac=min(1.0, config.frac * 1.5),
            max_train_rows=int(config.max_train_rows * 1.5),
            max_eval_rows=int(config.max_eval_rows * 1.5),
            recent_days=config.recent_days,
            stratify_cols=config.stratify_cols,
            group_col=config.group_col,
            min_rows_per_class=config.min_rows_per_class,
            qa_label_share_drift_tol=config.qa_label_share_drift_tol,
            qa_metric_drift_std_tol=config.qa_metric_drift_std_tol,
        )

    return TrainSampleConfig(
        mode=config.mode,
        seed=config.seed,
        frac=config.frac,
        max_train_rows=int(config.max_train_rows * 1.5),
        max_eval_rows=int(config.max_eval_rows * 1.5),
        recent_days=int(config.recent_days * 1.5) if config.recent_days else config.recent_days,
        stratify_cols=config.stratify_cols,
        group_col=config.group_col,
        min_rows_per_class=config.min_rows_per_class,
        qa_label_share_drift_tol=config.qa_label_share_drift_tol,
        qa_metric_drift_std_tol=config.qa_metric_drift_std_tol,
    )


def build_train_eval_samples(
    labeled_df: pd.DataFrame,
    config: TrainSampleConfig,
) -> SampleResult:
    if config.mode == "off":
        sampled = labeled_df.copy()
        if ROW_ID_COL not in sampled.columns:
            sampled[ROW_ID_COL] = np.arange(len(sampled), dtype=int)
        mask, cutoff = _time_split_mask(sampled, TIME_COL, train_frac=0.8)
        train_df = sampled.loc[mask].copy()
        eval_df = sampled.loc[~mask].copy()
        qa_report = {
            "config": asdict(config),
            "stage_meta": {"time_split_cutoff": str(cutoff)},
            "representative_pass": True,
            "note": "sampling_off",
        }
        return SampleResult(
            sampled_df=sampled,
            sampled_train_df=train_df,
            sampled_eval_df=eval_df,
            train_row_ids=train_df[ROW_ID_COL].astype(int).tolist() if ROW_ID_COL in train_df.columns else [],
            eval_row_ids=eval_df[ROW_ID_COL].astype(int).tolist() if ROW_ID_COL in eval_df.columns else [],
            qa_report=qa_report,
            config_used=asdict(config),
            fallback_applied=False,
        )

    tr, ev, stage_meta = _build_sample(labeled_df, config=config)
    src_df = labeled_df.copy()
    if ROW_ID_COL not in src_df.columns:
        src_df[ROW_ID_COL] = np.arange(len(src_df), dtype=int)
    src_df = src_df.dropna(subset=[LABEL_COL, "window_end_date", "brand_id", "window_size"]).copy()
    src_mask, _ = _time_split_mask(src_df, TIME_COL, train_frac=0.8)
    src_tr = src_df.loc[src_mask].copy()
    src_ev = src_df.loc[~src_mask].copy()

    qa = _build_qa_report(
        source_train_df=src_tr,
        source_eval_df=src_ev,
        sampled_train_df=tr,
        sampled_eval_df=ev,
        config=config,
        stage_meta=stage_meta,
    )

    fallback_applied = False
    used_cfg = config

    if not qa.get("representative_pass", True):
        relaxed = _relaxed_config(config)
        tr2, ev2, stage_meta2 = _build_sample(labeled_df, config=relaxed)
        qa2 = _build_qa_report(
            source_train_df=src_tr,
            source_eval_df=src_ev,
            sampled_train_df=tr2,
            sampled_eval_df=ev2,
            config=relaxed,
            stage_meta=stage_meta2,
        )
        if qa2.get("representative_pass", False) or len(tr2) >= len(tr):
            tr, ev = tr2, ev2
            qa = qa2
            used_cfg = relaxed
            fallback_applied = True

    sampled_df = pd.concat([tr, ev], axis=0).drop_duplicates(subset=[ROW_ID_COL]).copy()
    sampled_df = sampled_df.sort_values(TIME_COL).reset_index(drop=True)

    return SampleResult(
        sampled_df=sampled_df,
        sampled_train_df=tr.reset_index(drop=True),
        sampled_eval_df=ev.reset_index(drop=True),
        train_row_ids=tr[ROW_ID_COL].astype(int).tolist() if ROW_ID_COL in tr.columns else [],
        eval_row_ids=ev[ROW_ID_COL].astype(int).tolist() if ROW_ID_COL in ev.columns else [],
        qa_report=qa,
        config_used=asdict(used_cfg),
        fallback_applied=fallback_applied,
    )


def save_sample_outputs(result: SampleResult, output_dir: str | Path) -> None:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    cols = [
        ROW_ID_COL,
        "brand_id",
        "window_end_date",
        "window_size",
        LABEL_COL,
    ]

    train_cols = [c for c in cols if c in result.sampled_train_df.columns]
    eval_cols = [c for c in cols if c in result.sampled_eval_df.columns]

    result.sampled_train_df[train_cols].to_csv(out_dir / "sample_train_indices.csv", index=False)
    result.sampled_eval_df[eval_cols].to_csv(out_dir / "sample_eval_indices.csv", index=False)

    payload = {
        "config_used": result.config_used,
        "fallback_applied": result.fallback_applied,
        "train_rows": int(len(result.sampled_train_df)),
        "eval_rows": int(len(result.sampled_eval_df)),
        "sampled_total_rows": int(len(result.sampled_df)),
        "qa_report": result.qa_report,
    }
    (out_dir / "sample_qa_report.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")

