from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

import joblib
import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV, calibration_curve
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    balanced_accuracy_score,
    brier_score_loss,
    confusion_matrix,
    f1_score,
)
from sklearn.model_selection import GroupKFold
from sklearn.pipeline import Pipeline
from sklearn.feature_selection import mutual_info_classif
from sklearn.preprocessing import OneHotEncoder

LABEL_COL = "label_health_class"


@dataclass
class TrainArtifacts:
    final_model: Pipeline | CalibratedClassifierCV
    feature_columns: List[str]
    categorical_columns: List[str]
    numeric_columns: List[str]
    class_labels: List[str]
    metrics: Dict
    feature_importance: Dict[str, float]



def _build_preprocessor(categorical_cols: List[str], numeric_cols: List[str]) -> ColumnTransformer:
    return ColumnTransformer(
        transformers=[
            (
                "categorical",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
                    ]
                ),
                categorical_cols,
            ),
            (
                "numeric",
                Pipeline(steps=[("imputer", SimpleImputer(strategy="median"))]),
                numeric_cols,
            ),
        ],
        remainder="drop",
    )



def _model_pipelines(
    categorical_cols: List[str],
    numeric_cols: List[str],
    weight_classes: bool = True,
    sample_mode: str = "off",
) -> Dict[str, Pipeline]:
    preprocessor = _build_preprocessor(categorical_cols, numeric_cols)
    class_weight = "balanced" if weight_classes else None

    logistic = Pipeline(
        steps=[
            ("preprocess", preprocessor),
            (
                "model",
                LogisticRegression(
                    max_iter=2500,
                    class_weight=class_weight,
                    random_state=42,
                ),
            ),
        ]
    )

    hgb_max_iter = 220 if sample_mode == "quick" else 400
    hgb = Pipeline(
        steps=[
            ("preprocess", preprocessor),
            (
                "model",
                HistGradientBoostingClassifier(
                    max_iter=hgb_max_iter,
                    learning_rate=0.05,
                    max_depth=8,
                    min_samples_leaf=20,
                    l2_regularization=0.1,
                    class_weight=class_weight,
                    early_stopping=True,
                    validation_fraction=0.1,
                    n_iter_no_change=20,
                    random_state=42,
                ),
            ),
        ]
    )

    if sample_mode == "quick":
        return {"hgb": hgb}
    return {"logistic": logistic, "hgb": hgb}



def _calibration_summary(y_true: pd.Series, proba: np.ndarray, class_labels: List[str]) -> Dict[str, float | List[float]]:
    if "AtRisk" in class_labels:
        risk_idx = class_labels.index("AtRisk")
    else:
        risk_idx = 0

    y_bin = (y_true.values == class_labels[risk_idx]).astype(int)
    p = proba[:, risk_idx]

    if len(np.unique(y_bin)) < 2:
        return {
            "risk_class": class_labels[risk_idx],
            "brier": float("nan"),
            "calibration_ece": float("nan"),
            "bin_pred": [],
            "bin_true": [],
        }

    prob_true, prob_pred = calibration_curve(y_bin, p, n_bins=8, strategy="uniform")
    ece = float(np.mean(np.abs(prob_true - prob_pred))) if len(prob_true) else float("nan")
    brier = float(brier_score_loss(y_bin, p))

    return {
        "risk_class": class_labels[risk_idx],
        "brier": brier,
        "calibration_ece": ece,
        "bin_pred": [float(x) for x in prob_pred],
        "bin_true": [float(x) for x in prob_true],
    }



def _evaluate_model(model, X_test: pd.DataFrame, y_test: pd.Series, class_labels: List[str]) -> Dict:
    pred = model.predict(X_test)
    proba = model.predict_proba(X_test)

    macro_f1 = float(f1_score(y_test, pred, average="macro"))
    weighted_f1 = float(f1_score(y_test, pred, average="weighted"))
    bal_acc = float(balanced_accuracy_score(y_test, pred))
    cm = confusion_matrix(y_test, pred, labels=class_labels)

    calib = _calibration_summary(y_test, proba, class_labels)

    return {
        "macro_f1": macro_f1,
        "weighted_f1": weighted_f1,
        "balanced_accuracy": bal_acc,
        "confusion_matrix": cm.tolist(),
        "class_labels": class_labels,
        "calibration": calib,
    }


def _set_thread_limits(n_jobs: int) -> None:
    n = max(1, int(n_jobs))
    for key in [
        "OMP_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "MKL_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
        "VECLIB_MAXIMUM_THREADS",
    ]:
        os.environ[key] = str(n)


def _select_top_numeric_features(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    numeric_cols: Sequence[str],
    top_k: int,
) -> List[str]:
    if top_k <= 0:
        return list(numeric_cols)
    if not numeric_cols:
        return []

    cols = [c for c in numeric_cols if c in X_train.columns]
    if not cols:
        return []
    if len(cols) <= top_k:
        return cols

    Xn = X_train[cols].copy()
    Xn = Xn.fillna(Xn.median(numeric_only=True)).fillna(0.0)
    y_codes = pd.factorize(y_train.astype(str))[0]

    if len(np.unique(y_codes)) < 2:
        var = Xn.var(numeric_only=True).fillna(0.0)
        return var.sort_values(ascending=False).head(top_k).index.tolist()

    try:
        mi = mutual_info_classif(Xn, y_codes, discrete_features=False, random_state=42)
        s = pd.Series(mi, index=cols).fillna(0.0)
        return s.sort_values(ascending=False).head(top_k).index.tolist()
    except Exception:
        var = Xn.var(numeric_only=True).fillna(0.0)
        return var.sort_values(ascending=False).head(top_k).index.tolist()



def _prepare_training_frame(
    feature_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.Series, List[str], List[str], List[str], pd.Series, pd.Series]:
    df = feature_df.copy()
    if "__row_id" not in df.columns:
        df["__row_id"] = np.arange(len(df), dtype=int)
    df = df.dropna(subset=[LABEL_COL, "window_end_date", "brand_id", "window_size"])
    df = df.sort_values("window_end_date").reset_index(drop=True)

    # Keep date-derived seasonality signals while preventing timestamp object leakage.
    dt = pd.to_datetime(df["window_end_date"], errors="coerce", utc=True)
    df["window_end_ordinal"] = dt.dt.date.map(lambda d: d.toordinal() if pd.notna(d) else 0)
    df["window_end_month"] = dt.dt.month.fillna(0).astype(float)
    df["window_end_week"] = dt.dt.isocalendar().week.astype(float)

    exclude = {
        LABEL_COL,
        "label_health_score",
        "label_health_class_int",
        "window_end_date",
    }
    feature_cols = [c for c in df.columns if c not in exclude]

    X = df[feature_cols].copy()
    y = df[LABEL_COL].astype(str)

    categorical_cols = [c for c in ["brand_id", "window_size"] if c in X.columns]
    numeric_cols = [c for c in X.columns if c not in categorical_cols]

    class_labels = sorted(y.unique().tolist())
    row_ids = df["__row_id"].astype(int)
    time_series = pd.to_datetime(df["window_end_date"], errors="coerce", utc=True)
    return X, y, feature_cols, categorical_cols, numeric_cols, row_ids, time_series



def _time_split_mask(df: pd.DataFrame, time_col: str = "window_end_date", train_frac: float = 0.8) -> Tuple[pd.Series, pd.Timestamp]:
    ts = pd.to_datetime(df[time_col], utc=True)
    cutoff = ts.quantile(train_frac)
    train_mask = ts <= cutoff
    return train_mask, cutoff



def train_models(
    feature_df: pd.DataFrame,
    artifact_dir: str | Path,
    train_row_ids: Optional[Sequence[int]] = None,
    eval_row_ids: Optional[Sequence[int]] = None,
    sample_mode: str = "off",
    n_jobs: int = 4,
    weight_classes: bool = True,
    group_col: str = "brand_id",
    quick_top_k_features: Optional[int] = None,
) -> TrainArtifacts:
    sample_mode = str(sample_mode).strip().lower()
    _set_thread_limits(n_jobs)

    artifact_path = Path(artifact_dir)
    artifact_path.mkdir(parents=True, exist_ok=True)

    X, y, feature_cols, cat_cols, num_cols, row_ids, time_series = _prepare_training_frame(feature_df)

    cutoff = pd.NaT
    if train_row_ids is not None or eval_row_ids is not None:
        train_set = set(int(x) for x in (train_row_ids or []))
        eval_set = set(int(x) for x in (eval_row_ids or []))

        if train_set:
            train_mask = row_ids.isin(train_set)
        else:
            train_mask = ~row_ids.isin(eval_set)

        if eval_set:
            test_mask = row_ids.isin(eval_set)
        else:
            test_mask = ~train_mask
        test_mask = test_mask & (~train_mask)

        if int(test_mask.sum()) == 0:
            train_mask, cutoff = _time_split_mask(pd.DataFrame({"window_end_date": time_series}), time_col="window_end_date", train_frac=0.8)
            test_mask = ~train_mask
    else:
        train_mask, cutoff = _time_split_mask(pd.DataFrame({"window_end_date": time_series}), time_col="window_end_date", train_frac=0.8)
        test_mask = ~train_mask

    X_train, X_test = X.loc[train_mask], X.loc[test_mask]
    y_train, y_test = y.loc[train_mask], y.loc[test_mask]

    if len(X_test) == 0:
        split_idx = int(len(X) * 0.8)
        X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
        y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
        train_mask = pd.Series(False, index=X.index)
        train_mask.iloc[:split_idx] = True
        cutoff = time_series.iloc[split_idx - 1] if split_idx > 0 else pd.NaT

    if sample_mode == "quick":
        k_total = int(quick_top_k_features or 80)
        k_numeric = max(10, k_total - len(cat_cols))
        top_numeric = _select_top_numeric_features(X_train, y_train, num_cols, top_k=k_numeric)
        feature_cols = cat_cols + top_numeric
        num_cols = top_numeric
        X = X[feature_cols]
        X_train = X_train[feature_cols]
        X_test = X_test[feature_cols]

    class_labels = sorted(y.unique().tolist())
    models = _model_pipelines(cat_cols, num_cols, weight_classes=weight_classes, sample_mode=sample_mode)

    time_results = {}

    for name, model in models.items():
        model.fit(X_train, y_train)
        time_results[name] = _evaluate_model(model, X_test, y_test, class_labels)

    # Calibrated strong model on time split.
    cal_cv = 2 if sample_mode == "quick" else 3
    calibrated = CalibratedClassifierCV(
        estimator=_model_pipelines(cat_cols, num_cols, weight_classes=weight_classes, sample_mode=sample_mode)["hgb"],
        method="sigmoid",
        cv=cal_cv,
    )
    calibrated.fit(X_train, y_train)
    time_results["hgb_calibrated"] = _evaluate_model(calibrated, X_test, y_test, class_labels)

    # Cross-brand holdout.
    cross_brand_results = {}
    brands = sorted(X[group_col].astype(str).unique().tolist()) if group_col in X.columns else []
    for holdout in brands:
        mask = X[group_col].astype(str) == holdout
        X_tr, y_tr = X.loc[~mask], y.loc[~mask]
        X_te, y_te = X.loc[mask], y.loc[mask]

        if len(X_tr) == 0 or len(X_te) == 0 or y_tr.nunique() < 2:
            cross_brand_results[holdout] = {"status": "skipped", "reason": "insufficient class diversity"}
            continue

        hgb_model = _model_pipelines(cat_cols, num_cols, weight_classes=weight_classes, sample_mode=sample_mode)["hgb"]
        hgb_model.fit(X_tr, y_tr)
        cross_brand_results[holdout] = _evaluate_model(hgb_model, X_te, y_te, class_labels)

    # GroupKFold by brand (if 2+ brands).
    group_results = []
    if group_col in X.columns and len(brands) >= 2:
        groups = X[group_col].astype(str)
        gkf = GroupKFold(n_splits=min(2, len(brands)))
        for fold_id, (tr_idx, te_idx) in enumerate(gkf.split(X, y, groups=groups), start=1):
            X_tr, y_tr = X.iloc[tr_idx], y.iloc[tr_idx]
            X_te, y_te = X.iloc[te_idx], y.iloc[te_idx]
            if y_tr.nunique() < 2:
                continue
            m = _model_pipelines(cat_cols, num_cols, weight_classes=weight_classes, sample_mode=sample_mode)["hgb"]
            m.fit(X_tr, y_tr)
            fold_metrics = _evaluate_model(m, X_te, y_te, class_labels)
            fold_metrics["fold"] = fold_id
            group_results.append(fold_metrics)

    # Model selection by time-based macro F1.
    best_name = max(time_results.keys(), key=lambda k: time_results[k]["macro_f1"])
    best_is_calibrated = best_name == "hgb_calibrated"

    # Fit final selected model on full data.
    if best_is_calibrated:
        final_model = CalibratedClassifierCV(
            estimator=_model_pipelines(cat_cols, num_cols, weight_classes=weight_classes, sample_mode=sample_mode)["hgb"],
            method="sigmoid",
            cv=cal_cv,
        )
    else:
        final_model = _model_pipelines(cat_cols, num_cols, weight_classes=weight_classes, sample_mode=sample_mode)[best_name]
    final_model.fit(X, y)

    # Permutation importance on holdout (fallback to full sample if needed).
    feature_importance: Dict[str, float] = {}
    try:
        base_model = calibrated if best_is_calibrated else models[best_name]
        perm_X = X_test if len(X_test) > 0 else X
        perm_y = y_test if len(y_test) > 0 else y
        perm = permutation_importance(
            base_model,
            perm_X,
            perm_y,
            scoring="f1_macro",
            n_repeats=1,
            random_state=42,
            n_jobs=1,
        )
        feature_importance = {col: float(val) for col, val in zip(feature_cols, perm.importances_mean)}
    except Exception:
        feature_importance = {col: 0.0 for col in feature_cols}

    metrics = {
        "time_split_cutoff": str(cutoff),
        "time_split": time_results,
        "cross_brand": cross_brand_results,
        "group_kfold_brand": group_results,
        "selected_model": best_name,
        "train_mode": sample_mode,
        "n_jobs": int(n_jobs),
        "weight_classes": bool(weight_classes),
        "train_rows": int(len(X_train)),
        "eval_rows": int(len(X_test)),
        "feature_count": int(len(feature_cols)),
    }

    # Save artifacts.
    joblib.dump(final_model, artifact_path / "brand_health_model.joblib")
    with (artifact_path / "feature_importance.json").open("w", encoding="utf-8") as f:
        json.dump(feature_importance, f, indent=2)

    metadata = {
        "feature_columns": feature_cols,
        "categorical_columns": cat_cols,
        "numeric_columns": num_cols,
        "class_labels": class_labels,
        "metrics": metrics,
    }
    with (artifact_path / "model_metadata.json").open("w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    return TrainArtifacts(
        final_model=final_model,
        feature_columns=feature_cols,
        categorical_columns=cat_cols,
        numeric_columns=num_cols,
        class_labels=class_labels,
        metrics=metrics,
        feature_importance=feature_importance,
    )
