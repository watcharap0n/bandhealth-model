from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Mapping, Optional


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from src.mlops_runtime import read_json_manifest, utc_now_iso  # noqa: E402


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Promote a candidate Brand Health model manifest.")
    parser.add_argument("--candidate-manifest", type=str, required=True)
    parser.add_argument("--production-manifest-out", type=str, required=True)
    parser.add_argument("--current-production-manifest", type=str, default=None)
    parser.add_argument("--min-macro-f1", type=float, default=0.55)
    parser.add_argument("--min-balanced-accuracy", type=float, default=0.55)
    parser.add_argument("--require-better-than-production", type=str, default="true")
    parser.add_argument("--output-json", type=str, required=True)
    return parser


def _parse_bool_flag(raw: object) -> bool:
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _selected_model_metrics(manifest: Mapping[str, Any]) -> Dict[str, Optional[float]]:
    metrics = manifest.get("metrics", {})
    if not isinstance(metrics, Mapping):
        return {"macro_f1": None, "balanced_accuracy": None}
    selected_model = str(manifest.get("selected_model") or metrics.get("selected_model") or "").strip()
    time_split = metrics.get("time_split", {})
    if isinstance(time_split, Mapping) and selected_model and isinstance(time_split.get(selected_model), Mapping):
        selected = time_split.get(selected_model, {})
        return {
            "macro_f1": _safe_float(selected.get("macro_f1")),
            "balanced_accuracy": _safe_float(selected.get("balanced_accuracy")),
        }
    return {
        "macro_f1": None,
        "balanced_accuracy": None,
    }


def _safe_float(value: object) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def main() -> None:
    args = _build_parser().parse_args()

    candidate = read_json_manifest(args.candidate_manifest)
    candidate_metrics = _selected_model_metrics(candidate)

    errors = []
    passes = True

    macro_f1 = candidate_metrics.get("macro_f1")
    bal_acc = candidate_metrics.get("balanced_accuracy")
    if macro_f1 is None or macro_f1 < float(args.min_macro_f1):
        passes = False
        errors.append(f"macro_f1 below threshold: {macro_f1} < {float(args.min_macro_f1)}")
    if bal_acc is None or bal_acc < float(args.min_balanced_accuracy):
        passes = False
        errors.append(f"balanced_accuracy below threshold: {bal_acc} < {float(args.min_balanced_accuracy)}")

    current_prod_metrics = None
    if args.current_production_manifest:
        current_path = Path(args.current_production_manifest)
        if current_path.exists():
            current_prod = read_json_manifest(current_path)
            current_prod_metrics = _selected_model_metrics(current_prod)
            if _parse_bool_flag(args.require_better_than_production):
                current_macro = current_prod_metrics.get("macro_f1")
                if current_macro is not None and macro_f1 is not None and macro_f1 < current_macro:
                    passes = False
                    errors.append(f"macro_f1 below current production: {macro_f1} < {current_macro}")

    approved_manifest = dict(candidate)
    approved_manifest["status"] = "approved" if passes else "rejected"
    approved_manifest["approved_at"] = utc_now_iso() if passes else None

    if passes:
        output_manifest_path = Path(args.production_manifest_out)
        output_manifest_path.parent.mkdir(parents=True, exist_ok=True)
        output_manifest_path.write_text(
            json.dumps(approved_manifest, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )

    payload = {
        "evaluated_at": utc_now_iso(),
        "candidate_manifest": str(args.candidate_manifest),
        "status": "approved" if passes else "rejected",
        "candidate_metrics": candidate_metrics,
        "current_production_metrics": current_prod_metrics,
        "errors": errors,
        "production_manifest_out": str(args.production_manifest_out),
    }
    output_json = Path(args.output_json)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    if not passes:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
