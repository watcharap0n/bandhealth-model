from __future__ import annotations

import re
from typing import Dict, List, Mapping, Optional, Sequence

from .driver_mapping import canonical_driver_key, infer_metric_family_from_key

ACTION_MAP: Dict[str, List[str]] = {
    "active_users": [
        "Launch dormant-user reactivation campaigns with segmented incentives.",
        "Reduce message fatigue via tighter frequency caps and send-time optimization.",
        "Retarget recently lapsed cohorts with low-friction missions.",
    ],
    "activity_completion_rate": [
        "Audit mission completion friction (steps, UX, rules) and simplify top drop-off flows.",
        "Shift reward mix toward lower-friction, faster-win activities.",
        "A/B test mission copy and CTA clarity on the highest-volume activities.",
    ],
    "redeem_rate": [
        "Increase redemption visibility with in-app reminders and redemption-specific triggers.",
        "Rebalance point thresholds to reduce perceived redemption effort.",
        "Promote expiring-value nudges to accelerate redemption intent.",
    ],
    "gmv_net": [
        "Run short-cycle conversion pushes for high-intent audiences.",
        "Bundle offers to lift average order value and repeat basket behavior.",
        "Check checkout/payment friction and recover abandoned purchase attempts.",
    ],
    "transaction_count": [
        "Introduce repeat-purchase triggers based on product replenishment cadence.",
        "Activate personalized offer ladders for near-churn buyers.",
        "Prioritize retention promotions for buyers with recent order decline.",
    ],
    "dormant_share": [
        "Create dormant-tier win-back journeys with escalating incentives.",
        "Use triggered education content to reintroduce loyalty value.",
        "Suppress low-intent users from broad blasts and target by propensity.",
    ],
    "reward_efficiency": [
        "Recalibrate reward economics: trim low-yield rewards and raise completion-linked value.",
        "Prioritize activities with best completion-per-point efficiency.",
        "Set guardrails so point inflation does not outpace engagement conversion.",
    ],
    "sku_concentration": [
        "Diversify reward/product mix to reduce dependence on top SKU(s).",
        "Promote under-indexed SKUs through personalized recommendations.",
        "Use rotation strategy for featured SKUs to broaden basket composition.",
    ],
}


COMMERCE_SEGMENT_KEYS = {
    "buyers",
    "repeat_buyers",
    "high_aov_buyers",
    "discount_sensitive",
    "sku_affinity_top1",
}


ACTION_TH_MAP: Dict[str, str] = {
    "Launch dormant-user reactivation campaigns with segmented incentives.": "ทำแคมเปญกระตุ้นผู้ใช้ไม่เคลื่อนไหว โดยแยกสิทธิประโยชน์ตามเซกเมนต์",
    "Reduce message fatigue via tighter frequency caps and send-time optimization.": "ลดความล้าจากการสื่อสารด้วยการคุมความถี่และปรับเวลาส่งให้เหมาะสม",
    "Retarget recently lapsed cohorts with low-friction missions.": "ทำรีทาร์เก็ตกลุ่มที่เพิ่งเริ่มหลุด ด้วยมิชชันที่ทำได้ง่าย",
    "Audit mission completion friction (steps, UX, rules) and simplify top drop-off flows.": "ตรวจจุดฝืดของการทำมิชชัน (ขั้นตอน UX กติกา) และลดความซับซ้อนในจุดที่หลุดมากที่สุด",
    "Shift reward mix toward lower-friction, faster-win activities.": "ปรับสัดส่วนรางวัลไปที่กิจกรรมที่ทำง่ายและเห็นผลเร็ว",
    "A/B test mission copy and CTA clarity on the highest-volume activities.": "ทำ A/B test ข้อความมิชชันและ CTA ในกิจกรรมที่มีปริมาณสูงสุด",
    "Increase redemption visibility with in-app reminders and redemption-specific triggers.": "เพิ่มการมองเห็นการแลกรับด้วย in-app reminder และ trigger เฉพาะการแลก",
    "Rebalance point thresholds to reduce perceived redemption effort.": "ปรับ threshold การใช้แต้มให้แลกได้ง่ายขึ้น",
    "Promote expiring-value nudges to accelerate redemption intent.": "กระตุ้นการแลกด้วยข้อความแจ้งมูลค่าใกล้หมดอายุ",
    "Run short-cycle conversion pushes for high-intent audiences.": "ทำแคมเปญปิดการขายระยะสั้นกับกลุ่มที่มี intent สูง",
    "Bundle offers to lift average order value and repeat basket behavior.": "ทำข้อเสนอแบบ bundle เพื่อดัน AOV และเพิ่มการซื้อซ้ำ",
    "Check checkout/payment friction and recover abandoned purchase attempts.": "ตรวจจุดติดขัด checkout/payment และกู้คืนคำสั่งซื้อที่หลุดกลางทาง",
    "Introduce repeat-purchase triggers based on product replenishment cadence.": "ตั้ง trigger การซื้อซ้ำตามรอบการเติมสินค้า",
    "Activate personalized offer ladders for near-churn buyers.": "ทำลำดับข้อเสนอแบบ personalized สำหรับผู้ซื้อที่เสี่ยงหลุด",
    "Prioritize retention promotions for buyers with recent order decline.": "ให้โปร retention กับผู้ซื้อที่ออเดอร์ลดลงล่าสุด",
    "Create dormant-tier win-back journeys with escalating incentives.": "ออกแบบ win-back journey ตามระดับ dormancy พร้อมเพิ่มแรงจูงใจเป็นขั้น",
    "Use triggered education content to reintroduce loyalty value.": "ใช้คอนเทนต์แบบ trigger เพื่อสื่อสารคุณค่าของ loyalty อีกครั้ง",
    "Suppress low-intent users from broad blasts and target by propensity.": "ลดการยิงกว้างกับผู้ใช้ intent ต่ำ และยิงตาม propensity",
    "Recalibrate reward economics: trim low-yield rewards and raise completion-linked value.": "ปรับเศรษฐศาสตร์รางวัล: ตัดรางวัลที่ผลตอบแทนต่ำ และเพิ่มมูลค่าที่ผูกกับการทำสำเร็จ",
    "Prioritize activities with best completion-per-point efficiency.": "ให้ความสำคัญกับกิจกรรมที่มีประสิทธิภาพ completion-per-point สูง",
    "Set guardrails so point inflation does not outpace engagement conversion.": "ตั้ง guardrail ไม่ให้การเพิ่มแต้มเร็วกว่า conversion ของ engagement",
    "Diversify reward/product mix to reduce dependence on top SKU(s).": "กระจาย reward/product mix เพื่อลดการพึ่งพา SKU อันดับต้น",
    "Promote under-indexed SKUs through personalized recommendations.": "ดัน SKU ที่ยัง under-indexed ด้วยคำแนะนำแบบ personalized",
    "Use rotation strategy for featured SKUs to broaden basket composition.": "หมุน SKU ที่แสดงผลเพื่อกระจายองค์ประกอบตะกร้าสินค้า",
    "Active base is stable; run purchase triggers for `buyers_recent`-like cohorts (active/redeemers).": "ฐาน active ยังทรงตัว ให้รัน purchase trigger กับกลุ่มคล้าย `buyers_recent` (active/redeemers)",
    "Offer replenishment coupon to `repeat_buyers` cohort; test 7d expiry.": "ให้คูปองเติมสินค้ากับกลุ่ม `repeat_buyers` และทดสอบอายุคูปอง 7 วัน",
    "Monitor the next 1-2 windows and trigger targeted interventions for the weakest KPI.": "ติดตามอีก 1-2 window แล้วยิง intervention เฉพาะ KPI ที่อ่อนที่สุด",
    "Run controlled experiments on campaign cadence and reward mix.": "ทำ controlled experiment เรื่อง cadence แคมเปญและ reward mix",
    "Review segment-level funnel drops and prioritize high-impact fixes.": "ทบทวนจุดหลุดใน funnel ระดับเซกเมนต์และแก้จุดที่ impact สูงก่อน",
}


def _segment_objects(target_segments: Optional[Sequence[Mapping]]) -> List[dict]:
    if not target_segments:
        return []
    out: List[dict] = []
    for s in target_segments:
        if not isinstance(s, Mapping):
            continue
        k = str(s.get("segment_key", "")).strip()
        if not k:
            continue
        out.append(
            {
                "segment_key": k,
                "metric_family": str(s.get("metric_family", "")).strip(),
                "direction": str(s.get("direction", "")).strip(),
                "contribution_share": float(s.get("contribution_share", 0.0) or 0.0),
            }
        )
    out.sort(key=lambda x: x["contribution_share"], reverse=True)
    return out


def _segment_specific_actions(
    metric_family: str,
    seg_key: str,
    direction: str,
    commerce_joinable: bool,
    row: Optional[Mapping] = None,
) -> List[str]:
    actions: List[str] = []
    active_wow = float(row.get("active_users_wow_pct", 0.0)) if row else 0.0

    if metric_family == "active_users" and direction == "down" and seg_key in {"recently_lapsed_8_14d", "dormant_15_30d", "dormant_31_60d", "dormant_60d_plus"}:
        actions.extend(
            [
                f"Trigger winback rewards for `{seg_key}` with short expiry and capped frequency.",
                f"Set reactivation journeys for `{seg_key}` using low-friction missions first.",
            ]
        )
    if metric_family == "active_users" and direction == "down" and seg_key in {"new_users_0_7d"}:
        actions.append("Boost first-7-day activation missions for `new_users_0_7d` with immediate low-friction rewards.")

    if metric_family == "dormant_share" and direction == "up" and seg_key in {"dormant_15_30d", "dormant_31_60d", "dormant_60d_plus", "recently_lapsed_8_14d"}:
        actions.extend(
            [
                f"Apply reactivation + frequency caps specifically for `{seg_key}`.",
                f"Use low-friction missions for `{seg_key}` before high-effort offers.",
            ]
        )

    if metric_family == "activity_completion_rate" and direction == "down" and seg_key in {"active_0_7d", "non_redeemers"}:
        actions.append(f"Reduce mission friction for `{seg_key}` and A/B test mission flow copy.")

    if metric_family == "gmv_net" and direction == "down" and active_wow >= -0.05 and commerce_joinable:
        actions.append("Active base is stable; run purchase triggers for `buyers_recent`-like cohorts (active/redeemers).")
    if metric_family == "gmv_net" and direction == "down" and seg_key in {"buyers", "repeat_buyers", "high_aov_buyers"} and commerce_joinable:
        actions.append(f"Launch basket-building offers for `{seg_key}` to recover GMV quickly.")

    if metric_family == "transaction_count" and direction == "down" and seg_key in {"buyers", "repeat_buyers"} and commerce_joinable:
        actions.append(f"Trigger repeat-purchase journeys for `{seg_key}` using replenishment cadence.")
        actions.append("Offer replenishment coupon to `repeat_buyers` cohort; test 7d expiry.")

    if metric_family == "redeem_rate" and direction == "down" and seg_key in {"non_redeemers", "active_0_7d"}:
        actions.append(f"Send redemption-intent nudges to `{seg_key}` with easier point thresholds and expiry reminders.")

    if not commerce_joinable and seg_key in COMMERCE_SEGMENT_KEYS:
        return []

    return actions


def _action_to_i18n(action: str) -> dict:
    en = str(action)
    if en in ACTION_TH_MAP:
        return {"en": en, "th": ACTION_TH_MAP[en]}

    patterns = [
        (
            r"^Trigger winback rewards for `([^`]+)` with short expiry and capped frequency\.$",
            lambda seg: f"ส่งรางวัล winback ให้เซกเมนต์ `{seg}` โดยกำหนดอายุสั้นและคุมความถี่การส่ง",
        ),
        (
            r"^Set reactivation journeys for `([^`]+)` using low-friction missions first\.$",
            lambda seg: f"ตั้ง reactivation journey สำหรับ `{seg}` โดยเริ่มจากมิชชันที่ทำได้ง่าย",
        ),
        (
            r"^Boost first-7-day activation missions for `([^`]+)` with immediate low-friction rewards\.$",
            lambda seg: f"เพิ่มมิชชัน activation ช่วง 7 วันแรกสำหรับ `{seg}` พร้อมรางวัลที่รับได้ง่ายทันที",
        ),
        (
            r"^Apply reactivation \+ frequency caps specifically for `([^`]+)`\.$",
            lambda seg: f"ทำ reactivation และคุมความถี่เฉพาะเซกเมนต์ `{seg}`",
        ),
        (
            r"^Use low-friction missions for `([^`]+)` before high-effort offers\.$",
            lambda seg: f"ใช้มิชชันที่ทำง่ายกับ `{seg}` ก่อนข้อเสนอที่ต้องใช้ effort สูง",
        ),
        (
            r"^Reduce mission friction for `([^`]+)` and A/B test mission flow copy\.$",
            lambda seg: f"ลดความฝืดของมิชชันสำหรับ `{seg}` และทำ A/B test ข้อความใน flow",
        ),
        (
            r"^Launch basket-building offers for `([^`]+)` to recover GMV quickly\.$",
            lambda seg: f"ปล่อยข้อเสนอเพิ่มมูลค่าตะกร้าให้ `{seg}` เพื่อกู้ GMV อย่างรวดเร็ว",
        ),
        (
            r"^Trigger repeat-purchase journeys for `([^`]+)` using replenishment cadence\.$",
            lambda seg: f"ตั้ง journey ซื้อซ้ำให้ `{seg}` ตามรอบการเติมสินค้า",
        ),
        (
            r"^Send redemption-intent nudges to `([^`]+)` with easier point thresholds and expiry reminders\.$",
            lambda seg: f"ส่งข้อความกระตุ้นการแลกให้ `{seg}` โดยลด threshold แต้มและเตือนก่อนหมดอายุ",
        ),
    ]
    for pat, builder in patterns:
        m = re.match(pat, en)
        if m:
            return {"en": en, "th": builder(m.group(1))}

    return {"en": en, "th": en}


def build_actions_i18n(actions: Sequence[str]) -> List[dict]:
    out: List[dict] = []
    for action in actions:
        out.append(_action_to_i18n(str(action)))
    return out


def map_drivers_to_actions(
    drivers: Sequence[Mapping],
    target_segments: Optional[Sequence[Mapping]] = None,
    row: Optional[Mapping] = None,
    top_n: int = 3,
) -> List[str]:
    actions: List[str] = []
    seen: set = set()
    commerce_joinable = bool(float(row.get("commerce_joinable", 0.0))) if row is not None else False
    seg_objs = _segment_objects(target_segments)

    for seg in seg_objs:
        seg_key = seg["segment_key"]
        mf = seg["metric_family"]
        direction = seg["direction"]
        if not commerce_joinable and seg_key in COMMERCE_SEGMENT_KEYS:
            continue
        for action in _segment_specific_actions(
            metric_family=mf,
            seg_key=seg_key,
            direction=direction,
            commerce_joinable=commerce_joinable,
            row=row,
        ):
            if action not in seen:
                seen.add(action)
                actions.append(action)
            if len(actions) >= top_n:
                return actions

    for d in drivers:
        key = canonical_driver_key(str(d.get("key", "")))
        mf = str(d.get("metric_family", "")).strip() or infer_metric_family_from_key(key) or ""
        if mf in {"gmv_net", "transaction_count", "sku_concentration"} and not commerce_joinable:
            continue
        for action in ACTION_MAP.get(mf, []):
            if action not in seen:
                seen.add(action)
                actions.append(action)
            if len(actions) >= top_n:
                return actions
        for action in ACTION_MAP.get(key, []):
            if action not in seen:
                seen.add(action)
                actions.append(action)
            if len(actions) >= top_n:
                return actions

    if not actions:
        for d in drivers:
            key = canonical_driver_key(str(d.get("key", "")))
            mf = str(d.get("metric_family", "")).strip() or infer_metric_family_from_key(key) or ""
            if mf in {"gmv_net", "transaction_count", "sku_concentration"} and not commerce_joinable:
                continue
            for action in ACTION_MAP.get(mf, []):
                if action not in seen:
                    seen.add(action)
                    actions.append(action)
                if len(actions) >= top_n:
                    return actions

    # Fallback generic actions.
    if not actions:
        actions = [
            "Monitor the next 1-2 windows and trigger targeted interventions for the weakest KPI.",
            "Run controlled experiments on campaign cadence and reward mix.",
            "Review segment-level funnel drops and prioritize high-impact fixes.",
        ][:top_n]

    return actions


def attach_actions(predictions_df, top_n: int = 3):
    df = predictions_df.copy()
    df["suggested_actions"] = df.apply(
        lambda r: map_drivers_to_actions(
            r.get("drivers", []),
            target_segments=r.get("target_segments", []),
            row=r.to_dict(),
            top_n=top_n,
        ),
        axis=1,
    )
    return df
