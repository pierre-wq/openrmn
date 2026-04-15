"""openRMN agent — deterministic layer + AI brief (Anthropic)."""
from __future__ import annotations

import math
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd

from connectors import fetch_all


RMN_TO_SLUG: Dict[str, str] = {
    "Amazon Ads": "amazon",
    "Criteo Retail Media": "criteo",
    "Unlimitail": "unlimitail",
}
SLUG_TO_RMN: Dict[str, str] = {v: k for k, v in RMN_TO_SLUG.items()}

NETWORK_METHODOLOGIES: Dict[str, Dict[str, Any]] = {
    "amazon": {
        "attribution_window_days": 7,
        "attribution_type": "last-click",
        "includes_view_through": True,
        "includes_offline_sales": True,
        "mrc_certified": False,
        "documentation_url": "https://advertising.amazon.com/help/G3J6L9TLZWJ8XBQ7",
    },
    "criteo": {
        "attribution_window_days": 30,
        "attribution_type": "last-click",
        "includes_view_through": False,
        "includes_offline_sales": False,
        "mrc_certified": True,
        "documentation_url": "https://www.criteo.com/legal/measurement-methodology/",
    },
    "unlimitail": {
        "attribution_window_days": 14,
        "attribution_type": "last-click + assisted",
        "includes_view_through": True,
        "includes_offline_sales": True,
        "mrc_certified": False,
        "documentation_url": "",
    },
}

METHODOLOGY_TRANSPARENCY_SCORES: Dict[str, int] = {
    "amazon": 90,
    "criteo": 70,
    "unlimitail": 60,
}


def compute_kpis(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty:
        return {
            "spend_total_eur": 0.0, "sales_total_eur": 0.0, "roas_unified": 0.0,
            "ctr_pct": 0.0, "cpc_eur": 0.0, "breakdown_by_rmn": {},
            "rows": 0,
        }
    spend = float(df["spend_eur"].sum())
    sales = float(df["sales_eur"].sum())
    clicks = int(df["clicks"].sum())
    impressions = int(df["impressions"].sum())
    roas = sales / spend if spend else 0.0

    breakdown: Dict[str, Dict[str, float]] = {}
    for rmn, sub in df.groupby("rmn"):
        s_spend = float(sub["spend_eur"].sum())
        s_sales = float(sub["sales_eur"].sum())
        s_clicks = int(sub["clicks"].sum())
        s_impr = int(sub["impressions"].sum())
        breakdown[rmn] = {
            "spend_eur": round(s_spend, 2),
            "sales_eur": round(s_sales, 2),
            "roas": round(s_sales / s_spend, 2) if s_spend else 0.0,
            "ctr_pct": round(100 * s_clicks / s_impr, 3) if s_impr else 0.0,
            "cpc_eur": round(s_spend / s_clicks, 3) if s_clicks else 0.0,
            "units_sold": int(sub["units_sold"].sum()),
            "ntb_units": int(sub["new_to_brand_units"].sum()),
        }

    return {
        "spend_total_eur": round(spend, 2),
        "sales_total_eur": round(sales, 2),
        "roas_unified": round(roas, 2),
        "ctr_pct": round(100 * clicks / impressions, 3) if impressions else 0.0,
        "cpc_eur": round(spend / clicks, 3) if clicks else 0.0,
        "breakdown_by_rmn": breakdown,
        "rows": int(len(df)),
    }


def detect_anomalies(df: pd.DataFrame) -> List[Dict[str, Any]]:
    anomalies: List[Dict[str, Any]] = []
    if df.empty:
        return anomalies

    by_prod_rmn = (
        df.groupby(["product_name", "rmn"])[["spend_eur", "sales_eur"]]
        .sum()
        .reset_index()
    )
    by_prod_rmn["roas"] = by_prod_rmn["sales_eur"] / by_prod_rmn["spend_eur"].replace(0, pd.NA)

    pivot = by_prod_rmn.pivot(index="product_name", columns="rmn", values="roas")
    rmns = list(pivot.columns)
    seen_pairs = set()
    for product, row in pivot.iterrows():
        for i, a in enumerate(rmns):
            for b in rmns[i + 1:]:
                ra, rb = row.get(a), row.get(b)
                if pd.isna(ra) or pd.isna(rb) or rb == 0:
                    continue
                ratio = float(ra) / float(rb)
                if not (ratio > 1.8 or ratio < 0.55):
                    continue
                key = (product, a, b)
                if key in seen_pairs:
                    continue
                seen_pairs.add(key)
                anomalies.append({
                    "type": "potential_cannibalization",
                    "product": product,
                    "rmn_a": a, "rmn_b": b,
                    "roas_a": round(float(ra), 2),
                    "roas_b": round(float(rb), 2),
                    "ratio": round(ratio, 2),
                    "severity": "high" if (ratio > 2.5 or ratio < 0.4) else "medium",
                    "message": (
                        f"Significant ROAS gap on \"{product}\": "
                        f"{a} {float(ra):.2f} vs {b} {float(rb):.2f} (x{ratio:.2f})."
                    ),
                })

    by_campaign = (
        df.groupby(["rmn", "campaign_name"])[["spend_eur", "sales_eur"]]
        .sum()
        .reset_index()
    )
    by_campaign["roas"] = by_campaign["sales_eur"] / by_campaign["spend_eur"].replace(0, pd.NA)
    for _, r in by_campaign.iterrows():
        if r["spend_eur"] > 5000 and r["roas"] is not pd.NA and float(r["roas"]) < 1.5:
            anomalies.append({
                "type": "underperforming_campaign",
                "rmn": r["rmn"],
                "campaign": r["campaign_name"],
                "spend_eur": round(float(r["spend_eur"]), 2),
                "sales_eur": round(float(r["sales_eur"]), 2),
                "roas": round(float(r["roas"]), 2),
                "severity": "high" if float(r["roas"]) < 1.0 else "medium",
                "message": (
                    f"Campaign \"{r['campaign_name']}\" ({r['rmn']}): "
                    f"ROAS {float(r['roas']):.2f} on {float(r['spend_eur']):,.0f} EUR invested."
                ),
            })

    return anomalies


SOURCE_TO_RMN: Dict[str, str] = {
    "amazon": "Amazon Ads",
    "criteo": "Criteo Retail Media",
    "unlimitail": "Unlimitail",
}


def apply_filters(
    df: pd.DataFrame,
    products: List[str] | None = None,
    campaigns: List[str] | None = None,
    sources: List[str] | None = None,
) -> pd.DataFrame:
    """Filter a DataFrame by sources (networks), products and/or campaign_id."""
    if df.empty:
        return df
    out = df
    if sources:
        wanted_rmns = {SOURCE_TO_RMN.get(s.strip().lower(), s.strip()) for s in sources if s and s.strip()}
        if wanted_rmns:
            out = out[out["rmn"].isin(wanted_rmns)]
    if products:
        wanted = {p.strip().lower() for p in products if p and p.strip()}
        if wanted:
            out = out[out["product_name"].astype(str).str.strip().str.lower().isin(wanted)]
    if campaigns:
        wanted_c = {c.strip() for c in campaigns if c and c.strip()}
        if wanted_c:
            out = out[out["campaign_id"].astype(str).isin(wanted_c)]
    return out


def build_catalog(df: pd.DataFrame, period_days: int = 14) -> Dict[str, Any]:
    """Build the selection catalog: available products + campaigns."""
    if df.empty:
        return {
            "products": [], "campaigns": [],
            "summary": {"total_products": 0, "total_campaigns": 0, "total_rmns": 0, "period_days": period_days},
        }

    prod_rows: List[Dict[str, Any]] = []
    for product, g in df.groupby("product_name"):
        if not product:
            continue
        spend = float(g["spend_eur"].sum())
        sales = float(g["sales_eur"].sum())
        rmns = sorted(g["rmn"].unique().tolist())
        prod_rows.append({
            "name": product,
            "rmns": rmns,
            "total_spend": round(spend, 2),
            "total_sales": round(sales, 2),
            "roas": round(sales / spend, 2) if spend else 0.0,
        })
    prod_rows.sort(key=lambda x: -x["total_spend"])

    camp_rows: List[Dict[str, Any]] = []
    for (camp_id, rmn), g in df.groupby(["campaign_id", "rmn"]):
        if not camp_id:
            continue
        spend = float(g["spend_eur"].sum())
        sales = float(g["sales_eur"].sum())
        camp_name = str(g["campaign_name"].iloc[0])
        camp_rows.append({
            "id": str(camp_id),
            "name": camp_name,
            "rmn": rmn,
            "total_spend": round(spend, 2),
            "total_sales": round(sales, 2),
            "roas": round(sales / spend, 2) if spend else 0.0,
        })
    camp_rows.sort(key=lambda x: (x["rmn"], -x["total_spend"]))

    return {
        "products": prod_rows,
        "campaigns": camp_rows,
        "summary": {
            "total_products": len(prod_rows),
            "total_campaigns": len(camp_rows),
            "total_rmns": int(df["rmn"].nunique()),
            "period_days": period_days,
        },
    }


def product_detail(df: pd.DataFrame, product: str) -> Dict[str, Any]:
    """Per-product detail: per-network aggregates + daily series + neutrality."""
    empty = {"product_name": product, "by_rmn": [], "neutrality": None}
    if df.empty or not product:
        return empty
    target = product.strip().lower()
    sub = df[df["product_name"].astype(str).str.strip().str.lower() == target]
    if sub.empty:
        return {**empty, "error": "Product not found in the current period."}

    by_rmn: List[Dict[str, Any]] = []
    for rmn, g in sub.groupby("rmn"):
        spend = float(g["spend_eur"].sum())
        sales = float(g["sales_eur"].sum())
        clicks = int(g["clicks"].sum())
        impressions = int(g["impressions"].sum())

        daily_grp = (
            g.groupby("date")[["spend_eur", "sales_eur"]].sum().reset_index()
            .sort_values("date")
        )
        daily_grp["roas"] = daily_grp["sales_eur"] / daily_grp["spend_eur"].replace(0, pd.NA)
        daily = []
        for _, r in daily_grp.iterrows():
            roas_v = r["roas"]
            daily.append({
                "date": str(r["date"]),
                "spend": round(float(r["spend_eur"]), 2),
                "sales": round(float(r["sales_eur"]), 2),
                "roas": round(float(roas_v), 2) if not pd.isna(roas_v) else 0.0,
            })

        by_rmn.append({
            "rmn": rmn,
            "spend": round(spend, 2),
            "sales": round(sales, 2),
            "roas": round(sales / spend, 2) if spend else 0.0,
            "impressions": impressions,
            "clicks": clicks,
            "ctr": round(100 * clicks / impressions, 3) if impressions else 0.0,
            "cpc": round(spend / clicks, 3) if clicks else 0.0,
            "units": int(g["units_sold"].sum()),
            "new_to_brand_units": int(g["new_to_brand_units"].sum()),
            "daily": daily,
        })

    total_attr = sum(r["sales"] for r in by_rmn)
    max_rmn_sales = max((r["sales"] for r in by_rmn), default=0.0)
    est_low = round(max_rmn_sales * 0.85, 2)
    est_high = round(max_rmn_sales * 1.05, 2)
    est_mid = (est_low + est_high) / 2 if (est_low + est_high) else 0.0
    over_ratio = round(total_attr / est_mid, 2) if est_mid else 0.0
    over_pct = int(round((over_ratio - 1) * 100)) if over_ratio > 1 else 0
    note = (
        f"Networks collectively self-attribute {over_pct}% more than the estimated real "
        f"sales for this product." if over_pct > 0 else
        "No over-attribution detected on this product."
    )

    return {
        "product_name": sub["product_name"].iloc[0],
        "by_rmn": by_rmn,
        "neutrality": {
            "total_attributed_sales": round(total_attr, 2),
            "estimated_real_sales_range": [est_low, est_high],
            "over_attribution_ratio": over_ratio,
            "note": note,
        },
    }


def neutrality_audit(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty:
        return {"per_product": [], "shares_avg_pct": {}, "rmns": [],
                "avg_amazon_share_pct": None,
                "comment": "No data available for the audit."}
    by_prod_rmn = (
        df.groupby(["product_name", "rmn"])[["sales_eur", "units_sold"]]
        .sum()
        .reset_index()
    )
    pivot_sales = by_prod_rmn.pivot(index="product_name", columns="rmn", values="sales_eur").fillna(0)
    pivot_units = by_prod_rmn.pivot(index="product_name", columns="rmn", values="units_sold").fillna(0)
    rmns = list(pivot_sales.columns)

    results: List[Dict[str, Any]] = []
    for product in pivot_sales.index:
        sales_by_rmn = {r: float(pivot_sales.loc[product, r]) for r in rmns}
        units_by_rmn = {r: int(pivot_units.loc[product, r]) for r in rmns}
        total = sum(sales_by_rmn.values())
        if total <= 0:
            continue
        shares = {r: round(100 * v / total, 1) for r, v in sales_by_rmn.items()}
        amz_key = next((r for r in rmns if "Amazon" in r), None)
        results.append({
            "product": product,
            "sales_by_rmn": {r: round(v, 2) for r, v in sales_by_rmn.items()},
            "units_by_rmn": units_by_rmn,
            "shares_pct": shares,
            "amazon_share_pct": shares.get(amz_key, 0.0) if amz_key else None,
            "total_sales_eur": round(total, 2),
        })

    shares_avg: Dict[str, float] = {}
    if results:
        for r in rmns:
            shares_avg[r] = round(
                sum(item["shares_pct"].get(r, 0) for item in results) / len(results), 1
            )

    return {
        "per_product": results,
        "shares_avg_pct": shares_avg,
        "rmns": rmns,
        "avg_amazon_share_pct": shares_avg.get(
            next((r for r in rmns if "Amazon" in r), ""), None
        ),
        "comment": (
            "Share of sales each network self-attributes on common SKUs. "
            "The sum of shares exceeds 100% of real revenue: "
            "cumulative over-attribution measures the walled-garden bias."
        ),
    }


def _grade_from_score(score: float) -> str:
    if score > 90: return "A+"
    if score > 85: return "A"
    if score > 75: return "B+"
    if score > 65: return "B"
    if score > 55: return "C+"
    if score > 45: return "C"
    return "D"


def _safe_clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    if x != x:  # NaN
        return lo
    return max(lo, min(hi, x))


def trust_score(df: pd.DataFrame) -> Dict[str, Any]:
    """Compute a 0-100 trust score per network.
    Components (weighted):
      - internal_consistency 30%  : 100 - CV_ROAS_daily*100
      - cross_network_convergence 25%: 1 - |share - 1/N| on common SKUs
      - methodology_transparency 25% : static public-knowledge score
      - data_freshness 20%        : 100 if <24h, linear to 0 at 7d+
    """
    out: Dict[str, Any] = {}
    if df.empty:
        return out

    today = pd.Timestamp(datetime.now(timezone.utc).date())
    total_by_sku_rmn = (
        df.groupby(["product_name", "rmn"])["sales_eur"].sum().reset_index()
    )
    pivot = total_by_sku_rmn.pivot(
        index="product_name", columns="rmn", values="sales_eur"
    ).fillna(0)
    rmns_present = list(pivot.columns)
    n_rmns = len(rmns_present) if rmns_present else 1

    for rmn, sub in df.groupby("rmn"):
        slug = RMN_TO_SLUG.get(rmn, str(rmn).lower().replace(" ", "_"))

        daily = sub.groupby("date")[["spend_eur", "sales_eur"]].sum().reset_index()
        daily = daily[daily["spend_eur"] > 0]
        if len(daily) >= 2:
            daily["roas"] = daily["sales_eur"] / daily["spend_eur"]
            mean = float(daily["roas"].mean())
            std = float(daily["roas"].std(ddof=0))
            cv = std / mean if mean > 0 else 1.0
            ic_score = _safe_clamp(100.0 - cv * 100.0)
        else:
            ic_score = 60.0

        conv_num, conv_den = 0.0, 0.0
        for sku in pivot.index:
            row = pivot.loc[sku]
            total = float(row.sum())
            if total <= 0 or rmn not in row.index:
                continue
            share = float(row[rmn]) / total
            ideal = 1.0 / n_rmns
            # proximity to fair share (1-|share-ideal|/ideal clamped) then scale 0-100
            prox = 1.0 - min(1.0, abs(share - ideal) / max(ideal, 1e-9))
            conv_num += prox * total
            conv_den += total
        conv_score = _safe_clamp((conv_num / conv_den) * 100.0 if conv_den > 0 else 50.0)

        mt_score = float(METHODOLOGY_TRANSPARENCY_SCORES.get(slug, 50))

        latest = pd.to_datetime(sub["date"]).max()
        if pd.isna(latest):
            freshness_score = 50.0
        else:
            age_days = max(0.0, (today - pd.Timestamp(latest)).total_seconds() / 86400.0)
            if age_days < 1.0:
                freshness_score = 100.0
            elif age_days >= 7.0:
                freshness_score = 0.0
            else:
                freshness_score = 100.0 * (1.0 - (age_days - 1.0) / 6.0)
            freshness_score = _safe_clamp(freshness_score)

        weights = {"internal_consistency": 30, "cross_network_convergence": 25,
                   "methodology_transparency": 25, "data_freshness": 20}
        scores = {"internal_consistency": ic_score,
                  "cross_network_convergence": conv_score,
                  "methodology_transparency": mt_score,
                  "data_freshness": freshness_score}
        global_score = sum(scores[k] * weights[k] for k in weights) / 100.0
        grade = _grade_from_score(global_score)

        meth = NETWORK_METHODOLOGIES.get(slug, {})
        findings: List[str] = []
        if meth:
            win = meth.get("attribution_window_days")
            typ = meth.get("attribution_type", "—")
            findings.append(f"Attribution methodology: {typ}, {win}-day window"
                            + (" (MRC certified)" if meth.get("mrc_certified") else ""))
        if conv_score < 55:
            findings.append(
                f"Probable over- or under-attribution on common SKUs "
                f"(convergence {conv_score:.0f}/100)."
            )
        elif conv_score > 80:
            findings.append(f"Consistent share on common SKUs (convergence {conv_score:.0f}/100).")
        if freshness_score >= 85:
            findings.append("Fresh data (lag <24h).")
        elif freshness_score <= 30:
            findings.append("Data possibly stale (>5 days).")
        if ic_score < 50:
            findings.append(f"Daily ROAS highly volatile (consistency {ic_score:.0f}/100).")
        if not findings:
            findings.append("No strong alert signal.")

        out[slug] = {
            "rmn_label": rmn,
            "score": int(round(global_score)),
            "grade": grade,
            "components": {
                "internal_consistency": {
                    "score": int(round(ic_score)), "weight": weights["internal_consistency"],
                    "explanation": ("Variance of daily ROAS over the period. "
                                    "Low variance = high score."),
                },
                "cross_network_convergence": {
                    "score": int(round(conv_score)), "weight": weights["cross_network_convergence"],
                    "explanation": ("Gap between the network's share of attributed sales and the "
                                    "fair share on common SKUs. A network that attributes much "
                                    "more than peers has low convergence."),
                },
                "methodology_transparency": {
                    "score": int(round(mt_score)), "weight": weights["methodology_transparency"],
                    "explanation": ("Published attribution window, documented attribution type, "
                                    "MRC certification."),
                },
                "data_freshness": {
                    "score": int(round(freshness_score)), "weight": weights["data_freshness"],
                    "explanation": ("Delay between measurement and API ingestion. "
                                    "The fresher the better."),
                },
            },
            "key_findings": findings[:4],
        }
    return out


def methodology_comparison() -> Dict[str, Dict[str, Any]]:
    """Return declared methodologies per network."""
    return {slug: dict(meta) for slug, meta in NETWORK_METHODOLOGIES.items()}


def simulate_harmonization(
    df: pd.DataFrame,
    target_window_days: int = 7,
    target_type: str = "last-click",
) -> Dict[str, Any]:
    """Apply a deduplication coefficient based on declared windows.
    Heuristic: attributed sales grow as sqrt(window_days).
    coefficient = sqrt(target_window / actual_window)
    """
    if df.empty:
        return {
            "before": compute_kpis(df), "after": compute_kpis(df),
            "delta_per_network": {}, "target_window_days": target_window_days,
            "target_type": target_type,
            "assumptions": "Deduplication coefficient = sqrt(target_window / actual_window).",
        }

    before_kpis = compute_kpis(df)

    df2 = df.copy()
    per_network_coef: Dict[str, float] = {}
    for rmn, sub_idx in df2.groupby("rmn").groups.items():
        slug = RMN_TO_SLUG.get(rmn, str(rmn).lower().replace(" ", "_"))
        meth = NETWORK_METHODOLOGIES.get(slug, {})
        actual_w = float(meth.get("attribution_window_days", target_window_days))
        if actual_w <= 0:
            coef = 1.0
        else:
            coef = math.sqrt(target_window_days / actual_w)
        per_network_coef[slug] = round(coef, 4)
        df2.loc[sub_idx, "sales_eur"] = df2.loc[sub_idx, "sales_eur"] * coef

    after_kpis = compute_kpis(df2)

    delta: Dict[str, Any] = {}
    for rmn, b in before_kpis.get("breakdown_by_rmn", {}).items():
        a = after_kpis.get("breakdown_by_rmn", {}).get(rmn, {})
        slug = RMN_TO_SLUG.get(rmn, str(rmn).lower().replace(" ", "_"))
        roas_b = float(b.get("roas", 0) or 0)
        roas_a = float(a.get("roas", 0) or 0)
        pct = round(((roas_a / roas_b) - 1.0) * 100.0, 1) if roas_b else 0.0
        delta[slug] = {
            "rmn_label": rmn,
            "coef": per_network_coef.get(slug, 1.0),
            "roas_before": round(roas_b, 2),
            "roas_after": round(roas_a, 2),
            "delta_pct": pct,
            "sales_before_eur": b.get("sales_eur", 0),
            "sales_after_eur": a.get("sales_eur", 0),
        }

    roas_values_b = [v["roas_before"] for v in delta.values() if v["roas_before"]]
    roas_values_a = [v["roas_after"] for v in delta.values() if v["roas_after"]]
    spread_before = (max(roas_values_b) / min(roas_values_b) - 1) * 100 if roas_values_b and min(roas_values_b) > 0 else 0
    spread_after = (max(roas_values_a) / min(roas_values_a) - 1) * 100 if roas_values_a and min(roas_values_a) > 0 else 0

    return {
        "before": before_kpis,
        "after": after_kpis,
        "delta_per_network": delta,
        "target_window_days": target_window_days,
        "target_type": target_type,
        "roas_spread_before_pct": round(spread_before, 1),
        "roas_spread_after_pct": round(spread_after, 1),
        "assumptions": "Deduplication coefficient = sqrt(target_window / actual_window).",
    }


def double_counting_audit(df: pd.DataFrame) -> Dict[str, Any]:
    """Estimate cross-network over-attribution.
    Rule: real_sales = max(sales_per_rmn) * 1.1, overlap = total_attributed - real.
    Per-network allocation is proportional: real_rmn = sales_rmn * (real / total).
    """
    if df.empty:
        return {
            "total_attributed": 0.0, "estimated_real": 0.0, "overlap_amount": 0.0,
            "overlap_pct": 0.0, "per_product": [], "per_network": [], "flows": [],
            "note": "Methodology: estimated real sales = max(sales per network) × 1.1.",
        }

    by_prod_rmn = (
        df.groupby(["product_name", "rmn"])["sales_eur"].sum().reset_index()
    )
    pivot = by_prod_rmn.pivot(index="product_name", columns="rmn", values="sales_eur").fillna(0)

    total_attributed = 0.0
    total_real = 0.0
    per_product: List[Dict[str, Any]] = []
    per_rmn_attr: Dict[str, float] = {}

    for sku in pivot.index:
        row = pivot.loc[sku]
        total_row = float(row.sum())
        if total_row <= 0:
            continue
        if (row > 0).sum() < 2:
            # not a common SKU cross-network — include attribution anyway, but overlap=0
            real_sku = total_row
        else:
            real_sku = float(row.max()) * 1.1
            real_sku = min(real_sku, total_row)
        overlap_sku = max(0.0, total_row - real_sku)
        total_attributed += total_row
        total_real += real_sku
        per_product.append({
            "product": sku,
            "total_attributed": round(total_row, 2),
            "estimated_real": round(real_sku, 2),
            "overlap": round(overlap_sku, 2),
            "overlap_pct": round(100 * overlap_sku / total_row, 1) if total_row else 0.0,
            "sales_by_rmn": {r: round(float(row[r]), 2) for r in row.index if float(row[r]) > 0},
        })
        for r in row.index:
            per_rmn_attr[r] = per_rmn_attr.get(r, 0.0) + float(row[r])

    overlap_total = max(0.0, total_attributed - total_real)
    overlap_pct = round(100 * overlap_total / total_attributed, 1) if total_attributed else 0.0

    per_network: List[Dict[str, Any]] = []
    flows: List[Dict[str, Any]] = []
    for rmn, attr in per_rmn_attr.items():
        if attr <= 0:
            continue
        real_share = (total_real / total_attributed) if total_attributed else 1.0
        real_r = attr * real_share
        overlap_r = attr - real_r
        slug = RMN_TO_SLUG.get(rmn, str(rmn).lower().replace(" ", "_"))
        per_network.append({
            "rmn": rmn, "slug": slug,
            "attributed": round(attr, 2),
            "real": round(real_r, 2),
            "overlap": round(overlap_r, 2),
            "overlap_pct": round(100 * overlap_r / attr, 1) if attr else 0.0,
        })
        flows.append({
            "from": f"{rmn} attributed", "from_slug": slug,
            "to": "Real sales", "to_kind": "real",
            "value": round(real_r, 2),
        })
        flows.append({
            "from": f"{rmn} attributed", "from_slug": slug,
            "to": "Overlap", "to_kind": "overlap",
            "value": round(overlap_r, 2),
        })

    per_network.sort(key=lambda x: -x["attributed"])
    per_product.sort(key=lambda x: -x["overlap"])

    return {
        "total_attributed": round(total_attributed, 2),
        "estimated_real": round(total_real, 2),
        "overlap_amount": round(overlap_total, 2),
        "overlap_pct": overlap_pct,
        "per_product": per_product,
        "per_network": per_network,
        "flows": flows,
        "note": ("Methodology: estimated real sales = max(sales per network) × 1.1 on "
                 "common SKUs. Defensible assumption — to be validated with third-party "
                 "panel data (Wakoopa, Nielsen, Kantar Worldpanel)."),
    }


PERSONA_PROMPTS: Dict[str, str] = {
    "executive": """You are a strategic media advisor speaking to a CMO.
Your brief must be readable in 2 minutes. Focus on 30-day budget stakes,
cross-network arbitrages, and business impact. No technical jargon. Quantify
each recommendation with a magnitude of impact.

Strict markdown structure:

## Situation

## Risks

## Decisions to make

Professional English, direct. No preamble, no conclusion.""",

    "operational": """You are a senior retail media trader. Your brief is for
the operational team. Actionable recommendations week by week: bid adjustments,
dayparting, negative keywords, reallocation by retailer. Each action must
have an estimated impact and a priority (P0/P1/P2).

Strict markdown structure:

## Performance this week

## Immediate actions

## Tests to launch

English, trader tone (concise, quantified). No preamble, no conclusion.""",

    "neutrality": """You are an independent auditor of advertising measurement.
Your role is to challenge the numbers declared by each retail media network.
Analyze attribution gaps between Amazon, Criteo and Unlimitail on common SKUs.
Question methodologies (attribution windows, last-click vs assisted). Propose
a weighting coefficient to estimate deduplicated real sales.

Use the provided Trust Scores and double-counting audit figures to substantiate
your report. Name the Trust Score components (internal_consistency,
cross_network_convergence, methodology_transparency, data_freshness) that weigh
most in your findings, and rely on the over-attribution amount (overlap_amount)
and global percentage (overlap_pct) to quantify conclusions.

Factual, cautious audit tone. Free markdown structure but must include:

## Attribution findings

## Methodological hypotheses

## Proposed weighting coefficient

Formal English, uncompromising, no preamble.""",
}

SYSTEM_PROMPT = PERSONA_PROMPTS["executive"]

ASK_SYSTEM_PROMPT = """You are an experienced retail media analyst. The user
asks you free-form questions about their campaigns (Amazon Ads, Criteo Retail
Media, Unlimitail). You have access to aggregated KPIs, detected anomalies and
the neutrality audit — use them as factual source.

Answer in English, concise (max 250 words unless the question requires a
detailed response). Quantify your claims with the provided data. If the question
exceeds the available data, say so honestly and suggest what would be needed to
answer it. No preamble, no disclaimer."""


def build_brief_payload(
    df: pd.DataFrame,
    extra: str = "",
    products: List[str] | None = None,
    campaigns: List[str] | None = None,
) -> str:
    kpis = compute_kpis(df)
    anomalies = detect_anomalies(df)
    audit = neutrality_audit(df)
    trust = trust_score(df)
    dbl = double_counting_audit(df)
    trust_brief = {
        slug: {
            "score": t["score"], "grade": t["grade"],
            "components": {k: v["score"] for k, v in t["components"].items()},
            "key_findings": t["key_findings"],
        }
        for slug, t in trust.items()
    }
    dbl_brief = {
        "total_attributed": dbl.get("total_attributed", 0),
        "estimated_real": dbl.get("estimated_real", 0),
        "overlap_amount": dbl.get("overlap_amount", 0),
        "overlap_pct": dbl.get("overlap_pct", 0),
        "per_network": dbl.get("per_network", []),
        "top_products": dbl.get("per_product", [])[:3],
    }
    scope_lines = []
    if products:
        scope_lines.append(f"Products in scope ({len(products)}): {', '.join(products)}")
    if campaigns:
        scope_lines.append(f"Campaigns in scope ({len(campaigns)}): {', '.join(campaigns)}")
    if not scope_lines:
        scope_lines.append("Scope: full portfolio (no filter).")
    scope_block = "### Selection scope\n" + "\n".join(scope_lines) + "\n\n"
    base = (
        "Consolidated data over the last 14 days for the advertiser "
        "\"Maison Café & Thé\" (4 main SKUs, 3 retail media networks).\n\n"
        f"{scope_block}"
        f"### Aggregated KPIs\n{kpis}\n\n"
        f"### Automatically detected anomalies\n{anomalies}\n\n"
        f"### Neutrality audit (on cross-network common SKUs)\n{audit}\n\n"
        f"### Trust Score per network\n{trust_brief}\n\n"
        f"### Double-counting audit\n{dbl_brief}\n"
    )
    return base + (f"\n{extra}" if extra else "")


def run_agent(df: pd.DataFrame, persona: str = "executive") -> str:
    import anthropic
    system = PERSONA_PROMPTS.get(persona, PERSONA_PROMPTS["executive"])
    user_payload = build_brief_payload(df, "Produce the brief.")
    client = anthropic.Anthropic()
    resp = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=2000,
        system=system,
        messages=[{"role": "user", "content": user_payload}],
    )
    return resp.content[0].text


def main():
    print("=" * 70)
    print("openRMN — MVP Retail Media Analytics")
    print("=" * 70)

    print("\n[1/4] Multi-network ingestion…")
    df = fetch_all(days=14)
    print(f"  → {len(df)} unified rows across {df['rmn'].nunique()} networks.")

    print("\n[2/4] KPI computation…")
    kpis = compute_kpis(df)
    print(f"  Spend: {kpis['spend_total_eur']:,.0f} EUR")
    print(f"  Sales: {kpis['sales_total_eur']:,.0f} EUR")
    print(f"  Unified ROAS: {kpis['roas_unified']}")
    for rmn, b in kpis["breakdown_by_rmn"].items():
        print(f"  {rmn:25s} | spend {b['spend_eur']:>10,.0f} | sales {b['sales_eur']:>10,.0f} | ROAS {b['roas']:.2f} | CTR {b['ctr_pct']}% | CPC {b['cpc_eur']}€")

    print("\n[3/4] Anomaly detection…")
    anomalies = detect_anomalies(df)
    print(f"  → {len(anomalies)} anomalies detected.")
    for a in anomalies[:6]:
        print(f"  [{a['severity']:6s}] {a['type']:30s} — {a['message']}")
    if len(anomalies) > 6:
        print(f"  … (+{len(anomalies) - 6} more)")

    print("\n  Neutrality audit:")
    audit = neutrality_audit(df)
    print(f"  Average Amazon share on common SKUs: {audit['avg_amazon_share_pct']}%")

    print("\n[4/4] AI brief (Anthropic)…")
    if not os.getenv("ANTHROPIC_API_KEY"):
        print("  ⚠ ANTHROPIC_API_KEY not set — skipping AI brief.")
        return
    brief = run_agent(df)
    print("\n" + "=" * 70)
    print("EXECUTIVE BRIEF")
    print("=" * 70)
    print(brief)


if __name__ == "__main__":
    main()
