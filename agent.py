"""openRMN agent — deterministic layer + AI brief (Anthropic)."""
from __future__ import annotations

import os
from typing import Any, Dict, List

import pandas as pd

from connectors import fetch_all


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


def apply_filters(
    df: pd.DataFrame,
    products: List[str] | None = None,
    campaigns: List[str] | None = None,
) -> pd.DataFrame:
    """Filtre un DataFrame par liste de produits et/ou campaign_id (intersection)."""
    if df.empty:
        return df
    out = df
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
        return {**empty, "error": "Product not found for the current period."}

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
        f"The networks collectively self-attribute {over_pct}% more than the "
        f"estimated real sales for this product." if over_pct > 0 else
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
            "The sum of shares exceeds 100% of real revenue: cumulated "
            "over-attribution measures the walled-garden bias."
        ),
    }


PERSONA_PROMPTS: Dict[str, str] = {
    "executive": """You are a strategic media advisor speaking to a CMO. Your \
brief must be readable in 2 minutes. Focus on 30-day budget stakes, cross-network \
trade-offs, and business impact. No technical jargon. Quantify each recommendation \
with an order-of-magnitude impact.

Strict markdown structure:
## Situation
## Risks
## Decisions to make

English, professional, direct. No preamble, no conclusion.""",

    "operational": """You are a senior retail media trader. Your brief is for \
the operational team. Actionable recommendations week by week: bid adjustments, \
dayparting, negative keywords, retailer-level reallocation. Each action must \
include an estimated impact and a priority (P0/P1/P2).

Strict markdown structure:
## Performance this week
## Immediate actions
## Tests to launch

English, trader tone (concise, numerical). No preamble, no conclusion.""",

    "neutrality": """You are an independent auditor of advertising measurement. \
Your role is to challenge the figures self-reported by each retail media network. \
Analyze attribution gaps between Amazon, Criteo and Unlimitail on common SKUs. \
Question methodologies (attribution windows, last-click vs assisted). Propose a \
weighting coefficient to estimate de-duplicated real sales.

Factual, cautious audit-report tone. Free markdown structure but must include:
## Attribution findings
## Methodological hypotheses
## Proposed weighting coefficient

Formal English, uncompromising, no preamble.""",
}

SYSTEM_PROMPT = PERSONA_PROMPTS["executive"]

ASK_SYSTEM_PROMPT = """You are a seasoned retail media analyst. The user is \
asking a free-form question about their campaigns (Amazon Ads, Criteo Retail \
Media, Unlimitail). You have access to aggregated KPIs, detected anomalies and \
the neutrality audit — use them as factual grounding.

Reply in English, concise (max 250 words unless the question requires a detailed \
answer). Quantify your statements with the provided data. If the question goes \
beyond the available data, say so honestly and suggest what would be needed to \
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
    scope_lines = []
    if products:
        scope_lines.append(f"Products in scope ({len(products)}): {', '.join(products)}")
    if campaigns:
        scope_lines.append(f"Campaigns in scope ({len(campaigns)}): {', '.join(campaigns)}")
    if not scope_lines:
        scope_lines.append("Scope: full portfolio (no filter applied).")
    scope_block = "### Selection scope\n" + "\n".join(scope_lines) + "\n\n"
    base = (
        "Consolidated data for the past 14 days for the advertiser "
        "\"Maison Café & Thé\" (4 main SKUs, 3 retail media networks).\n\n"
        f"{scope_block}"
        f"### Aggregated KPIs\n{kpis}\n\n"
        f"### Anomalies detected automatically\n{anomalies}\n\n"
        f"### Neutrality audit (on cross-network common SKUs)\n{audit}\n"
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
