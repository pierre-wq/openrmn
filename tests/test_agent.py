"""Unit tests for the deterministic analytics layer (agent.py).

Focus on the hardened credibility logic:
  - bounded double-counting estimate (range, not false-precision point)
  - per-product drill-down reconciles with the portfolio audit (shared estimator)
  - trust-score convergence benchmarks sales-share against click-share
"""
import os
import sys
from datetime import date

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agent import (
    _estimate_real_sales,
    double_counting_audit,
    product_detail,
    trust_score,
)


def _row(rmn, product, sales, clicks, spend=100.0, impressions=1000):
    return {
        "date": date.today(),
        "rmn": rmn,
        "retailer": rmn,
        "campaign_id": f"c-{rmn[:3].lower()}",
        "campaign_name": f"camp-{rmn[:3].lower()}",
        "sku": f"SKU-{product}",
        "product_name": product,
        "impressions": impressions,
        "clicks": clicks,
        "spend_eur": spend,
        "units_sold": int(sales // 20),
        "sales_eur": float(sales),
        "new_to_brand_units": 0,
    }


AMZ, CRT, UNL = "Amazon Ads", "Criteo Retail Media", "Unlimitail"


@pytest.fixture
def df():
    # Product "P" shared by 3 networks: Unlimitail over-attributes (10x sales,
    # same clicks) — the signature convergence must catch.
    rows = [
        _row(AMZ, "P", sales=100, clicks=100),
        _row(CRT, "P", sales=100, clicks=100),
        _row(UNL, "P", sales=1000, clicks=100),
    ]
    return pd.DataFrame(rows)


# ---- _estimate_real_sales: the shared bounded estimator ----

def test_estimate_real_sales_bounds():
    est = _estimate_real_sales([100.0, 100.0, 1000.0])
    assert est["total"] == 1200.0
    assert est["low"] == 1000.0          # >= biggest network
    assert est["high"] == 1200.0         # <= sum
    assert est["low"] <= est["point"] <= est["high"]
    assert est["point"] == pytest.approx(1100.0)  # 1000 * 1.10
    assert est["overlap"] == pytest.approx(100.0)
    assert est["overlap_max"] == pytest.approx(200.0)
    assert est["overlap"] <= est["overlap_max"]


def test_estimate_real_sales_single_network_no_overlap():
    est = _estimate_real_sales([500.0])
    assert est["overlap"] == 0.0
    assert est["point"] == 500.0


def test_estimate_real_sales_empty():
    est = _estimate_real_sales([0.0, 0.0])
    assert est["total"] == 0.0 and est["overlap"] == 0.0


# ---- double_counting_audit: bounded, keys present, monotone ----

def test_double_counting_range_and_keys(df):
    a = double_counting_audit(df)
    for k in ("estimated_real_low", "estimated_real", "estimated_real_high",
              "overlap_amount", "overlap_amount_max", "overlap_pct_max",
              "organic_uplift", "per_product", "per_network"):
        assert k in a
    assert a["estimated_real_low"] <= a["estimated_real"] <= a["estimated_real_high"]
    assert a["overlap_amount"] <= a["overlap_amount_max"]
    assert a["total_attributed"] == 1200.0


def test_double_counting_empty_df():
    a = double_counting_audit(pd.DataFrame(
        columns=["product_name", "rmn", "sales_eur", "clicks"]))
    assert a["overlap_amount"] == 0.0 and a["per_product"] == []


# ---- product_detail reconciles with the portfolio audit ----

def test_product_detail_reconciles_with_audit(df):
    detail = product_detail(df, "P")
    est = _estimate_real_sales([100.0, 100.0, 1000.0])
    assert detail["neutrality"]["estimated_real_sales"] == pytest.approx(est["point"])
    assert detail["neutrality"]["estimated_real_sales_range"] == [est["low"], est["high"]]
    # same estimator as the portfolio audit -> per-product figures match
    audit_p = next(p for p in double_counting_audit(df)["per_product"] if p["product"] == "P")
    assert audit_p["estimated_real"] == pytest.approx(est["point"])
    assert audit_p["overlap"] == pytest.approx(est["overlap"])


# ---- trust-score convergence: click-share, not 1/N ----

def test_convergence_penalises_over_attribution(df):
    ts = trust_score(df)
    amz = ts["amazon"]["components"]["cross_network_convergence"]["score"]
    unl = ts["unlimitail"]["components"]["cross_network_convergence"]["score"]
    # Unlimitail claims 10x the sales-share for the same click-share -> lower convergence
    assert unl < amz


def test_scores_bounded(df):
    for slug, t in trust_score(df).items():
        assert 0 <= t["score"] <= 100
        for comp in t["components"].values():
            assert 0 <= comp["score"] <= 100
