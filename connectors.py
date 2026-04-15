"""openRMN connectors — unifie les données Retail Media multi-régies."""
from __future__ import annotations

import gzip
import io
import json
import logging
import os
import random
import time
from dataclasses import dataclass, asdict
from datetime import date, timedelta
from typing import List, Optional

import httpx
import pandas as pd

log = logging.getLogger("openrmn.connectors")


@dataclass
class UnifiedRow:
    date: date
    rmn: str
    retailer: str
    campaign_id: str
    campaign_name: str
    sku: str
    product_name: str
    impressions: int
    clicks: int
    spend_eur: float
    units_sold: int
    sales_eur: float
    new_to_brand_units: int


PRODUCT_CATALOG = [
    ("SKU-CAF-001", "Cafetière Italienne Inox 6 tasses", 34.90),
    ("SKU-THE-002", "Théière Fonte Traditionnelle 1.2L", 42.50),
    ("SKU-MUG-003", "Mug Céramique Mat Noir 350ml", 18.90),
    ("SKU-FIL-004", "Filtres Papier Bio x100", 22.00),
]

AMAZON_CAMPAIGNS = [
    ("amz-sp-def-01", "SP_Defense_Brand_FR", "SP"),
    ("amz-sp-con-02", "SP_Conquest_Competitors_FR", "SP"),
    ("amz-sb-awa-03", "SB_Awareness_Cafe_Collection_FR", "SB"),
]

CRITEO_CAMPAIGNS = [
    ("crt-sp-def-11", "SP_Defense_Brand_FR", "SP"),
    ("crt-sp-con-12", "SP_Conquest_Competitors_FR", "SP"),
    ("crt-sb-awa-13", "SB_Awareness_Cafe_Collection_FR", "SB"),
]


def _daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


class AmazonAdsConnector:
    RMN = "Amazon Ads"
    RETAILER = "Amazon.fr"
    AUTH_URL = "https://api.amazon.com/auth/o2/token"
    ADS_BASE = "https://advertising-api-eu.amazon.com"

    _token_cache = {"access_token": None, "expires_at": 0.0}

    def __init__(self, profile_id: str | None = None, refresh_token: str | None = None, seed: int = 42):
        self.profile_id = profile_id or os.getenv("AMZ_PROFILE_ID")
        self.refresh_token = refresh_token or os.getenv("AMZ_REFRESH_TOKEN")
        self.client_id = os.getenv("AMZ_LWA_CLIENT_ID")
        self.client_secret = os.getenv("AMZ_LWA_CLIENT_SECRET")
        self.seed = seed

    def has_real_credentials(self) -> bool:
        return all([self.profile_id, self.refresh_token, self.client_id, self.client_secret])

    def fetch(self, start: date, end: date, mode: str = "auto") -> List[UnifiedRow]:
        if mode == "mock":
            return self._fetch_mock(start, end)
        if mode == "real":
            if not self.has_real_credentials():
                raise RuntimeError(
                    "Amazon Ads: credentials manquants — il faut AMZ_LWA_CLIENT_ID, "
                    "AMZ_LWA_CLIENT_SECRET, AMZ_REFRESH_TOKEN, AMZ_PROFILE_ID."
                )
            return self._fetch_real(start, end)
        if self.has_real_credentials():
            return self._fetch_real(start, end)
        return self._fetch_mock(start, end)

    def _get_access_token(self) -> str:
        now = time.time()
        if self._token_cache["access_token"] and self._token_cache["expires_at"] > now + 60:
            return self._token_cache["access_token"]
        r = httpx.post(
            self.AUTH_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=30.0,
        )
        r.raise_for_status()
        payload = r.json()
        token = payload["access_token"]
        AmazonAdsConnector._token_cache["access_token"] = token
        AmazonAdsConnector._token_cache["expires_at"] = now + min(
            int(payload.get("expires_in", 3600)) - 60, 3000
        )
        return token

    def _ads_headers(self, access_token: str, content_type: Optional[str] = None) -> dict:
        h = {
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": self.client_id,
            "Amazon-Advertising-API-Scope": str(self.profile_id),
        }
        if content_type:
            h["Content-Type"] = content_type
            h["Accept"] = content_type
        return h

    def _fetch_real(self, start: date, end: date) -> List[UnifiedRow]:
        access = self._get_access_token()
        report_id = self._create_report(access, start, end)
        log.info("Amazon report created: %s", report_id)
        report_url = self._poll_report(access, report_id)
        log.info("Amazon report ready: %s", report_id)
        return self._download_report(report_url, start, end)

    def _create_report(self, access: str, start: date, end: date) -> str:
        body = {
            "name": f"openrmn-spCampaigns-{start}-{end}",
            "startDate": start.isoformat(),
            "endDate": end.isoformat(),
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS",
                "groupBy": ["campaign"],
                "columns": [
                    "campaignId", "campaignName", "date",
                    "impressions", "clicks", "cost",
                    "purchases7d", "sales7d", "unitsSoldClicks7d",
                ],
                "reportTypeId": "spCampaigns",
                "timeUnit": "DAILY",
                "format": "GZIP_JSON",
            },
        }
        r = httpx.post(
            f"{self.ADS_BASE}/reporting/reports",
            headers=self._ads_headers(
                access, "application/vnd.createasyncreportrequest.v3+json"
            ),
            json=body,
            timeout=30.0,
        )
        r.raise_for_status()
        return r.json()["reportId"]

    def _poll_report(self, access: str, report_id: str, timeout: int = 60, interval: int = 3) -> str:
        deadline = time.time() + timeout
        while time.time() < deadline:
            r = httpx.get(
                f"{self.ADS_BASE}/reporting/reports/{report_id}",
                headers=self._ads_headers(access),
                timeout=30.0,
            )
            r.raise_for_status()
            j = r.json()
            status = j.get("status")
            if status == "COMPLETED":
                return j["url"]
            if status == "FAILED":
                raise RuntimeError(f"Amazon report failed: {j.get('failureReason')}")
            time.sleep(interval)
        raise TimeoutError(f"Amazon report {report_id} not ready after {timeout}s")

    def _download_report(self, url: str, start: date, end: date) -> List[UnifiedRow]:
        r = httpx.get(url, timeout=60.0, follow_redirects=True)
        r.raise_for_status()
        try:
            raw = gzip.decompress(r.content)
        except OSError:
            raw = r.content
        records = json.loads(raw)
        rows: List[UnifiedRow] = []
        for rec in records:
            d_str = rec.get("date") or rec.get("reportDate")
            try:
                d = date.fromisoformat(d_str) if d_str else end
            except (TypeError, ValueError):
                d = end
            spend = float(rec.get("cost", 0) or 0)
            sales = float(rec.get("sales7d", 0) or 0)
            units = int(rec.get("unitsSoldClicks7d", 0) or 0)
            rows.append(UnifiedRow(
                date=d, rmn=self.RMN, retailer=self.RETAILER,
                campaign_id=str(rec.get("campaignId", "")),
                campaign_name=str(rec.get("campaignName", "")),
                sku="", product_name="",
                impressions=int(rec.get("impressions", 0) or 0),
                clicks=int(rec.get("clicks", 0) or 0),
                spend_eur=round(spend, 2),
                units_sold=units,
                sales_eur=round(sales, 2),
                new_to_brand_units=0,
            ))
        return rows

    def _fetch_mock(self, start: date, end: date) -> List[UnifiedRow]:
        rng = random.Random(self.seed)
        rows: List[UnifiedRow] = []
        for d in _daterange(start, end):
            for camp_id, camp_name, camp_type in AMAZON_CAMPAIGNS:
                for sku, pname, price in PRODUCT_CATALOG:
                    impressions = rng.randint(4500, 22000) if camp_type == "SB" else rng.randint(1800, 9000)
                    ctr = rng.uniform(0.008, 0.025)
                    clicks = max(1, int(impressions * ctr))
                    cpc = rng.uniform(0.35, 1.10)
                    spend = round(clicks * cpc, 2)
                    conv = rng.uniform(0.05, 0.18)
                    units = max(0, int(clicks * conv))
                    basket = rng.uniform(18.0, 45.0)
                    sales = round(units * max(basket, price * 0.6), 2)
                    ntb = int(units * rng.uniform(0.15, 0.40))
                    rows.append(UnifiedRow(
                        date=d, rmn=self.RMN, retailer=self.RETAILER,
                        campaign_id=camp_id, campaign_name=camp_name,
                        sku=sku, product_name=pname,
                        impressions=impressions, clicks=clicks, spend_eur=spend,
                        units_sold=units, sales_eur=sales, new_to_brand_units=ntb,
                    ))
        return rows


class CriteoRetailMediaConnector:
    RMN = "Criteo Retail Media"
    RETAILERS = ["Cdiscount", "La Redoute", "LDLC"]

    def __init__(self, api_key: str | None = None, account_id: str | None = None, seed: int = 7):
        self.api_key = api_key or os.getenv("CRITEO_API_KEY")
        self.account_id = account_id or os.getenv("CRITEO_ACCOUNT_ID")
        self.seed = seed

    def has_real_credentials(self) -> bool:
        return bool(self.api_key and self.account_id)

    def fetch(self, start: date, end: date, mode: str = "auto") -> List[UnifiedRow]:
        if mode == "mock":
            return self._fetch_mock(start, end)
        if mode == "real":
            if not self.has_real_credentials():
                raise RuntimeError("Criteo: credentials manquants (CRITEO_API_KEY, CRITEO_ACCOUNT_ID).")
            return self._fetch_real(start, end)
        if self.has_real_credentials():
            return self._fetch_real(start, end)
        return self._fetch_mock(start, end)

    def _fetch_real(self, start: date, end: date) -> List[UnifiedRow]:
        raise NotImplementedError(
            "Appel réel au Criteo Retail Media API (OAuth2 client_credentials) non implémenté."
        )

    def _fetch_mock(self, start: date, end: date) -> List[UnifiedRow]:
        rng = random.Random(self.seed)
        rows: List[UnifiedRow] = []
        for d in _daterange(start, end):
            for retailer in self.RETAILERS:
                for camp_id, camp_name, camp_type in CRITEO_CAMPAIGNS:
                    for sku, pname, price in PRODUCT_CATALOG:
                        impressions = rng.randint(2000, 14000) if camp_type == "SB" else rng.randint(900, 5500)
                        ctr = rng.uniform(0.006, 0.018)
                        clicks = max(1, int(impressions * ctr))
                        cpc = rng.uniform(0.28, 0.65)
                        spend = round(clicks * cpc, 2)
                        conv = rng.uniform(0.05, 0.18)
                        units = max(0, int(clicks * conv))
                        basket = rng.uniform(18.0, 45.0)
                        sales = round(units * max(basket, price * 0.6), 2)
                        ntb = int(units * rng.uniform(0.10, 0.32))
                        rows.append(UnifiedRow(
                            date=d, rmn=self.RMN, retailer=retailer,
                            campaign_id=f"{camp_id}-{retailer[:3].lower()}",
                            campaign_name=camp_name,
                            sku=sku, product_name=pname,
                            impressions=impressions, clicks=clicks, spend_eur=spend,
                            units_sold=units, sales_eur=sales, new_to_brand_units=ntb,
                        ))
        return rows


UNLIMITAIL_CAMPAIGNS = [
    ("unl-ons-cuis-21", "Onsite_Carrefour_Cuisine", "ONSITE"),
    ("unl-off-disp-22", "Offsite_Display_Grocery", "OFFSITE"),
]


class UnlimitailConnector:
    RMN = "Unlimitail"
    RETAILERS = ["carrefour.fr", "monoprix.fr"]

    def __init__(self, api_key: str | None = None, account_id: str | None = None, seed: int = 19):
        self.api_key = api_key or os.getenv("UNLIMITAIL_API_KEY")
        self.account_id = account_id or os.getenv("UNLIMITAIL_ACCOUNT_ID")
        self.seed = seed

    def has_real_credentials(self) -> bool:
        return bool(self.api_key and self.account_id)

    def fetch(self, start: date, end: date, mode: str = "auto") -> List[UnifiedRow]:
        if mode == "real":
            return self._fetch_real(start, end)
        return self._fetch_mock(start, end)

    def _fetch_real(self, start: date, end: date) -> List[UnifiedRow]:
        raise NotImplementedError(
            "Connecteur Unlimitail (Carrefour Links API) non implémenté."
        )

    def _fetch_mock(self, start: date, end: date) -> List[UnifiedRow]:
        rng = random.Random(self.seed)
        rows: List[UnifiedRow] = []
        for d in _daterange(start, end):
            for retailer in self.RETAILERS:
                for camp_id, camp_name, camp_type in UNLIMITAIL_CAMPAIGNS:
                    for sku, pname, price in PRODUCT_CATALOG:
                        if rng.random() < 0.58:
                            continue
                        impressions = rng.randint(1500, 9000) if camp_type == "OFFSITE" else rng.randint(700, 3500)
                        ctr = rng.uniform(0.005, 0.012)
                        clicks = max(1, int(impressions * ctr))
                        cpc = rng.uniform(0.22, 0.50)
                        spend = round(clicks * cpc, 2)
                        conv = rng.uniform(0.02, 0.08)
                        units = max(0, int(clicks * conv))
                        basket = rng.uniform(16.0, 38.0)
                        sales = round(units * max(basket, price * 0.55), 2)
                        ntb = int(units * rng.uniform(0.08, 0.25))
                        rows.append(UnifiedRow(
                            date=d, rmn=self.RMN, retailer=retailer,
                            campaign_id=f"{camp_id}-{retailer.split('.')[0][:3]}",
                            campaign_name=camp_name,
                            sku=sku, product_name=pname,
                            impressions=impressions, clicks=clicks, spend_eur=spend,
                            units_sold=units, sales_eur=sales, new_to_brand_units=ntb,
                        ))
        return rows


def fetch_all(days: int = 14, mode: str = "auto") -> pd.DataFrame:
    """Fetch unifié. mode ∈ {"auto", "mock", "real"}."""
    end = date.today()
    start = end - timedelta(days=days - 1)
    rows: List[UnifiedRow] = []
    rows.extend(AmazonAdsConnector().fetch(start, end, mode=mode))
    rows.extend(CriteoRetailMediaConnector().fetch(start, end, mode=mode))
    if mode != "real":
        rows.extend(UnlimitailConnector().fetch(start, end, mode=mode))
    df = pd.DataFrame([asdict(r) for r in rows])
    if df.empty:
        df = pd.DataFrame(columns=[f.name for f in UnifiedRow.__dataclass_fields__.values()])
    return df


if __name__ == "__main__":
    df = fetch_all(days=14)
    print(f"Rows: {len(df)}")
    print(f"Période: {df['date'].min()} → {df['date'].max()}")
    print(f"Régies: {df['rmn'].unique().tolist()}")
    print(f"Retailers: {df['retailer'].unique().tolist()}")
    print(f"Produits: {df['product_name'].nunique()}")
    print(f"Spend total: {df['spend_eur'].sum():,.0f} EUR")
    print(f"Sales total: {df['sales_eur'].sum():,.0f} EUR")
    print(f"ROAS global: {df['sales_eur'].sum() / df['spend_eur'].sum():.2f}")
    print("\nAperçu:")
    print(df.head(3).to_string(index=False))
