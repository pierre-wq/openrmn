# openRMN

> The independent retail media analytics layer.
> Connects to Amazon Ads, Criteo and Unlimitail APIs, normalizes data,
> and uses AI to reveal what each walled garden hides.

🔗 Live demo : https://lab.holco.co/retail-audience

## Why openRMN ?

Retail media budgets are fragmenting across an ever-growing list of walled
gardens — Amazon Ads, Criteo Retail Media, Unlimitail, Walmart Connect,
Carrefour Links and more. Each network reports its own metrics, attributes
sales with its own methodology, and double-counts conversions its peers also
claim. Advertisers end up with contradictory dashboards and no neutral
arbitrator to reconcile them. In Skai's 2026 state-of-retail-media survey,
**75% of advertisers cite incrementality measurement as their #1 challenge**.
openRMN is the third-party layer that consolidates, normalizes and audits
those self-reported figures so the buyer — not the seller — owns the truth.

## Features (v0.5)

- Multi-RMN connectors : Amazon Ads (real + mock), Criteo Retail Media (mock), Unlimitail (mock)
- Unified schema (`UnifiedRow`) cross-RMN
- Deterministic analytics layer : KPIs, anomaly detection, neutrality audit
- **Trust Score per network** — auditable 0-100 score with 4 weighted components
- **Methodology comparison** — declared attribution windows, view-through, MRC certification
- **Harmonization simulator** — "what if all networks used the same methodology?"
- **Double-counting audit** — estimate cross-network over-attribution with a pure-SVG Sankey
- AI agent (Claude Sonnet 4.5) with 3 personas : Executive / Operational / Auditor
- Free-form Q&A on your data
- Web dashboard : narrative 4-act experience, Mock/Real toggle, OAuth Amazon
- Per-product drill-down with cross-network attribution comparison

## Methodology

> Open methodology — every score is auditable in [`agent.py`](./agent.py).

### Trust Score (0-100, weighted)

```
score = 0.30 · internal_consistency
      + 0.25 · cross_network_convergence
      + 0.25 · methodology_transparency
      + 0.20 · data_freshness
```

- **internal_consistency** = `clamp(100 − CV(ROAS_daily) × 100, 0, 100)` — stability
  of the network's own reported ROAS over the period. High variance = low score.
- **cross_network_convergence** = `100 × Σ(1 − |share_i − 1/N| / (1/N)) · total_i  /  Σ total_i`
  on SKUs common to ≥ 2 networks. A network that over-attributes vs. peers scores low.
- **methodology_transparency** = static score from public disclosure (Amazon=90, Criteo=70,
  Unlimitail=60 by default).
- **data_freshness** = 100 if ingested <24h ago, linearly decays to 0 at 7+ days.

Grades : A+ (>90) · A (>85) · B+ (>75) · B (>65) · C+ (>55) · C (>45) · D.

→ Source : [`trust_score()`](./agent.py).

### Harmonization simulator

Apply a deduplication coefficient per network based on declared attribution windows :

```
coef = sqrt(target_window / actual_window)
sales_harmonized = sales_declared × coef
```

Heuristic : attributed sales grow roughly as the square root of the attribution
window (defensible approximation, to be validated against panel data).

→ Source : [`simulate_harmonization()`](./agent.py).

### Double-counting audit

For each SKU common to ≥ 2 networks :

```
total_attributed = Σ sales_per_rmn
estimated_real   = max(sales_per_rmn) × 1.1
overlap          = max(0, total_attributed − estimated_real)
```

Per-network allocation is proportional :

```
real_rmn    = sales_rmn × (estimated_real / total_attributed)
overlap_rmn = sales_rmn − real_rmn
```

Hypothesis : the most declarative network is closest to ground truth, +10% covers
uncaptured organic sales. To be validated with third-party panel data (Wakoopa,
Nielsen, Kantar Worldpanel).

→ Source : [`double_counting_audit()`](./agent.py).

## Architecture

```
 ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
 │  Amazon Ads API  │    │  Criteo RM API   │    │   Unlimitail     │
 └────────┬─────────┘    └────────┬─────────┘    └────────┬─────────┘
          │ (real + mock)         │ (mock)                │ (mock)
          ▼                       ▼                       ▼
 ┌─────────────────────────────────────────────────────────────────┐
 │  Connectors → UnifiedRow (dataclass)                            │
 │  date • rmn • retailer • campaign • sku • product_name          │
 │  impressions • clicks • spend_eur • units_sold • sales_eur • …  │
 └───────────────────────────────┬─────────────────────────────────┘
                                 │ pandas.DataFrame
                                 ▼
 ┌─────────────────────────────────────────────────────────────────┐
 │  Deterministic layer (agent.py)                                 │
 │   • compute_kpis()        • detect_anomalies()                  │
 │   • neutrality_audit()    • product_detail()                    │
 └───────────────────────────────┬─────────────────────────────────┘
                                 │ KPI + anomalies + audit
                                 ▼
 ┌─────────────────────────────────────────────────────────────────┐
 │  AI agent — Claude Sonnet 4.5 (SSE-streamed)                    │
 │   personas: executive · operational · neutrality                │
 │   free-form Q&A on the grounded data                            │
 └───────────────────────────────┬─────────────────────────────────┘
                                 │
                                 ▼
 ┌─────────────────────────────────────────────────────────────────┐
 │  FastAPI (api.py) + static dashboard (4-act narrative UI)       │
 └─────────────────────────────────────────────────────────────────┘
```

## Quickstart

### Local dev with mock data

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export ANTHROPIC_API_KEY=sk-ant-...
uvicorn api:app --reload
# Open http://localhost:8000
```

### Production (behind reverse proxy)

```bash
ROOT_PATH=/retail-audience uvicorn api:app --host 127.0.0.1 --port 8000
```

### Connecting your real Amazon Ads account

1. Get LWA credentials from https://developer.amazon.com/loginwithamazon
2. Add to `.env` :
   ```
   AMZ_LWA_CLIENT_ID=...
   AMZ_LWA_CLIENT_SECRET=...
   ```
3. Whitelist the redirect URI in your LWA security profile :
   `https://your-domain.com/api/amazon/oauth/callback`
4. Visit `/api/amazon/oauth/start` in your browser → authorize → done.
   The refresh token and EU profile ID are persisted to `.env`.

## API endpoints

All endpoints accept a `mode` query param : `auto` (default) · `mock` · `real`.
Most also accept optional `products=a,b,c` and `campaigns=id1,id2` filters.

```bash
# Health
curl https://lab.holco.co/retail-audience/api/health

# Consolidated KPIs (ROAS unifié, spend, sales, breakdown par régie)
curl "https://lab.holco.co/retail-audience/api/kpis?mode=mock"

# Anomalies détectées automatiquement
curl "https://lab.holco.co/retail-audience/api/anomalies?mode=mock"

# Audit de neutralité (parts d'attribution par régie sur SKU communs)
curl "https://lab.holco.co/retail-audience/api/audit?mode=mock"

# Trust Score par régie (composants + grade + key_findings)
curl "https://lab.holco.co/retail-audience/api/trust-score?mode=mock"

# Méthodologies déclarées par les régies
curl "https://lab.holco.co/retail-audience/api/methodology-comparison"

# Simulateur d'harmonisation (what-if sur fenêtre d'attribution)
curl "https://lab.holco.co/retail-audience/api/harmonization-simulator?window=7&type=last-click&mode=mock"

# Audit du double-comptage (flux Sankey)
curl "https://lab.holco.co/retail-audience/api/double-counting?mode=mock"

# Catalogue (produits + campagnes pour la sélection)
curl "https://lab.holco.co/retail-audience/api/catalog?mode=mock"

# Drill-down par produit
curl "https://lab.holco.co/retail-audience/api/product-detail?product=Capsules%20Origine%20Colombie&mode=mock"

# Raw UnifiedRows (14 jours par défaut, cap 90)
curl "https://lab.holco.co/retail-audience/api/raw?mode=mock&days=14"

# Série journalière ROAS par régie
curl "https://lab.holco.co/retail-audience/api/daily?mode=mock"

# État des connecteurs (mode_available par RMN)
curl "https://lab.holco.co/retail-audience/api/sources/status"

# Invalider le cache (force refetch)
curl -X POST "https://lab.holco.co/retail-audience/api/refresh?mode=real"

# Brief IA streamé (SSE, persona ∈ {executive, operational, neutrality})
curl -N -X POST "https://lab.holco.co/retail-audience/api/brief?persona=executive&mode=mock"

# Q&A libre streamé (SSE)
curl -N -X POST https://lab.holco.co/retail-audience/api/ask \
  -H 'Content-Type: application/json' \
  -d '{"question":"Pourquoi Criteo sur-attribue sur les capsules ?","mode":"mock"}'
```

## Roadmap

- [ ] Real Criteo Retail Media connector (OAuth client_credentials)
- [ ] Real Unlimitail connector
- [ ] Walmart Connect, Carrefour Links, Mirakl Ads, Leclerc Média
- [ ] Geo-holdout incrementality testing
- [ ] Third-party panel data integration (e.g. Wakoopa) for neutrality audit
- [ ] Streamlit / Next.js production-grade UI
- [ ] Self-hosted deployment via Docker Compose

## License

MIT — contributions welcome.

## Contributing

Fork the repo, open a pull request against `main`, and describe your change.
Bug reports and feature requests go in GitHub Issues. By contributing you agree
to keep discussions professional and constructive.
