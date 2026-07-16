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

openRMN attacks the *first* half of that problem: de-duplicating and
stress-testing self-reported attribution across networks. It is **not** an
incrementality engine on its own — true incrementality needs a controlled test
(geo-holdout, on the roadmap) or a third-party panel. openRMN tells you which
declared numbers to distrust and by how much; it does not yet prove causal lift.

## Features (v0.7)

- Multi-RMN connectors : Amazon Ads (real + mock), Criteo Retail Media (mock), Unlimitail (mock)
- Unified schema (`UnifiedRow`) cross-RMN
- Deterministic analytics layer : KPIs, anomaly detection, neutrality audit
- **Trust Score per network** — auditable 0-100 score with 4 weighted components
- **Methodology comparison** — declared attribution windows, view-through, MRC certification
- **Harmonization simulator** — "what if all networks used the same methodology?"
- **Double-counting audit** — estimate cross-network over-attribution with a pure-SVG Sankey
- AI agent (Claude Sonnet 4.5) with 3 personas : Executive / Operational / Auditor
- **MCP server** — expose all analytics as tools for Claude Desktop, Claude Code, Cursor, any MCP-compatible AI agent
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
- **cross_network_convergence** = sales-weighted `100 × Σ(1 − |sales_share − clicks_share|) · total_i / Σ total_i`
  on SKUs common to ≥ 2 networks. Each network's share of attributed sales is compared to its
  share of clicks (a proxy for real exposure), **not** to an arbitrary `1/N` "fair share" — so a
  network that genuinely performs better is not penalised, only one claiming far more sales-share
  than its click-share is.
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

For each SKU common to ≥ 2 networks, real deduplicated sales are **bounded**, not
guessed with false precision :

```
lower_bound (max overlap)  = max(sales_per_rmn)   # peers fully double-count the leader
upper_bound (zero overlap) = Σ sales_per_rmn      # every attribution is incremental
point_estimate             = clamp(max(sales_per_rmn) × (1 + organic_uplift), lower, upper)
```

We report the **range** (`estimated_real_low` / `estimated_real_high`) and both a
point (`overlap_amount`) and worst-case (`overlap_amount_max`) over-attribution.
`organic_uplift` defaults to `0.10`. Per-network allocation is proportional :

```
real_rmn    = sales_rmn × (total_real / total_attributed)
overlap_rmn = sales_rmn − real_rmn
```

Hypothesis : the most declarative network is closest to ground truth, `+organic_uplift`
covers sales influenced without a logged interaction. This is an **explicit, auditable
assumption, not a measurement** — the true figure lives somewhere in the reported range.
Point-level ground truth requires third-party panel data (Wakoopa, NielsenIQ, Kantar
Worldpanel). The same estimator backs the per-product drill-down, so figures reconcile
across the app.

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

# Consolidated KPIs (unified ROAS, spend, sales, per-network breakdown)
curl "https://lab.holco.co/retail-audience/api/kpis?mode=mock"

# Auto-detected anomalies
curl "https://lab.holco.co/retail-audience/api/anomalies?mode=mock"

# Neutrality audit (per-network attribution share on shared SKUs)
curl "https://lab.holco.co/retail-audience/api/audit?mode=mock"

# Trust Score per network (components + grade + key_findings)
curl "https://lab.holco.co/retail-audience/api/trust-score?mode=mock"

# Network-declared methodologies
curl "https://lab.holco.co/retail-audience/api/methodology-comparison"

# Harmonization simulator (what-if on attribution window)
curl "https://lab.holco.co/retail-audience/api/harmonization-simulator?window=7&type=last-click&mode=mock"

# Double-counting audit (Sankey flow)
curl "https://lab.holco.co/retail-audience/api/double-counting?mode=mock"

# Catalog (products + campaigns for selection)
curl "https://lab.holco.co/retail-audience/api/catalog?mode=mock"

# Per-product drill-down
curl "https://lab.holco.co/retail-audience/api/product-detail?product=Capsules%20Origine%20Colombie&mode=mock"

# Raw UnifiedRows (14 days default, cap 90)
curl "https://lab.holco.co/retail-audience/api/raw?mode=mock&days=14"

# Daily ROAS time series per network
curl "https://lab.holco.co/retail-audience/api/daily?mode=mock"

# Connector status (mode_available per RMN)
curl "https://lab.holco.co/retail-audience/api/sources/status"

# Invalidate cache (force refetch)
curl -X POST "https://lab.holco.co/retail-audience/api/refresh?mode=real"

# AI brief, streamed (SSE, persona ∈ {executive, operational, neutrality})
curl -N -X POST "https://lab.holco.co/retail-audience/api/brief?persona=executive&mode=mock"

# Free-form Q&A, streamed (SSE)
curl -N -X POST https://lab.holco.co/retail-audience/api/ask \
  -H 'Content-Type: application/json' \
  -d '{"question":"Why is Criteo over-attributing on the capsules?","mode":"mock"}'
```

## MCP Server (for AI agents)

openRMN exposes its analytics layer as a Model Context Protocol (MCP) 
server. Any MCP-compatible AI agent — Claude Desktop, Claude Code, Cursor, 
custom agents — can query retail media metrics in natural language.

### Use from Claude Desktop

Add to your `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "openrmn": {
      "command": "python",
      "args": ["/absolute/path/to/openrmn/mcp_server.py"]
    }
  }
}
```

Restart Claude Desktop. You can now ask questions like:

- "What's my unified ROAS across Amazon, Criteo and Unlimitail?"
- "Which product has the highest over-attribution?"
- "Show me the Trust Score for each network and explain the weakest one."
- "Simulate harmonizing everything to 30-day attribution."

### Available tools

| Tool | Purpose |
|------|---------|
| `get_kpis` | Unified ROAS and per-network breakdown |
| `get_anomalies` | Cannibalization and underperformance detection |
| `get_trust_score` | 0-100 scoring per network, 4 components |
| `get_double_counting_audit` | Over-attribution estimation |
| `simulate_harmonization` | What-if attribution alignment |
| `get_neutrality_audit` | Per-product attribution share comparison |
| `get_methodology_comparison` | Side-by-side methodology table |

### Use from Python (programmatic)

```python
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

async with stdio_client(StdioServerParameters(
    command="python",
    args=["mcp_server.py"],
)) as (read, write):
    async with ClientSession(read, write) as session:
        await session.initialize()
        result = await session.call_tool("get_trust_score", {"days": 14})
        print(result.content[0].text)
```

### Alignment with AdCP

The openRMN MCP server is designed to be composable with emerging standards 
like the [Ad Context Protocol (AdCP)](https://adcontextprotocol.org). 
The current `get_media_buy_delivery` concept in AdCP's Media Buy Protocol 
is the closest analog — openRMN provides the independent, cross-network 
audit layer that `get_media_buy_delivery` on its own does not cover.

We're actively following AdCP development and are open to contributing 
a Measurement extension. Join the conversation in our 
[GitHub discussions](https://github.com/holco-apps/openrmn/discussions).

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
