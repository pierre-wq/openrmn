# openRMN

**Open source MVP — Retail Media Analytics augmentée par IA.**

openRMN agrège les données publicitaires de plusieurs régies retail media
(Amazon Ads, Criteo Retail Media, etc.) dans un schéma unifié, puis laisse un
agent IA (Claude Sonnet 4.5) produire des insights actionnables en français
pour les annonceurs.

## Pourquoi

Les annonceurs multi-régies perdent un temps fou à :
- consolider manuellement des exports hétérogènes (taxonomies, définitions de
  conversion, fenêtres d'attribution incompatibles),
- arbitrer les biais d'auto-attribution des régies (walled gardens),
- traduire des KPI bruts en décisions business.

openRMN propose une couche déterministe (schéma unifié + KPI + détection
d'anomalies + audit de neutralité) **augmentée** par un agent IA qui raisonne
sur ces artefacts et produit un brief exécutif en 3 sections.

## Quickstart

```bash
pip install pandas anthropic
export ANTHROPIC_API_KEY=sk-ant-...
python agent.py
```

Sans variables d'env `AMZ_PROFILE_ID` / `CRITEO_API_KEY`, les connecteurs
tournent en **mode mock** avec des données simulées réalistes (4 SKU café,
3 campagnes, 14 jours, 3 retailers Criteo).

Tests isolés :

```bash
python connectors.py   # vérifie l'ingestion mock
python agent.py        # pipeline complet (brief IA si ANTHROPIC_API_KEY)
```

## Architecture

```
 ┌──────────────────┐    ┌──────────────────┐
 │  Amazon Ads API  │    │  Criteo RM API   │   ← stubs `_fetch_real()`
 └────────┬─────────┘    └────────┬─────────┘
          │ (mock par défaut)      │
          ▼                        ▼
 ┌──────────────────────────────────────────┐
 │  Connecteurs → UnifiedRow (dataclass)    │
 │  date • rmn • retailer • campaign • sku  │
 │  impressions • clicks • spend • sales …  │
 └─────────────────────┬────────────────────┘
                       │ pandas.DataFrame
                       ▼
 ┌──────────────────────────────────────────┐
 │  Couche déterministe (agent.py)          │
 │   • compute_kpis()                       │
 │   • detect_anomalies()                   │
 │   • neutrality_audit()                   │
 └─────────────────────┬────────────────────┘
                       │ KPI + anomalies + audit
                       ▼
 ┌──────────────────────────────────────────┐
 │  Agent IA — Claude Sonnet 4.5            │
 │   system: analyste senior retail media   │
 │   output: brief exécutif FR              │
 │     1. Diagnostic                        │
 │     2. Anomalies & risques               │
 │     3. Recommandations                   │
 └──────────────────────────────────────────┘
```

## Schéma unifié `UnifiedRow`

| Champ                 | Type    | Description                          |
|-----------------------|---------|--------------------------------------|
| `date`                | date    | Jour de mesure                       |
| `rmn`                 | str     | Régie (Amazon Ads, Criteo RM, …)     |
| `retailer`            | str     | Enseigne (Amazon.fr, Cdiscount, …)   |
| `campaign_id`         | str     | ID natif régie                       |
| `campaign_name`       | str     | Nom de campagne (normalisé côté MVP) |
| `sku`                 | str     | Référence produit                    |
| `product_name`        | str     | Nom produit (clé de jointure cross-régie) |
| `impressions`         | int     |                                      |
| `clicks`              | int     |                                      |
| `spend_eur`           | float   |                                      |
| `units_sold`          | int     |                                      |
| `sales_eur`           | float   |                                      |
| `new_to_brand_units`  | int     | Unités vendues à nouveaux acheteurs  |

## Anomalies détectées

- **Cannibalisation potentielle** : même produit, ROAS très divergent entre
  Amazon et Criteo (ratio > 1.8 ou < 0.55).
- **Campagnes sous-performantes** : ROAS < 1.5 sur > 5 000 EUR de spend.

## Audit de neutralité

Comparaison des ventes attribuées par chaque régie sur les SKU communs.
Une part Amazon > 70% sur les ventes agrégées est un signal fort de
sur-attribution côté walled garden.

## Roadmap

- [ ] Connecteurs réels (OAuth Amazon Ads v3, Criteo Retail Media API)
- [ ] Normaliseur de taxonomies campagne (naming SP/SB/SD vs Criteo)
- [ ] Modèle d'attribution neutre (last-click cross-régie, halo offline)
- [ ] Dashboard Streamlit (KPI + conversational agent)
- [ ] Détection de saturation enchère (spend plateau vs sales plateau)
- [ ] Alertes programmées (cron + Slack)
- [ ] Ajout Walmart Connect, Carrefour Links, Leclerc Média

## Licence

MIT.
