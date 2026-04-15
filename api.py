"""openRMN — API FastAPI : analytics + OAuth Amazon + dashboard."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import httpx
import pandas as pd
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from sse_starlette.sse import EventSourceResponse

from connectors import fetch_all, AmazonAdsConnector, CriteoRetailMediaConnector, UnlimitailConnector
from agent import (
    compute_kpis, detect_anomalies, neutrality_audit, product_detail,
    apply_filters, build_catalog,
    PERSONA_PROMPTS, ASK_SYSTEM_PROMPT, build_brief_payload,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("openrmn")

ROOT_PATH = os.getenv("ROOT_PATH", "")
PUBLIC_BASE = os.getenv("PUBLIC_BASE", "https://lab.holco.co/retail-audience")
VERSION = "0.4.0"
CACHE_TTL_S = 300
BASE_DIR = Path(__file__).parent
STATIC_DIR = BASE_DIR / "static"
ENV_PATH = BASE_DIR / ".env"

# ─── Auth (password + signed cookie) ───────────────────────────────────────
OPENRMN_PASSWORD = os.getenv("OPENRMN_PASSWORD", "")
SESSION_SECRET = os.getenv("OPENRMN_SESSION_SECRET", secrets.token_urlsafe(32))
SESSION_COOKIE_NAME = "openrmn_session"
SESSION_TTL_S = 12 * 3600  # 12h
PUBLIC_PATHS = {
    "/login", "/api/login",
    "/api/health",
    "/api/amazon/oauth/start", "/api/amazon/oauth/callback",
}
PUBLIC_PREFIXES = ("/static/",)


def make_session_token() -> str:
    ts = str(int(time.time()))
    sig = hmac.new(SESSION_SECRET.encode(), ts.encode(), hashlib.sha256).hexdigest()
    return f"{ts}.{sig}"


def verify_session_token(token: str) -> bool:
    try:
        ts, sig = token.split(".", 1)
        expected = hmac.new(SESSION_SECRET.encode(), ts.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            return False
        return (time.time() - int(ts)) < SESSION_TTL_S
    except Exception:
        return False

app = FastAPI(
    title="openRMN",
    version=VERSION,
    root_path=ROOT_PATH,
    description="Retail Media Analytics API — Amazon Ads × Criteo",
)


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    # If no password configured → dev mode, open access.
    if not OPENRMN_PASSWORD:
        return await call_next(request)
    path = request.url.path
    if path in PUBLIC_PATHS or any(path.startswith(p) for p in PUBLIC_PREFIXES):
        return await call_next(request)
    token = request.cookies.get(SESSION_COOKIE_NAME, "")
    if verify_session_token(token):
        return await call_next(request)
    accept = request.headers.get("accept", "")
    if "text/html" in accept:
        return RedirectResponse(url=f"{ROOT_PATH}/login", status_code=302)
    return JSONResponse({"error": "unauthorized"}, status_code=401)


app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Cache par mode : "auto" | "mock" | "real"
_cache: Dict[str, Dict[str, Any]] = {
    m: {"df": None, "ts": 0.0, "days": 14} for m in ("auto", "mock", "real")
}
_oauth_state: Dict[str, float] = {}
_last_real_fetch: Dict[str, Any] = {"rows": None, "at": None, "error": None}


def get_df(days: int = 14, mode: str = "auto", force: bool = False) -> pd.DataFrame:
    mode = mode if mode in _cache else "auto"
    c = _cache[mode]
    now = time.time()
    if (
        not force
        and c["df"] is not None
        and c["days"] == days
        and (now - c["ts"]) < CACHE_TTL_S
    ):
        return c["df"]
    log.info("fetch (mode=%s, days=%d, force=%s)", mode, days, force)
    df = fetch_all(days=days, mode=mode)
    c["df"], c["ts"], c["days"] = df, now, days
    if mode == "real":
        _last_real_fetch.update({
            "rows": int(len(df)),
            "at": datetime.now(timezone.utc).isoformat(),
            "error": None,
        })
    return df


def df_to_records(df: pd.DataFrame) -> list:
    out = df.copy()
    if not out.empty:
        out["date"] = out["date"].astype(str)
    return out.to_dict(orient="records")


def update_env_file(updates: Dict[str, str]) -> None:
    """Update /root/openrmn/.env (replace or append). Atomic."""
    existing: Dict[str, str] = {}
    order: list = []
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text().splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                k, _, v = line.partition("=")
                k = k.strip()
                if k not in existing:
                    order.append(k)
                existing[k] = v
            else:
                order.append(("__raw__", line))
    for k, v in updates.items():
        if k not in existing:
            order.append(k)
        existing[k] = v
        os.environ[k] = v
    out_lines = []
    seen = set()
    for item in order:
        if isinstance(item, tuple) and item[0] == "__raw__":
            out_lines.append(item[1])
        elif item in existing and item not in seen:
            out_lines.append(f"{item}={existing[item]}")
            seen.add(item)
    for k, v in updates.items():
        if k not in seen:
            out_lines.append(f"{k}={v}")
            seen.add(k)
    tmp = ENV_PATH.with_suffix(".env.tmp")
    tmp.write_text("\n".join(out_lines) + "\n")
    tmp.replace(ENV_PATH)
    ENV_PATH.chmod(0o600)


# ─── Routes statiques ──────────────────────────────────────────────────────
@app.get("/")
def index():
    return FileResponse(
        str(STATIC_DIR / "index.html"),
        headers={"Cache-Control": "no-cache, must-revalidate"},
    )


@app.get("/api/health")
def health():
    return {"status": "ok", "version": VERSION}


# ─── Auth endpoints ────────────────────────────────────────────────────────
@app.get("/login")
def login_page():
    return FileResponse(
        str(STATIC_DIR / "login.html"),
        headers={"Cache-Control": "no-cache, must-revalidate"},
    )


@app.post("/api/login")
async def api_login(request: Request):
    body = await request.json()
    password = str(body.get("password", ""))
    if not OPENRMN_PASSWORD:
        return JSONResponse({"status": "ok", "note": "auth disabled"})
    if not hmac.compare_digest(password, OPENRMN_PASSWORD):
        await asyncio.sleep(0.5)  # anti brute-force
        return JSONResponse({"error": "invalid_password"}, status_code=401)
    resp = JSONResponse({"status": "ok"})
    resp.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=make_session_token(),
        max_age=SESSION_TTL_S,
        httponly=True,
        secure=True,
        samesite="lax",
        path=ROOT_PATH or "/",
    )
    return resp


# ─── Sources status ────────────────────────────────────────────────────────
@app.get("/api/sources/status")
def sources_status():
    amz = AmazonAdsConnector()
    crt = CriteoRetailMediaConnector()
    unl = UnlimitailConnector()
    return {
        "amazon": {
            "mode_available": "real" if amz.has_real_credentials() else "mock_only",
            "profile_id": amz.profile_id,
            "has_client_id": bool(amz.client_id),
            "has_refresh_token": bool(amz.refresh_token),
            "last_real_fetch_rows": _last_real_fetch["rows"],
            "last_real_fetch_at": _last_real_fetch["at"],
            "last_real_fetch_error": _last_real_fetch["error"],
        },
        "criteo": {
            "mode_available": "real" if crt.has_real_credentials() else "mock_only",
            "account_id": crt.account_id,
        },
        "unlimitail": {
            "mode_available": "real" if unl.has_real_credentials() else "mock_only",
            "account_id": unl.account_id,
        },
    }


# ─── OAuth Amazon LWA ──────────────────────────────────────────────────────
def _redirect_uri() -> str:
    return f"{PUBLIC_BASE.rstrip('/')}/api/amazon/oauth/callback"


@app.get("/api/amazon/oauth/start")
def amazon_oauth_start():
    client_id = os.getenv("AMZ_LWA_CLIENT_ID")
    if not client_id:
        raise HTTPException(500, "AMZ_LWA_CLIENT_ID n'est pas défini dans .env.")
    state = secrets.token_urlsafe(24)
    _oauth_state[state] = time.time()
    # purge > 10 min
    cutoff = time.time() - 600
    for k in [k for k, v in _oauth_state.items() if v < cutoff]:
        _oauth_state.pop(k, None)

    params = {
        "client_id": client_id,
        "scope": "advertising::campaign_management",
        "response_type": "code",
        "redirect_uri": _redirect_uri(),
        "state": state,
    }
    url = "https://www.amazon.com/ap/oa?" + urlencode(params)
    return RedirectResponse(url, status_code=302)


@app.get("/api/amazon/oauth/callback")
def amazon_oauth_callback(code: str = "", state: str = "", error: str = "", error_description: str = ""):
    if error:
        return HTMLResponse(
            f"<h2>❌ Amazon a refusé l'autorisation</h2><p>{error} : {error_description}</p>",
            status_code=400,
        )
    if not code or state not in _oauth_state:
        raise HTTPException(400, "State invalide ou code manquant (protection CSRF).")
    _oauth_state.pop(state, None)

    client_id = os.getenv("AMZ_LWA_CLIENT_ID")
    client_secret = os.getenv("AMZ_LWA_CLIENT_SECRET")
    if not (client_id and client_secret):
        raise HTTPException(500, "AMZ_LWA_CLIENT_ID / AMZ_LWA_CLIENT_SECRET manquants.")

    try:
        r = httpx.post(
            "https://api.amazon.com/auth/o2/token",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": _redirect_uri(),
                "client_id": client_id,
                "client_secret": client_secret,
            },
            timeout=30.0,
        )
        r.raise_for_status()
    except httpx.HTTPError as e:
        log.exception("token exchange failed")
        return HTMLResponse(f"<h2>❌ Échec de l'échange de token</h2><pre>{e}</pre>", status_code=502)

    tok = r.json()
    access_token = tok["access_token"]
    refresh_token = tok["refresh_token"]

    # Fetch EU profiles
    try:
        rp = httpx.get(
            "https://advertising-api-eu.amazon.com/v2/profiles",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Amazon-Advertising-API-ClientId": client_id,
            },
            timeout=30.0,
        )
        rp.raise_for_status()
        profiles = rp.json() or []
    except httpx.HTTPError as e:
        log.exception("profiles fetch failed")
        return HTMLResponse(f"<h2>❌ Récupération des profils échouée</h2><pre>{e}</pre>", status_code=502)

    if not profiles:
        return HTMLResponse(
            "<h2>⚠️ Refresh token obtenu mais aucun profil EU trouvé</h2>"
            "<p>Vérifie que le compte Amazon Ads dispose d'au moins un compte EU.</p>",
            status_code=200,
        )

    fr = [p for p in profiles if p.get("countryCode") == "FR"]
    chosen = (fr or profiles)[0]
    profile_id = str(chosen.get("profileId"))

    update_env_file({
        "AMZ_REFRESH_TOKEN": refresh_token,
        "AMZ_PROFILE_ID": profile_id,
    })
    # invalider le cache real
    _cache["real"]["df"] = None
    _cache["auto"]["df"] = None

    return HTMLResponse(f"""
    <html><head><meta charset=utf-8><title>openRMN — Amazon connecté</title>
    <style>body{{font-family:system-ui;max-width:560px;margin:60px auto;padding:0 24px;color:#111827}}
    .ok{{background:#d1fae5;color:#065f46;padding:18px 22px;border-radius:8px;border:1px solid #6ee7b7}}
    code{{background:#f3f4f6;padding:2px 6px;border-radius:4px}}
    a{{color:#2563eb}}</style></head>
    <body>
    <div class=ok>
      <h2 style='margin:0 0 8px'>✅ Amazon Ads connecté</h2>
      <p style='margin:0'>Profil sélectionné : <code>{profile_id}</code> ({chosen.get('countryCode')} · {chosen.get('accountInfo', {}).get('name', '')})</p>
      <p style='margin:8px 0 0;font-size:14px'>Refresh token enregistré dans <code>.env</code>. Vous pouvez fermer cette fenêtre.</p>
    </div>
    <p style='margin-top:20px'><a href='{ROOT_PATH or "/"}'>← Retour au dashboard</a></p>
    </body></html>
    """)


# ─── Endpoints data (avec mode) ────────────────────────────────────────────
def _wrap_real(fn, mode: str):
    try:
        return fn()
    except RuntimeError as e:
        if mode == "real":
            _last_real_fetch.update({"error": str(e), "at": datetime.now(timezone.utc).isoformat()})
        raise HTTPException(400, str(e))
    except Exception as e:
        if mode == "real":
            _last_real_fetch.update({"error": str(e), "at": datetime.now(timezone.utc).isoformat()})
        log.exception("fetch failed (mode=%s)", mode)
        raise HTTPException(502, f"Échec du fetch {mode} : {e}")


def _split_csv(value: Optional[str]) -> list:
    if not value:
        return []
    return [s for s in (p.strip() for p in value.split(",")) if s]


@app.get("/api/catalog")
def api_catalog(
    mode: str = Query("auto", pattern="^(auto|mock|real)$"),
    sources: Optional[str] = None,
):
    df = _wrap_real(lambda: get_df(mode=mode), mode)
    df = apply_filters(df, sources=_split_csv(sources))
    return build_catalog(df, period_days=_cache.get(mode, _cache["auto"]).get("days", 14))


@app.get("/api/kpis")
def api_kpis(
    mode: str = Query("auto", pattern="^(auto|mock|real)$"),
    products: Optional[str] = None,
    campaigns: Optional[str] = None,
    sources: Optional[str] = None,
):
    df = _wrap_real(lambda: get_df(mode=mode), mode)
    df = apply_filters(df, _split_csv(products), _split_csv(campaigns), _split_csv(sources))
    return compute_kpis(df)


@app.get("/api/anomalies")
def api_anomalies(
    mode: str = Query("auto", pattern="^(auto|mock|real)$"),
    products: Optional[str] = None,
    campaigns: Optional[str] = None,
    sources: Optional[str] = None,
):
    df = _wrap_real(lambda: get_df(mode=mode), mode)
    df = apply_filters(df, _split_csv(products), _split_csv(campaigns), _split_csv(sources))
    return detect_anomalies(df)


@app.get("/api/audit")
def api_audit(
    mode: str = Query("auto", pattern="^(auto|mock|real)$"),
    products: Optional[str] = None,
    campaigns: Optional[str] = None,
    sources: Optional[str] = None,
):
    df = _wrap_real(lambda: get_df(mode=mode), mode)
    df = apply_filters(df, _split_csv(products), _split_csv(campaigns), _split_csv(sources))
    return neutrality_audit(df)


@app.get("/api/product-detail")
def api_product_detail(
    product: str = Query(..., min_length=1),
    mode: str = Query("auto", pattern="^(auto|mock|real)$"),
    campaigns: Optional[str] = None,
    sources: Optional[str] = None,
):
    df = _wrap_real(lambda: get_df(mode=mode), mode)
    df = apply_filters(df, None, _split_csv(campaigns), _split_csv(sources))
    return product_detail(df, product)


@app.get("/api/raw")
def api_raw(
    days: int = 14,
    mode: str = Query("auto", pattern="^(auto|mock|real)$"),
    products: Optional[str] = None,
    campaigns: Optional[str] = None,
    sources: Optional[str] = None,
):
    days = max(1, min(days, 90))
    df = _wrap_real(lambda: get_df(days=days, mode=mode), mode)
    df = apply_filters(df, _split_csv(products), _split_csv(campaigns), _split_csv(sources))
    return df_to_records(df)


@app.get("/api/daily")
def api_daily(
    mode: str = Query("auto", pattern="^(auto|mock|real)$"),
    products: Optional[str] = None,
    campaigns: Optional[str] = None,
    sources: Optional[str] = None,
):
    df = _wrap_real(lambda: get_df(mode=mode), mode)
    df = apply_filters(df, _split_csv(products), _split_csv(campaigns), _split_csv(sources))
    if df.empty:
        return {"dates": [], "series": {}}
    grouped = df.groupby(["date", "rmn"])[["spend_eur", "sales_eur"]].sum().reset_index()
    grouped["roas"] = grouped["sales_eur"] / grouped["spend_eur"].replace(0, pd.NA)
    grouped["date"] = grouped["date"].astype(str)
    series: Dict[str, list] = {}
    dates = sorted(grouped["date"].unique().tolist())
    for rmn, sub in grouped.groupby("rmn"):
        sub_map = dict(zip(sub["date"], sub["roas"]))
        series[rmn] = [
            round(float(sub_map.get(d, 0)), 2) if not pd.isna(sub_map.get(d, 0)) else 0
            for d in dates
        ]
    return {"dates": dates, "series": series}


@app.post("/api/refresh")
def api_refresh(mode: str = Query("auto", pattern="^(auto|mock|real)$")):
    df = _wrap_real(lambda: get_df(mode=mode, force=True), mode)
    return {"status": "ok", "rows": len(df), "mode": mode}


async def _stream_claude(request: Request, system: str, user_payload: str):
    try:
        import anthropic
    except ImportError as e:
        yield {"event": "error", "data": json.dumps({"message": f"Librairie anthropic manquante : {e}"})}
        return
    if not os.getenv("ANTHROPIC_API_KEY"):
        yield {"event": "error", "data": json.dumps({"message": "ANTHROPIC_API_KEY non défini."})}
        return
    client = anthropic.Anthropic()
    try:
        with client.messages.stream(
            model="claude-sonnet-4-5",
            max_tokens=2000,
            system=system,
            messages=[{"role": "user", "content": user_payload}],
        ) as stream:
            for text in stream.text_stream:
                if await request.is_disconnected():
                    break
                yield {"event": "token", "data": json.dumps({"text": text})}
            yield {"event": "done", "data": "{}"}
    except Exception as e:
        log.exception("stream error")
        yield {"event": "error", "data": json.dumps({"message": str(e)})}


@app.post("/api/brief")
async def api_brief(
    request: Request,
    mode: str = Query("auto", pattern="^(auto|mock|real)$"),
    persona: str = Query("executive", pattern="^(executive|operational|neutrality)$"),
    products: Optional[str] = None,
    campaigns: Optional[str] = None,
    sources: Optional[str] = None,
):
    prod_list = _split_csv(products)
    camp_list = _split_csv(campaigns)
    src_list = _split_csv(sources)

    async def event_gen():
        try:
            df = get_df(mode=mode)
        except Exception as e:
            yield {"event": "error", "data": json.dumps({"message": f"Échec du fetch {mode} : {e}"})}
            return
        df = apply_filters(df, prod_list, camp_list, src_list)
        if df.empty:
            yield {"event": "error", "data": json.dumps({"message": f"Aucune donnée en mode {mode} pour la sélection — brief impossible."})}
            return
        system = PERSONA_PROMPTS.get(persona, PERSONA_PROMPTS["executive"])
        payload = build_brief_payload(df, "Produis le brief.", products=prod_list, campaigns=camp_list)
        async for evt in _stream_claude(request, system, payload):
            yield evt

    return EventSourceResponse(event_gen())


@app.post("/api/ask")
async def api_ask(request: Request):
    body = await request.json()
    question = (body.get("question") or "").strip()
    mode = body.get("mode", "auto")
    if mode not in ("auto", "mock", "real"):
        mode = "auto"
    raw_products = body.get("products") or []
    raw_campaigns = body.get("campaigns") or []
    raw_sources = body.get("sources") or []
    if isinstance(raw_products, str):
        raw_products = _split_csv(raw_products)
    if isinstance(raw_campaigns, str):
        raw_campaigns = _split_csv(raw_campaigns)
    if isinstance(raw_sources, str):
        raw_sources = _split_csv(raw_sources)
    prod_list = [str(p).strip() for p in raw_products if str(p).strip()]
    camp_list = [str(c).strip() for c in raw_campaigns if str(c).strip()]
    src_list = [str(s).strip() for s in raw_sources if str(s).strip()]
    if not question:
        raise HTTPException(400, "Le champ 'question' est requis.")

    async def event_gen():
        try:
            df = get_df(mode=mode)
        except Exception as e:
            yield {"event": "error", "data": json.dumps({"message": f"Échec du fetch {mode} : {e}"})}
            return
        df = apply_filters(df, prod_list, camp_list, src_list)
        payload = build_brief_payload(
            df, f"### Question de l'utilisateur\n{question}\n\nRéponds.",
            products=prod_list, campaigns=camp_list,
        )
        async for evt in _stream_claude(request, ASK_SYSTEM_PROMPT, payload):
            yield evt

    return EventSourceResponse(event_gen())
