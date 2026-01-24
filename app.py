from __future__ import annotations

import io
import math
import os
import re
import secrets
import time
import uuid
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Tuple
from urllib.parse import urlencode

import pandas as pd
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from openpyxl import Workbook, load_workbook
from pydantic import BaseModel
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.sessions import SessionMiddleware

load_dotenv()

# =========================
# ENV / OAUTH
# =========================
OAUTH_CLIENT_ID = os.getenv("OAUTH_CLIENT_ID", "")
OAUTH_CLIENT_SECRET = os.getenv("OAUTH_CLIENT_SECRET", "")
OAUTH_REDIRECT_URI = os.getenv("OAUTH_REDIRECT_URI", "")
OAUTH_AUTH_URL = os.getenv("OAUTH_AUTH_URL", "")
OAUTH_TOKEN_URL = os.getenv("OAUTH_TOKEN_URL", "")
OAUTH_SCOPES = os.getenv("OAUTH_SCOPES", "journal_voucher_save sales_invoice_save sales_receipt_save")

# =========================
# CONFIG
# =========================
ACCURATE_ACCOUNT_BASE_URL = os.getenv("ACCURATE_ACCOUNT_BASE_URL", "https://account.accurate.id").rstrip("/")
OPEN_DB_PATH = os.getenv("OPEN_DB_PATH", "/api/open-db.do")
DB_LIST_PATH = os.getenv("DB_LIST_PATH", "/api/db-list.do")

JOURNAL_BULK_SAVE_PATH = os.getenv("JOURNAL_BULK_SAVE_PATH", "/accurate/api/journal-voucher/bulk-save.do")
SALES_INVOICE_BULK_SAVE_PATH = os.getenv("SALES_INVOICE_BULK_SAVE_PATH", "/accurate/api/sales-invoice/bulk-save.do")
SALES_RECEIPT_BULK_SAVE_PATH = os.getenv("SALES_RECEIPT_BULK_SAVE_PATH", "/accurate/api/sales-receipt/bulk-save.do")

TIMEOUT_SEC = int(os.getenv("TIMEOUT_SEC", "60"))
SESSION_SECRET = os.getenv("SESSION_SECRET", "CHANGE_ME_SUPER_SECRET")

# =========================
# ACCESS CODE / TENANT
# =========================
DEFAULT_ACCESS_CODE = os.getenv("DEFAULT_ACCESS_CODE", "DEMO-ACA-001").strip()
TENANT_BASE_DOMAIN = os.getenv("TENANT_BASE_DOMAIN", "aca-aol.id").strip().lower()
TENANT_ACCESS_CODES = os.getenv("TENANT_ACCESS_CODES", "").strip()

# Cookie/session (PROD) - set COOKIE_DOMAIN=".aca-aol.id"
COOKIE_DOMAIN = os.getenv("COOKIE_DOMAIN", "").strip()
HTTPS_ONLY = os.getenv("HTTPS_ONLY", "true").lower() == "true"

# =========================
# APP SETUP
# =========================
app = FastAPI(title="Accurate Importer (SaaS)")
templates = Jinja2Templates(directory="templates")

# static (jangan crash kalau folder tidak ada)
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# In-memory job store (MVP)
JOBS: Dict[str, Dict[str, Any]] = {}

# fallback store
COMPANY_STORE = {"items": [{"id": "1161648", "name": "ACIS"}]}

# OAuth store (MVP single token)
OAUTH_STORE = {"state": None, "token": None, "token_expiry": 0}


# =========================
# TENANT HELPERS
# =========================
def _parse_tenant_codes() -> Dict[str, str]:
    """
    TENANT_ACCESS_CODES="acis:KODE1;demo:KODE2"
    Return: dict slug -> CODE_UPPER
    """
    out: Dict[str, str] = {}
    if not TENANT_ACCESS_CODES:
        return out
    parts = [p.strip() for p in TENANT_ACCESS_CODES.split(";") if p.strip()]
    for p in parts:
        if ":" not in p:
            continue
        slug, code = p.split(":", 1)
        slug = slug.strip().lower()
        code = code.strip().upper()
        if slug and code:
            out[slug] = code
    return out


TENANT_CODES = _parse_tenant_codes()


def get_host(request: Request) -> str:
    host = (request.headers.get("x-forwarded-host") or request.headers.get("host") or "").lower()
    return host.split(":")[0].strip()


def get_tenant_slug_from_request(request: Request) -> str:
    """
    Rules:
    - localhost / railway => default
    - aca-aol.id / www.aca-aol.id => default
    - {tenant}.aca-aol.id => tenant
    """
    host = get_host(request)
    if not host:
        return "default"

    if host in ("localhost", "127.0.0.1") or host.endswith(".railway.app"):
        return "default"

    if host in (TENANT_BASE_DOMAIN, f"www.{TENANT_BASE_DOMAIN}"):
        return "default"

    # subdomain tenant
    if TENANT_BASE_DOMAIN and host.endswith(TENANT_BASE_DOMAIN):
        left = host[: -len(TENANT_BASE_DOMAIN)].rstrip(".")
        slug = (left.split(".")[0] if left else "").strip().lower()
        if slug in ("", "www"):
            return "default"
        return slug

    return "default"


def is_public_path(path: str) -> bool:
    if path in ("/", "/favicon.ico", "/app"):
        return True
    if path.startswith("/oauth/"):
        return True
    if path.startswith("/auth/"):
        return True
    if path.startswith("/templates/"):
        return True
    if path.startswith("/static/"):
        return True
    if path.startswith("/health"):
        return True
    return False


def is_protected_path(path: str) -> bool:
    return path.startswith("/api/") or path.startswith("/accurate/")


# =========================
# SESSION MIDDLEWARE
# =========================
app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    same_site="lax",
    https_only=HTTPS_ONLY,
    domain=COOKIE_DOMAIN or None,
)


# =========================
# FORCE WWW (aca-aol.id -> www.aca-aol.id)
# =========================
@app.middleware("http")
async def force_www_redirect(request: Request, call_next):
    host = request.headers.get("host", "")
    # kalau ada port, buang
    host = host.split(":")[0].lower().strip()

    if host == TENANT_BASE_DOMAIN:
        url = request.url.replace(netloc=f"www.{TENANT_BASE_DOMAIN}")
        return RedirectResponse(str(url), status_code=308)  # aman untuk POST/redirect
    return await call_next(request)


# =========================
# ACCESS GATE MIDDLEWARE (protect API)
# =========================
class AccessGateMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        if is_public_path(path):
            return await call_next(request)

        if is_protected_path(path):
            if not request.session.get("access_ok"):
                return JSONResponse({"ok": False, "error": "Unauthorized. Masukkan kode akses dulu."}, status_code=401)

            tenant = get_tenant_slug_from_request(request)
            sess_tenant = (request.session.get("tenant") or "default").lower()
            if tenant != sess_tenant:
                return JSONResponse({"ok": False, "error": "Tenant session tidak cocok. Silakan login ulang."}, status_code=401)

        return await call_next(request)


app.add_middleware(AccessGateMiddleware)


# =========================
# AUTH ROUTES (ACCESS CODE)
# =========================
class VerifyAccessBody(BaseModel):
    code: str


@app.get("/auth/status")
def auth_status(request: Request):
    tenant = get_tenant_slug_from_request(request)

    configured = True
    if tenant != "default" and TENANT_CODES:
        configured = tenant in TENANT_CODES

    return {
        "ok": True,
        "tenant": tenant,
        "configured": configured,
        "access_ok": bool(request.session.get("access_ok")) and (request.session.get("tenant") == tenant),
        "host": get_host(request),
    }


@app.post("/auth/verify")
def auth_verify(request: Request, body: VerifyAccessBody):
    tenant = get_tenant_slug_from_request(request)
    code = (body.code or "").strip().upper()
    if not code:
        return JSONResponse({"ok": False, "error": "Kode akses kosong."}, status_code=400)

    if tenant == "default":
        if not secrets.compare_digest(code, DEFAULT_ACCESS_CODE.upper()):
            return JSONResponse({"ok": False, "error": "Kode akses salah."}, status_code=401)
    else:
        expected = TENANT_CODES.get(tenant)
        if not expected:
            return JSONResponse({"ok": False, "error": f"Tenant '{tenant}' belum terdaftar."}, status_code=403)
        if not secrets.compare_digest(code, expected):
            return JSONResponse({"ok": False, "error": "Kode akses salah."}, status_code=401)

    request.session["access_ok"] = True
    request.session["tenant"] = tenant
    request.session["access_at"] = int(time.time())

    return {"ok": True, "tenant": tenant}


@app.post("/auth/logout")
def auth_logout(request: Request):
    for k in ["access_ok", "tenant", "access_at", "x_session_id", "accurate_api_host", "selected_company_id"]:
        request.session.pop(k, None)

    OAUTH_STORE["token"] = None
    OAUTH_STORE["token_expiry"] = 0
    OAUTH_STORE["state"] = None

    return {"ok": True}


# =========================
# UI ROUTES
# =========================
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    if request.session.get("access_ok"):
        return RedirectResponse("/app", status_code=302)
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/app", response_class=HTMLResponse)
def app_home(request: Request):
    if not request.session.get("access_ok"):
        return RedirectResponse("/", status_code=302)

    # biar kalau app.html belum ada, errornya jelas
    try:
        return templates.TemplateResponse("app.html", {"request": request})
    except Exception as e:
        return HTMLResponse(f"Template app.html belum ada / error render: {e}", status_code=500)


# =========================
# OAUTH HELPERS
# =========================
def oauth_build_auth_url() -> str:
    state = secrets.token_urlsafe(24)
    OAUTH_STORE["state"] = state
    params = {
        "response_type": "code",
        "client_id": OAUTH_CLIENT_ID,
        "redirect_uri": OAUTH_REDIRECT_URI,
        "scope": OAUTH_SCOPES,
        "state": state,
    }
    return f"{OAUTH_AUTH_URL}?{urlencode(params)}"


def oauth_exchange_code(code: str) -> dict:
    data = {"grant_type": "authorization_code", "code": code, "redirect_uri": OAUTH_REDIRECT_URI}
    r = requests.post(
        OAUTH_TOKEN_URL,
        data=data,
        auth=(OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET),
        timeout=30,
    )
    try:
        r.raise_for_status()
    except Exception:
        raise RuntimeError(f"{r.status_code} {r.text}")

    token = r.json()
    expires_in = int(token.get("expires_in", 3600))
    OAUTH_STORE["token"] = token
    OAUTH_STORE["token_expiry"] = int(time.time()) + max(60, expires_in - 60)
    return token


def get_access_token() -> str:
    token = OAUTH_STORE.get("token") or {}
    access = token.get("access_token")
    if not access:
        raise RuntimeError("Belum login OAuth / access_token belum ada. Klik Login Accurate dulu.")
    return access


def fetch_companies_from_accurate() -> List[Dict[str, str]]:
    access = get_access_token()
    url = f"{ACCURATE_ACCOUNT_BASE_URL}{DB_LIST_PATH}"

    r = requests.get(url, headers={"Authorization": f"Bearer {access}", "Accept": "application/json"}, timeout=30)
    ct = (r.headers.get("content-type") or "").lower()
    if "application/json" not in ct:
        snippet = (r.text or "")[:300].replace("\n", " ").replace("\r", " ")
        raise RuntimeError(f"db-list non-JSON: status={r.status_code}, ct={ct}, body={snippet}")

    data = r.json()
    if data.get("s") is False:
        raise RuntimeError(f"db-list gagal: {data}")

    out: List[Dict[str, str]] = []
    for it in (data.get("d") or []):
        cid = str(it.get("id", "")).strip()
        name = str(it.get("alias") or it.get("name") or f"DB-{cid}").strip()
        if cid:
            out.append({"id": cid, "name": name})
    return out


def open_db_get_session_and_host(request: Request) -> Tuple[str, str]:
    access = get_access_token()
    cid = request.session.get("selected_company_id")
    if not cid:
        raise RuntimeError("Belum pilih Data Usaha. Pilih dulu di dropdown.")

    url = f"{ACCURATE_ACCOUNT_BASE_URL}{OPEN_DB_PATH}"
    params = {"id": cid}

    r = requests.post(
        url,
        params=params,
        headers={"Authorization": f"Bearer {access}", "Accept": "application/json"},
        timeout=30,
        allow_redirects=False,
    )

    ct = (r.headers.get("content-type") or "").lower()
    if "application/json" not in ct:
        snippet = (r.text or "")[:300].replace("\n", " ").replace("\r", " ")
        raise RuntimeError(f"open-db non-JSON: status={r.status_code}, ct={ct}, body={snippet}")

    data = r.json()
    if data.get("s") is False:
        raise RuntimeError(f"open-db gagal: {data}")

    sid = data.get("session") or data.get("sessionId") or data.get("xSessionId") or data.get("d")
    if not sid or isinstance(sid, (list, dict)):
        raise RuntimeError(f"Tidak ada session di response open-db: {data}")

    host = data.get("host") or data.get("h")
    if not host or not isinstance(host, str):
        raise RuntimeError(f"Tidak ada host di response open-db: {data}")

    host = host.rstrip("/")
    request.session["accurate_api_host"] = host
    request.session["x_session_id"] = sid
    return sid, host


def get_cached_session_and_host(request: Request) -> Tuple[str, str]:
    sid = (request.session.get("x_session_id") or "").strip()
    host = (request.session.get("accurate_api_host") or "").strip().rstrip("/")
    return sid, host


# =========================
# OAUTH ROUTES
# =========================
@app.get("/oauth/login")
def oauth_login():
    if not (OAUTH_CLIENT_ID and OAUTH_CLIENT_SECRET and OAUTH_AUTH_URL and OAUTH_TOKEN_URL and OAUTH_REDIRECT_URI):
        return HTMLResponse("Config OAuth belum lengkap. Cek file .env (OAUTH_*).", status_code=500)
    return RedirectResponse(oauth_build_auth_url())


@app.get("/oauth/callback")
def oauth_callback(code: str = "", state: str = ""):
    if not code:
        return HTMLResponse("Tidak ada code dari Accurate. Login dibatalkan/gagal.", status_code=400)
    if not state or state != OAUTH_STORE.get("state"):
        return HTMLResponse("State tidak cocok. Silakan coba login ulang.", status_code=400)

    try:
        oauth_exchange_code(code)
        return RedirectResponse("/")
    except Exception as e:
        return HTMLResponse(f"Gagal tukar token: {e}", status_code=400)


@app.get("/oauth/status")
def oauth_status():
    token = OAUTH_STORE.get("token")
    if not token:
        return {"logged_in": False}
    return {
        "logged_in": True,
        "has_access_token": bool(token.get("access_token")),
        "expires_in_store": OAUTH_STORE.get("token_expiry", 0),
        "token_type": token.get("token_type"),
        "scope": token.get("scope"),
    }


# =========================
# COMPANY PICKER API
# =========================
class SelectCompanyBody(BaseModel):
    id: str


class AddCompanyFromUrlBody(BaseModel):
    url: str


@app.get("/accurate/companies")
def accurate_companies(request: Request):
    selected = request.session.get("selected_company_id")
    try:
        token = (OAUTH_STORE.get("token") or {}).get("access_token")
        if token:
            items = fetch_companies_from_accurate()
            COMPANY_STORE["items"] = items
            return {"items": items, "selected_company_id": selected}
    except Exception as e:
        return {"items": COMPANY_STORE["items"], "selected_company_id": selected, "warning": str(e)}

    return {"items": COMPANY_STORE["items"], "selected_company_id": selected}


@app.post("/accurate/select-company")
def accurate_select_company(request: Request, body: SelectCompanyBody):
    cid = body.id.strip()
    if not cid.isdigit():
        return {"ok": False, "error": "ID harus angka"}
    request.session["selected_company_id"] = cid
    request.session.pop("x_session_id", None)
    request.session.pop("accurate_api_host", None)
    return {"ok": True, "selected": cid}


@app.post("/accurate/add-company-from-url")
def accurate_add_company_from_url(request: Request, body: AddCompanyFromUrlBody):
    url = body.url.strip()
    m = re.search(r"[?&]id=(\d+)", url)
    if not m:
        return {"ok": False, "error": "URL tidak mengandung id=angka. Contoh: ...open.do?id=1161648"}
    cid = m.group(1)

    for it in COMPANY_STORE["items"]:
        if it["id"] == cid:
            return {"ok": True, "item": it}

    item = {"id": cid, "name": f"DataUsaha-{cid}"}
    COMPANY_STORE["items"].append(item)
    return {"ok": True, "item": item}


# =========================
# EXCEL HELPERS (biar app tetap lengkap)
# =========================
def is_nan(x: Any) -> bool:
    return isinstance(x, float) and math.isnan(x)


def normalize_str(x: Any) -> str:
    if x is None or is_nan(x):
        return ""
    return str(x).strip()


def read_excel_to_df(file_bytes: bytes) -> pd.DataFrame:
    wb = load_workbook(io.BytesIO(file_bytes), data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows or not rows[0]:
        raise ValueError("Sheet kosong")

    headers = [str(h).strip() if h is not None else "" for h in rows[0]]
    if not any(headers):
        raise ValueError("Header kosong")

    data_rows = []
    for r in rows[1:]:
        if r is None:
            continue
        row_dict = {}
        empty = True
        for i, h in enumerate(headers):
            if not h:
                continue
            val = r[i] if i < len(r) else None
            if val not in (None, ""):
                empty = False
            row_dict[h] = val
        if not empty:
            data_rows.append(row_dict)

    return pd.DataFrame(data_rows)


# =========================
# >>> PASTE BLOK BUILDER + ENDPOINT /api/* KAMU DI SINI <<<
# =========================
# (Paste mulai dari REQUIRED_COLS_JV ... sampai template download kalau ada)
# =========================


# =========================
# HEALTH
# =========================
@app.get("/health")
def health(request: Request):
    return {
        "ok": True,
        "host": get_host(request),
        "tenant": get_tenant_slug_from_request(request),
        "access_ok": bool(request.session.get("access_ok")),
        "session_tenant": request.session.get("tenant"),
        "oauth_logged_in": bool((OAUTH_STORE.get("token") or {}).get("access_token")),
        "selected_company_id": request.session.get("selected_company_id"),
        "scopes_env": OAUTH_SCOPES,
        "cookie_domain": COOKIE_DOMAIN or None,
        "https_only": HTTPS_ONLY,
    }
