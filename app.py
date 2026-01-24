from __future__ import annotations

import io
import os
import time
import uuid
import secrets
from datetime import datetime
from typing import Any, Dict, List, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, UploadFile, File, Request, Form
from fastapi.responses import JSONResponse, RedirectResponse
from starlette.middleware.sessions import SessionMiddleware

from openpyxl import load_workbook

load_dotenv()

# ==================================================
# ENV
# ==================================================
SESSION_SECRET = os.getenv("SESSION_SECRET", "CHANGE_ME")
DEFAULT_ACCESS_CODE = os.getenv("DEFAULT_ACCESS_CODE", "DEMO-ACA-001")

OAUTH_CLIENT_ID = os.getenv("OAUTH_CLIENT_ID")
OAUTH_CLIENT_SECRET = os.getenv("OAUTH_CLIENT_SECRET")
OAUTH_REDIRECT_URI = os.getenv("OAUTH_REDIRECT_URI")
OAUTH_AUTH_URL = "https://account.accurate.id/oauth/authorize"
OAUTH_TOKEN_URL = "https://account.accurate.id/oauth/token"

ACCURATE_BASE = "https://account.accurate.id"
OPEN_DB_PATH = "/api/open-db.do"
DB_LIST_PATH = "/api/db-list.do"
JV_BULK_PATH = "/accurate/api/journal-voucher/bulk-save.do"

# ==================================================
# APP
# ==================================================
app = FastAPI(title="ACA-AOL Import Service")

app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    same_site="lax",
    https_only=True,
)

# ==================================================
# STATE (MVP)
# ==================================================
OAUTH = {"state": None, "token": None}
JOBS: Dict[str, Any] = {}

# ==================================================
# AUTH ACCESS CODE
# ==================================================
@app.post("/auth/verify")
def verify_access(request: Request, body: Dict[str, str]):
    if body.get("code", "").upper() != DEFAULT_ACCESS_CODE.upper():
        return JSONResponse({"ok": False, "error": "Kode salah"}, status_code=401)

    request.session["access_ok"] = True
    return {"ok": True}


@app.get("/auth/status")
def auth_status(request: Request):
    return {
        "access_ok": bool(request.session.get("access_ok")),
        "oauth": bool(OAUTH.get("token")),
    }


# ==================================================
# OAUTH ACCURATE
# ==================================================
@app.get("/oauth/login")
def oauth_login():
    state = secrets.token_urlsafe(16)
    OAUTH["state"] = state

    return RedirectResponse(
        f"{OAUTH_AUTH_URL}?response_type=code"
        f"&client_id={OAUTH_CLIENT_ID}"
        f"&redirect_uri={OAUTH_REDIRECT_URI}"
        f"&scope=journal_voucher_save"
        f"&state={state}"
    )


@app.get("/oauth/callback")
def oauth_callback(code: str = "", state: str = ""):
    if state != OAUTH.get("state"):
        return JSONResponse({"error": "state invalid"}, status_code=400)

    r = requests.post(
        OAUTH_TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": OAUTH_REDIRECT_URI,
        },
        auth=(OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET),
        timeout=30,
    )
    r.raise_for_status()
    OAUTH["token"] = r.json()
    return RedirectResponse("/health")


def get_access_token():
    token = OAUTH.get("token")
    if not token:
        raise RuntimeError("Belum OAuth login")
    return token["access_token"]


# ==================================================
# CORE: IMPORT JOURNAL VOUCHER
# ==================================================
@app.post("/api/import/journal-voucher")
async def import_jv(request: Request, file: UploadFile = File(...)):
    if not request.session.get("access_ok"):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    access = get_access_token()

    # open-db
    r = requests.post(
        ACCURATE_BASE + OPEN_DB_PATH,
        params={"id": request.session.get("company_id")},
        headers={"Authorization": f"Bearer {access}"},
    )
    r.raise_for_status()
    data = r.json()

    sid = data["session"]
    host = data["host"]

    # NOTE: payload builder kamu tetap dipakai di sini
    payload = {}

    resp = requests.post(
        host + JV_BULK_PATH,
        headers={
            "Authorization": f"Bearer {access}",
            "X-Session-ID": sid,
        },
        data=payload,
        timeout=60,
    )

    return {"status": resp.status_code, "body": resp.text}


# ==================================================
# HEALTH
# ==================================================
@app.get("/health")
def health(request: Request):
    return {
        "access_ok": bool(request.session.get("access_ok")),
        "oauth": bool(OAUTH.get("token")),
    }
