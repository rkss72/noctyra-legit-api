import os
import sqlite3
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field


DB_PATH = Path(__file__).resolve().parent / "legit_server.db"
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "")

app = FastAPI(title="Noctyra Legit API")


class AddLicensePayload(BaseModel):
    license_key: str
    username: str = ""
    expires_at: str = ""  # ISO datetime, optional


class LoginPayload(BaseModel):
    license_key: str
    username: str = ""


class RestockPayload(BaseModel):
    product_code: str
    items: list[str] = Field(default_factory=list)


class ConsumePayload(BaseModel):
    license_key: str
    product_code: str


def now_iso() -> str:
    return datetime.utcnow().isoformat()


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def require_admin(x_admin_key: str | None) -> None:
    if not ADMIN_API_KEY:
        raise HTTPException(status_code=500, detail="ADMIN_API_KEY is not configured")
    if not x_admin_key or x_admin_key != ADMIN_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid admin key")


def normalize_product(product_code: str) -> str:
    return product_code.strip().lower()


@app.on_event("startup")
def startup() -> None:
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS licenses (
            license_key TEXT PRIMARY KEY,
            username TEXT NOT NULL DEFAULT '',
            expires_at TEXT NOT NULL DEFAULT '',
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_code TEXT NOT NULL,
            item_value TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS consumptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            license_key TEXT NOT NULL,
            product_code TEXT NOT NULL,
            item_value TEXT NOT NULL,
            consumed_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()


@app.get("/health")
def health() -> dict:
    return {"ok": True}


@app.post("/admin/add-license")
def admin_add_license(payload: AddLicensePayload, x_admin_key: str | None = Header(default=None)) -> dict:
    require_admin(x_admin_key)
    key = payload.license_key.strip()
    if not key:
        raise HTTPException(status_code=400, detail="license_key required")

    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO licenses(license_key, username, expires_at, active, created_at)
        VALUES(?, ?, ?, 1, ?)
        ON CONFLICT(license_key) DO UPDATE SET
            username=excluded.username,
            expires_at=excluded.expires_at,
            active=1
        """,
        (key, payload.username.strip(), payload.expires_at.strip(), now_iso()),
    )
    conn.commit()
    conn.close()
    return {"ok": True, "license_key": key}


@app.post("/admin/restock")
def admin_restock(payload: RestockPayload, x_admin_key: str | None = Header(default=None)) -> dict:
    require_admin(x_admin_key)
    product = normalize_product(payload.product_code)
    items = [i.strip() for i in payload.items if i.strip()]
    if not product:
        raise HTTPException(status_code=400, detail="product_code required")
    if not items:
        return {"ok": True, "inserted": 0, "product_code": product}

    conn = db()
    cur = conn.cursor()
    cur.executemany(
        "INSERT INTO stock_items(product_code, item_value, created_at) VALUES(?, ?, ?)",
        [(product, item, now_iso()) for item in items],
    )
    conn.commit()
    conn.close()
    return {"ok": True, "inserted": len(items), "product_code": product}


@app.post("/admin/stock-count")
def admin_stock_count(x_admin_key: str | None = Header(default=None)) -> dict:
    require_admin(x_admin_key)
    conn = db()
    cur = conn.cursor()
    cur.execute(
        "SELECT product_code, COUNT(*) as c FROM stock_items GROUP BY product_code ORDER BY product_code"
    )
    rows = cur.fetchall()
    conn.close()
    return {"ok": True, "counts": {row["product_code"]: row["c"] for row in rows}}


@app.post("/client/login")
def client_login(payload: LoginPayload) -> dict:
    key = payload.license_key.strip()
    if not key:
        return {"ok": False, "detail": "missing_key"}

    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM licenses WHERE license_key = ?", (key,))
    row = cur.fetchone()
    if row is None or int(row["active"]) != 1:
        conn.close()
        return {"ok": False, "detail": "invalid_key"}

    expires_at = (row["expires_at"] or "").strip()
    if expires_at:
        try:
            if datetime.utcnow() > datetime.fromisoformat(expires_at):
                conn.close()
                return {"ok": False, "detail": "expired_key"}
        except ValueError:
            pass

    wanted_username = payload.username.strip()
    if wanted_username and wanted_username != (row["username"] or ""):
        cur.execute("UPDATE licenses SET username = ? WHERE license_key = ?", (wanted_username, key))
        conn.commit()

    conn.close()
    return {
        "ok": True,
        "username": wanted_username or (row["username"] or ""),
        "expires_at": expires_at or "Lifetime",
    }


@app.post("/client/consume")
def client_consume(payload: ConsumePayload) -> dict:
    key = payload.license_key.strip()
    product = normalize_product(payload.product_code)
    if not key or not product:
        return {"ok": False, "detail": "missing_fields"}

    # validate key
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM licenses WHERE license_key = ? AND active = 1", (key,))
    license_row = cur.fetchone()
    if license_row is None:
        conn.close()
        return {"ok": False, "detail": "invalid_key"}

    # atomic consume
    cur.execute("BEGIN IMMEDIATE")
    cur.execute(
        "SELECT id, item_value FROM stock_items WHERE product_code = ? ORDER BY id ASC LIMIT 1",
        (product,),
    )
    row = cur.fetchone()
    if row is None:
        conn.rollback()
        conn.close()
        return {"ok": False, "detail": "out_of_stock"}

    item_id = int(row["id"])
    item_value = str(row["item_value"])
    cur.execute("DELETE FROM stock_items WHERE id = ?", (item_id,))
    cur.execute(
        "INSERT INTO consumptions(license_key, product_code, item_value, consumed_at) VALUES(?, ?, ?, ?)",
        (key, product, item_value, now_iso()),
    )
    conn.commit()
    conn.close()
    return {"ok": True, "item": item_value}
