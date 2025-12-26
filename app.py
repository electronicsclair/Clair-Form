import os
import re
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from flask import Flask, render_template, request

# ------------------------------------------------------------
# Load env
# ------------------------------------------------------------
load_dotenv()

FLASK_SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "dev_secret")
PORT = int(os.getenv("PORT", "5055"))

NOTION_TOKEN = os.getenv("NOTION_TOKEN", "")
NOTION_VERSION = os.getenv("NOTION_VERSION", "2022-06-28")

DB_DAILY_SALES = os.getenv("DB_DAILY_SALES", "")
DB_MASTER_SALESMAN = os.getenv("DB_MASTER_SALESMAN", "")
DB_MASTER_DISTRIBUTOR = os.getenv("DB_MASTER_DISTRIBUTOR", "")
DB_MASTER_SKU = os.getenv("DB_MASTER_SKU", "")
# OPTIONAL: if you have a Master Outlet DB, put it here in .env, else leave blank
DB_MASTER_OUTLET = os.getenv("DB_MASTER_OUTLET", "")

if not NOTION_TOKEN:
    raise RuntimeError("NOTION_TOKEN missing in .env")

# ------------------------------------------------------------
# Flask
# ------------------------------------------------------------
app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

NOTION_BASE = "https://api.notion.com/v1"

# ------------------------------------------------------------
# Notion helpers
# ------------------------------------------------------------
def _notion_post(path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{NOTION_BASE}{path}"
    r = requests.post(url, headers=NOTION_HEADERS, json=payload, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"Notion error {r.status_code}: {r.text}")
    return r.json()

def _query_database_all(database_id: str, filter_payload: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Pulls ALL rows from a Notion database (handles pagination).
    """
    if not database_id:
        return []

    results: List[Dict[str, Any]] = []
    start_cursor = None

    while True:
        payload: Dict[str, Any] = {"page_size": 100}

        # optional filter
        if filter_payload:
            payload["filter"] = filter_payload

        if start_cursor:
            payload["start_cursor"] = start_cursor

        data = _notion_post(f"/databases/{database_id}/query", payload)
        results.extend(data.get("results", []))

        if data.get("has_more"):
            start_cursor = data.get("next_cursor")
        else:
            break

    return results

def _get_prop_value(props: Dict[str, Any], prop_name: str) -> Optional[str]:
    """
    Returns a string value from common Notion property types:
    title, rich_text, number, select, status, formula, relation (as count), etc.
    """
    p = props.get(prop_name)
    if not p:
        return None

    t = p.get("type")

    if t == "title":
        arr = p.get("title", [])
        return "".join([x.get("plain_text", "") for x in arr]).strip() or None

    if t == "rich_text":
        arr = p.get("rich_text", [])
        return "".join([x.get("plain_text", "") for x in arr]).strip() or None

    if t == "number":
        v = p.get("number")
        return None if v is None else str(v)

    if t == "select":
        sel = p.get("select")
        return sel.get("name") if sel else None

    if t == "status":
        st = p.get("status")
        return st.get("name") if st else None

    if t == "formula":
        f = p.get("formula", {})
        # formula can be string/number/boolean/date
        if f.get("type") == "string":
            return f.get("string")
        if f.get("type") == "number":
            v = f.get("number")
            return None if v is None else str(v)
        if f.get("type") == "boolean":
            b = f.get("boolean")
            return None if b is None else ("true" if b else "false")
        if f.get("type") == "date":
            d = f.get("date")
            return d.get("start") if d else None
        return None

    # fallback: try plain_text-ish
    return None

def _options_from_db(database_id: str, id_property_name: str, label_property_name: Optional[str] = None) -> List[Tuple[str, str]]:
    """
    Returns dropdown options as list of (value, label).
    - value: the ID field you store (e.g., Salesman_ID)
    - label: either same as value, or "value - name" if label property provided
    """
    pages = _query_database_all(database_id)

    opts: List[Tuple[str, str]] = []
    for pg in pages:
        props = pg.get("properties", {})

        v = _get_prop_value(props, id_property_name)
        if not v:
            continue

        v = str(v).strip()
        label = v

        if label_property_name:
            nm = _get_prop_value(props, label_property_name)
            if nm:
                label = f"{v} — {nm.strip()}"

        opts.append((v, label))

    # stable sort
    opts.sort(key=lambda x: x[0])
    return opts

def _parse_date_yyyy_mm_dd(s: str) -> str:
    """
    Accepts 'YYYY-MM-DD' and returns same if valid.
    """
    s = (s or "").strip()
    dt.datetime.strptime(s, "%Y-%m-%d")  # raises if invalid
    return s

def _to_float(s: str) -> float:
    s = (s or "").strip()
    # allow commas
    s = s.replace(",", "")
    return float(s)

def _to_int(s: str) -> int:
    s = (s or "").strip()
    s = s.replace(",", "")
    return int(float(s))

def _notion_create_daily_sales_row(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not DB_DAILY_SALES:
        raise RuntimeError("DB_DAILY_SALES missing/blank in .env")

    body = {
        "parent": {"database_id": DB_DAILY_SALES},
        "properties": payload,
    }
    return _notion_post("/pages", body)

# ------------------------------------------------------------
# Routes
# ------------------------------------------------------------
@app.route("/", methods=["GET", "POST"])
def daily_sales_form():
    # Dropdown options (pulled from Notion)
    salesman_opts = _options_from_db(DB_MASTER_SALESMAN, "Salesman_ID", "Salesman_Name")
    distributor_opts = _options_from_db(DB_MASTER_DISTRIBUTOR, "Distributor_ID", "Distributor_Name")
    sku_opts = _options_from_db(DB_MASTER_SKU, "SKU_ID", "SKU Name")

    outlet_opts: List[Tuple[str, str]] = []
    if DB_MASTER_OUTLET:
        # If you have a master outlet DB, set its properties here:
        outlet_opts = _options_from_db(DB_MASTER_OUTLET, "Outlet ID", "Outlet_Name")

    errors: List[str] = []
    form_values: Dict[str, Any] = {}

    if request.method == "POST":
        # read form fields
        form_values = {
            "date": request.form.get("date", ""),
            "salesman_id": request.form.get("salesman_id", ""),
            "distributor_id": request.form.get("distributor_id", ""),
            "region": request.form.get("region", ""),
            "outlet_id": request.form.get("outlet_id", ""),
            "outlet_name": request.form.get("outlet_name", ""),
            "sku_id": request.form.get("sku_id", ""),
            "quantity": request.form.get("quantity", ""),
            "value": request.form.get("value", ""),
            "selling_mode": request.form.get("selling_mode", ""),
            "visit_yn": request.form.get("visit_yn", ""),
        }

        # validate required
        required_fields = [
            "date", "salesman_id", "distributor_id", "region", "outlet_id",
            "sku_id", "quantity", "value", "selling_mode", "visit_yn"
        ]
        missing = [f for f in required_fields if not str(form_values.get(f, "")).strip()]
        if missing:
            errors.append("Missing fields: " + ", ".join(missing))

        # type checks
        if not errors:
            try:
                date_iso = _parse_date_yyyy_mm_dd(form_values["date"])
            except Exception:
                errors.append("Date must be in YYYY-MM-DD format.")

            try:
                qty = _to_int(form_values["quantity"])
            except Exception:
                errors.append("Quantity must be a number.")

            try:
                val = _to_float(form_values["value"])
            except Exception:
                errors.append("Value must be a number.")

        # if ok, push to Notion
        if not errors:
            date_iso = _parse_date_yyyy_mm_dd(form_values["date"])
            qty = _to_int(form_values["quantity"])
            val = _to_float(form_values["value"])

            # Notion "Name" (title) — make it meaningful
            title_text = f"{date_iso} | Outlet {form_values['outlet_id']} | SKU {form_values['sku_id']}"

            # Map to your Notion DB property names exactly as in your screenshot:
            # Date (date), Salesman_ID (text), Distributor_ID (text), Region (text),
            # Outlet ID (text/number), Outlet_Name (text), SKU_ID (text),
            # Quantity (number), Value (number), Selling_Mode (select), Visit (select)
            notion_props = {
                "Name": {"title": [{"text": {"content": title_text}}]},
                "Date": {"date": {"start": date_iso}},
                "Salesman_ID": {"rich_text": [{"text": {"content": form_values["salesman_id"].strip()}}]},
                "Distributor_ID": {"rich_text": [{"text": {"content": form_values["distributor_id"].strip()}}]},
                "Region": {"rich_text": [{"text": {"content": form_values["region"].strip()}}]},
                "Outlet ID": {"rich_text": [{"text": {"content": form_values["outlet_id"].strip()}}]},
                "Outlet_Name": {"rich_text": [{"text": {"content": (form_values["outlet_name"] or "").strip()}}]},
                "SKU_ID": {"rich_text": [{"text": {"content": form_values["sku_id"].strip()}}]},
                "Quantity": {"number": qty},
                "Value": {"number": val},
                "Selling_Mode": {"select": {"name": form_values["selling_mode"].strip()}},
                "Visit": {"select": {"name": form_values["visit_yn"].strip()}},
            }

            _notion_create_daily_sales_row(notion_props)
            return render_template("success.html", title=title_text)

    return render_template(
        "daily_sales_form.html",
        salesman_opts=salesman_opts,
        distributor_opts=distributor_opts,
        sku_opts=sku_opts,
        outlet_opts=outlet_opts,
        has_outlet_dropdown=bool(outlet_opts),
        errors=errors,
        v=form_values,
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=True)
