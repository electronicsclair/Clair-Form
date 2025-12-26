import os
import re
import json
import requests
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, flash

# ----------------------------
# Flask
# ----------------------------
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "change_me")

# ----------------------------
# Env
# ----------------------------
NOTION_TOKEN = os.getenv("NOTION_TOKEN", "")
NOTION_VERSION = os.getenv("NOTION_VERSION", "2022-06-28")

DB_DAILY_SALES = os.getenv("DB_DAILY_SALES", "")
DB_MASTER_SALESMAN = os.getenv("DB_MASTER_SALESMAN", "")
DB_MASTER_DISTRIBUTOR = os.getenv("DB_MASTER_DISTRIBUTOR", "")
DB_MASTER_SKU = os.getenv("DB_MASTER_SKU", "")

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

NOTION_API = "https://api.notion.com/v1"


# ----------------------------
# Helpers
# ----------------------------
def _clean_uuid(x: str) -> str:
    """
    Notion accepts database_id with or without hyphens in many cases,
    but we keep as-is if user already provides raw 32-char id.
    """
    if not x:
        return ""
    return x.strip()


def _notion_post(path: str, payload: dict) -> dict:
    url = f"{NOTION_API}{path}"
    r = requests.post(url, headers=NOTION_HEADERS, data=json.dumps(payload))
    if r.status_code >= 400:
        raise RuntimeError(f"Notion error {r.status_code}: {r.text}")
    return r.json()


def _notion_get(path: str) -> dict:
    url = f"{NOTION_API}{path}"
    r = requests.get(url, headers=NOTION_HEADERS)
    if r.status_code >= 400:
        raise RuntimeError(f"Notion error {r.status_code}: {r.text}")
    return r.json()


def _query_database_all(database_id: str, filter_payload: dict | None = None) -> list[dict]:
    database_id = _clean_uuid(database_id)
    if not database_id:
        return []

    results = []
    start_cursor = None

    while True:
        payload = {"page_size": 100}
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


def _extract_prop_value(page: dict, prop_name: str) -> str:
    """
    Best-effort extraction for common property types:
    title, rich_text, select, number
    """
    props = page.get("properties", {})
    prop = props.get(prop_name)
    if not prop:
        return ""

    t = prop.get("type")
    if t == "title":
        arr = prop.get("title", [])
        return "".join([x.get("plain_text", "") for x in arr]).strip()
    if t == "rich_text":
        arr = prop.get("rich_text", [])
        return "".join([x.get("plain_text", "") for x in arr]).strip()
    if t == "select":
        s = prop.get("select")
        return (s or {}).get("name", "") if s else ""
    if t == "number":
        n = prop.get("number")
        return "" if n is None else str(n)
    return ""


def _options_from_db(database_id: str, id_prop: str, label_prop: str | None = None) -> list[dict]:
    """
    Returns list of dicts: { "value": "...", "label": "..." }
    """
    pages = _query_database_all(database_id)
    out = []
    for p in pages:
        v = _extract_prop_value(p, id_prop)
        if not v:
            continue
        lbl = v
        if label_prop:
            lab = _extract_prop_value(p, label_prop)
            if lab:
                lbl = f"{v} — {lab}"
        out.append({"value": v, "label": lbl})
    # Sort nicely
    out.sort(key=lambda x: x["label"].lower())
    return out


def _distributor_options_with_region(database_id: str) -> list[dict]:
    """
    Distributor dropdown with region embedded:
    { value, label, region }
    """
    pages = _query_database_all(database_id)
    out = []
    for p in pages:
        dist_id = _extract_prop_value(p, "Distributor_ID")
        if not dist_id:
            continue
        dist_name = _extract_prop_value(p, "Distributor_Name")
        region = _extract_prop_value(p, "Region")
        label = dist_id if not dist_name else f"{dist_id} — {dist_name}"
        out.append({"value": dist_id, "label": label, "region": region})
    out.sort(key=lambda x: x["label"].lower())
    return out


def _rt(text: str) -> dict:
    """Notion rich_text field helper."""
    return {"rich_text": [{"type": "text", "text": {"content": text}}]}


def _title(text: str) -> dict:
    """Notion title field helper."""
    return {"title": [{"type": "text", "text": {"content": text}}]}


# ----------------------------
# Routes
# ----------------------------
@app.route("/", methods=["GET", "POST"])
def daily_sales_form():
    if not NOTION_TOKEN:
        return "Missing NOTION_TOKEN in env", 500

    # Dropdown data from Notion
    salesman_opts = _options_from_db(DB_MASTER_SALESMAN, id_prop="Salesman_ID", label_prop="Salesman_Name")
    distributor_opts = _distributor_options_with_region(DB_MASTER_DISTRIBUTOR)
    sku_opts = _options_from_db(DB_MASTER_SKU, id_prop="SKU_ID", label_prop="SKU Name")

    if request.method == "POST":
        # IMPORTANT: these names must match <name="..."> in HTML
        date = (request.form.get("date") or "").strip()
        salesman_id = (request.form.get("salesman_id") or "").strip()
        distributor_id = (request.form.get("distributor_id") or "").strip()
        region = (request.form.get("region") or "").strip()
        outlet_id = (request.form.get("outlet_id") or "").strip()
        outlet_name = (request.form.get("outlet_name") or "").strip()
        sku_id = (request.form.get("sku_id") or "").strip()
        quantity = (request.form.get("quantity") or "").strip()
        value = (request.form.get("value") or "").strip()
        selling_mode = (request.form.get("selling_mode") or "").strip()
        visit_yn = (request.form.get("visit_yn") or "").strip()

        missing = []
        for k, v in [
            ("date", date),
            ("salesman_id", salesman_id),
            ("distributor_id", distributor_id),
            ("region", region),
            ("outlet_id", outlet_id),
            ("sku_id", sku_id),
            ("quantity", quantity),
            ("value", value),
            ("selling_mode", selling_mode),
            ("visit_yn", visit_yn),
        ]:
            if not v:
                missing.append(k)

        if missing:
            flash(f"Missing fields: {', '.join(missing)}", "error")
            return render_template(
                "daily_sales_form.html",
                salesman_opts=salesman_opts,
                distributor_opts=distributor_opts,
                sku_opts=sku_opts,
                form=request.form,
            )

        # Convert number fields safely
        try:
            quantity_num = float(quantity)
        except:
            flash("Quantity must be a number", "error")
            return redirect(url_for("daily_sales_form"))

        try:
            value_num = float(value)
        except:
            flash("Value must be a number", "error")
            return redirect(url_for("daily_sales_form"))

        # Notion "date" should be YYYY-MM-DD
        # HTML date input already gives YYYY-MM-DD
        try:
            datetime.strptime(date, "%Y-%m-%d")
        except:
            flash("Date must be valid (YYYY-MM-DD). Use the date picker.", "error")
            return redirect(url_for("daily_sales_form"))

        # Page title for the "Name" column in Notion
        title_text = f"{date} | {salesman_id} | {outlet_id} | {sku_id}"

        # Create page payload for Daily Secondary Sales DB
        # NOTE: property names MUST match your Notion DB columns exactly.
        payload = {
            "parent": {"database_id": _clean_uuid(DB_DAILY_SALES)},
            "properties": {
                # Title column in Notion DB
                "Name": _title(title_text),

                # Your DB columns
                "Date": {"date": {"start": date}},
                "Salesman_ID": _rt(salesman_id),
                "Distributor_ID": _rt(distributor_id),
                "Region": _rt(region),
                "Outlet ID": _rt(outlet_id),
                "Outlet_Name": _rt(outlet_name),
                "SKU_ID": _rt(sku_id),
                "Quantity": {"number": quantity_num},
                "Value": {"number": value_num},

                # These two are SELECT columns (per your screenshot)
                "Selling_Mode": {"select": {"name": selling_mode}},
                "Visit": {"select": {"name": "Yes" if visit_yn == "Y" else "No"}},
            },
        }

        # Validate DB id
        if not DB_DAILY_SALES:
            raise RuntimeError("DB_DAILY_SALES is empty. Put it in .env")

        # Create in Notion
        try:
            _notion_post("/pages", payload)
        except Exception as e:
            flash(str(e), "error")
            return render_template(
                "daily_sales_form.html",
                salesman_opts=salesman_opts,
                distributor_opts=distributor_opts,
                sku_opts=sku_opts,
                form=request.form,
            )

        flash("Saved to Notion ✅", "success")
        return redirect(url_for("daily_sales_form"))

    # GET
    return render_template(
        "daily_sales_form.html",
        salesman_opts=salesman_opts,
        distributor_opts=distributor_opts,
        sku_opts=sku_opts,
        form={},
    )


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)
