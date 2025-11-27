import os
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, List
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import httpx

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ROOT_PATH = os.getenv("ROOT_PATH", "/expensesms")

app = FastAPI(
    title="Накладные расходы - МойСклад",
    root_path=ROOT_PATH
)
templates = Jinja2Templates(directory="templates")

DATA_DIR = Path("/app/data")
ACCOUNTS_FILE = DATA_DIR / "accounts.json"
SETTINGS_FILE = DATA_DIR / "settings.json"

BASE_API_URL = "https://api.moysklad.ru/api/remap/1.2"
DICTIONARY_NAME = "Статьи накладных расходов"

MSK = timezone(timedelta(hours=3))


def now_msk() -> datetime:
    return datetime.now(MSK)


# ============== Хранилище ==============

def ensure_data_dir():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def load_json(path: Path, default: dict) -> dict:
    ensure_data_dir()
    if path.exists():
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return default

def save_json(path: Path, data: dict):
    ensure_data_dir()
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def load_accounts(): return load_json(ACCOUNTS_FILE, {"accounts": {}})
def save_accounts(data): save_json(ACCOUNTS_FILE, data)
def load_settings(): return load_json(SETTINGS_FILE, {"accounts_settings": {}})
def save_settings(data): save_json(SETTINGS_FILE, data)

def save_account(account_id: str, account_data: dict):
    data = load_accounts()
    account_data["updated_at"] = now_msk().isoformat()
    if "accounts" not in data:
        data["accounts"] = {}
    data["accounts"][account_id] = account_data
    save_accounts(data)

def get_account(account_id: str):
    return load_accounts().get("accounts", {}).get(account_id)

def get_account_by_context(context_key: str) -> Optional[dict]:
    """Получить аккаунт по contextKey"""
    # contextKey привязан к аккаунту, ищем по сохранённым контекстам
    data = load_accounts()
    for acc_id, acc in data.get("accounts", {}).items():
        if acc.get("status") == "active" and acc.get("access_token"):
            # Сохраняем связь contextKey -> account_id
            contexts = acc.get("contexts", [])
            if context_key in contexts:
                return acc
    # Если контекст не найден, возвращаем первый активный (для обратной совместимости)
    return get_any_active_account()

def get_any_active_account() -> Optional[dict]:
    """Получить любой активный аккаунт"""
    for acc in load_accounts().get("accounts", {}).values():
        if acc.get("status") == "active" and acc.get("access_token"):
            return acc
    return None

def save_context_for_account(account_id: str, context_key: str):
    """Сохранить связь contextKey с аккаунтом"""
    data = load_accounts()
    if account_id in data.get("accounts", {}):
        contexts = data["accounts"][account_id].get("contexts", [])
        if context_key not in contexts:
            contexts.append(context_key)
            # Храним только последние 100 контекстов
            data["accounts"][account_id]["contexts"] = contexts[-100:]
            save_accounts(data)

def get_dictionary_id(account_id: str) -> Optional[str]:
    """Получить ID справочника для конкретного аккаунта"""
    settings = load_settings()
    return settings.get("accounts_settings", {}).get(account_id, {}).get("dictionary_id")

def save_dictionary_id(account_id: str, dict_id: str):
    """Сохранить ID справочника для аккаунта"""
    settings = load_settings()
    if "accounts_settings" not in settings:
        settings["accounts_settings"] = {}
    if account_id not in settings["accounts_settings"]:
        settings["accounts_settings"][account_id] = {}
    settings["accounts_settings"][account_id]["dictionary_id"] = dict_id
    settings["accounts_settings"][account_id]["updated_at"] = now_msk().isoformat()
    save_settings(settings)


# ============== API МойСклад ==============

async def ms_api(method: str, endpoint: str, token: str, data: dict = None) -> dict:
    url = f"{BASE_API_URL}{endpoint}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip"
    }
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        logger.info(f"🔵 REQUEST: {method} {url}")
        if data:
            logger.info(f"🔵 BODY: {json.dumps(data, ensure_ascii=False)}")
        
        if method == "GET":
            resp = await client.get(url, headers=headers)
        elif method == "POST":
            resp = await client.post(url, headers=headers, json=data)
        elif method == "PUT":
            resp = await client.put(url, headers=headers, json=data)
        else:
            return {"_error": "Unknown method"}
        
        logger.info(f"🟢 RESPONSE: {resp.status_code}")
        
        try:
            result = resp.json()
            resp_str = json.dumps(result, ensure_ascii=False, default=str)
            logger.info(f"🟢 BODY: {resp_str[:500]}...")
        except:
            result = {"_text": resp.text[:1000]}
        
        result["_status"] = resp.status_code
        return result


# ============== Справочник ==============

async def ensure_dictionary(token: str, account_id: str) -> Optional[str]:
    dict_id = get_dictionary_id(account_id)
    if dict_id:
        check = await ms_api("GET", f"/entity/customentity/{dict_id}", token)
        if check.get("_status") == 200:
            return dict_id
    
    result = await ms_api("POST", "/entity/customentity", token, {"name": DICTIONARY_NAME})
    if result.get("_status") in [200, 201] and result.get("id"):
        save_dictionary_id(account_id, result["id"])
        return result["id"]
    if result.get("_status") == 412:
        return get_dictionary_id(account_id)
    return None


async def get_expense_categories(token: str, dict_id: str) -> List[dict]:
    result = await ms_api("GET", f"/entity/customentity/{dict_id}", token)
    categories = []
    if result.get("_status") == 200 and "rows" in result:
        for elem in result["rows"]:
            categories.append({"id": elem.get("id"), "name": elem.get("name")})
    return categories


async def add_expense_category(token: str, dict_id: str, name: str) -> Optional[dict]:
    result = await ms_api("POST", f"/entity/customentity/{dict_id}", token, {"name": name})
    if result.get("_status") in [200, 201] and result.get("id"):
        return {"id": result["id"], "name": result.get("name", name)}
    if result.get("_status") == 412:
        return {"id": "exists", "name": name}
    return None


# ============== Отгрузки ==============

async def search_demand(token: str, name: str):
    for ep in [
        f"/entity/demand?filter=name={name}",
        f"/entity/demand?filter=name~{name}",
        f"/entity/demand?search={name}"
    ]:
        r = await ms_api("GET", ep, token)
        if r.get("_status") == 200 and r.get("rows"):
            for row in r["rows"]:
                if name in row.get("name", ""):
                    return row
            return r["rows"][0]
    return None


async def update_demand_overhead(token: str, demand_id: str, add_sum: float, category: str) -> dict:
    demand = await ms_api("GET", f"/entity/demand/{demand_id}", token)
    if demand.get("_status") != 200:
        return {"success": False, "error": "Отгрузка не найдена"}
    
    demand_name = demand.get("name", "")
    
    current_overhead = 0
    overhead_data = demand.get("overhead")
    if overhead_data and overhead_data.get("sum"):
        current_overhead = overhead_data.get("sum", 0)
    
    new_overhead = current_overhead + int(add_sum * 100)
    
    logger.info(f"📊 {demand_name}: {current_overhead/100:.2f} + {add_sum:.2f} = {new_overhead/100:.2f}")
    
    timestamp = now_msk().strftime("%d.%m.%Y %H:%M")
    new_comment = f"[{timestamp}] +{add_sum:.2f} руб - {category}"
    current_desc = demand.get("description") or ""
    new_desc = f"{current_desc}\n{new_comment}".strip()
    
    update_data = {
        "description": new_desc,
        "overhead": {
            "sum": new_overhead,
            "distribution": "price"
        }
    }
    
    logger.info(f"⚡ ОБНОВЛЕНИЕ ОТГРУЗКИ {demand_name}")
    result = await ms_api("PUT", f"/entity/demand/{demand_id}", token, update_data)
    
    if result.get("_status") == 200:
        result_overhead = result.get("overhead")
        logger.info(f"✅ Результат overhead: {result_overhead}")
        
        return {
            "success": True,
            "demand_name": demand_name,
            "added": add_sum,
            "total": new_overhead / 100,
            "result_overhead": result_overhead
        }
    
    logger.error(f"❌ Ошибка обновления: {result}")
    return {"success": False, "error": str(result)}


# ============== Vendor API ==============

@app.put("/api/moysklad/vendor/1.0/apps/{app_id}/{account_id}")
async def activate_app(app_id: str, account_id: str, request: Request):
    body = await request.json()
    logger.info(f"🟢 АКТИВАЦИЯ: {account_id}")
    logger.info(f"📦 Данные: {json.dumps(body, ensure_ascii=False, default=str)[:500]}")
    
    token = None
    for acc in body.get("access", []):
        if acc.get("access_token"):
            token = acc["access_token"]
            break
    
    save_account(account_id, {
        "app_id": app_id,
        "account_id": account_id,
        "account_name": body.get("accountName", ""),
        "status": "active",
        "access_token": token,
        "activated_at": now_msk().isoformat(),
        "contexts": []
    })
    
    if token:
        dict_id = await ensure_dictionary(token, account_id)
        logger.info(f"📚 Справочник для {account_id}: {dict_id}")
    
    return JSONResponse({"status": "Activated"})


@app.delete("/api/moysklad/vendor/1.0/apps/{app_id}/{account_id}")
async def deactivate_app(app_id: str, account_id: str):
    logger.info(f"🔴 ДЕАКТИВАЦИЯ: {account_id}")
    acc = get_account(account_id)
    if acc:
        acc["status"] = "inactive"
        acc["access_token"] = None
        save_account(account_id, acc)
    return JSONResponse(status_code=200, content={})


@app.get("/api/moysklad/vendor/1.0/apps/{app_id}/{account_id}/status")
async def get_status(app_id: str, account_id: str):
    acc = get_account(account_id)
    return JSONResponse({"status": "Activated" if acc and acc.get("status") == "active" else "SettingsRequired"})


# ============== API ==============

def get_account_from_request(request: Request) -> Optional[dict]:
    """Получить аккаунт из запроса по contextKey"""
    context_key = request.query_params.get("contextKey", "")
    if context_key:
        acc = get_account_by_context(context_key)
        if acc:
            # Сохраняем связь контекста с аккаунтом
            save_context_for_account(acc["account_id"], context_key)
            return acc
    return get_any_active_account()


@app.get("/api/expense-categories")
async def api_get_categories(request: Request):
    acc = get_account_from_request(request)
    if not acc or not acc.get("access_token"):
        return JSONResponse({"categories": [], "error": "Нет токена"})
    
    token = acc["access_token"]
    account_id = acc["account_id"]
    
    dict_id = await ensure_dictionary(token, account_id)
    if not dict_id:
        return JSONResponse({"categories": [], "error": "Нет справочника"})
    
    categories = await get_expense_categories(token, dict_id)
    return JSONResponse({"categories": categories})


@app.post("/api/expense-categories")
async def api_add_category(request: Request):
    body = await request.json()
    name = body.get("name", "").strip()
    if not name:
        return JSONResponse({"success": False, "error": "Название не указано"})
    
    acc = get_account_from_request(request)
    if not acc or not acc.get("access_token"):
        return JSONResponse({"success": False, "error": "Нет токена"})
    
    token = acc["access_token"]
    account_id = acc["account_id"]
    
    dict_id = await ensure_dictionary(token, account_id)
    if not dict_id:
        return JSONResponse({"success": False, "error": "Нет справочника"})
    
    cat = await add_expense_category(token, dict_id, name)
    if cat:
        return JSONResponse({"success": True, "category": cat})
    return JSONResponse({"success": False, "error": "Ошибка"})


@app.post("/api/process-expenses")
async def process_expenses(request: Request):
    body = await request.json()
    expenses = body.get("expenses", [])
    category = body.get("category", "Накладные расходы")
    
    acc = get_account_from_request(request)
    if not acc or not acc.get("access_token"):
        return JSONResponse({"success": False, "error": "Нет токена"})
    
    token = acc["access_token"]
    account_id = acc["account_id"]
    
    logger.info("=" * 70)
    logger.info(f"📊 ОБРАБОТКА РАСХОДОВ: {len(expenses)} записей")
    logger.info(f"📁 Категория: {category}")
    logger.info(f"👤 Аккаунт: {account_id}")
    logger.info(f"🕐 Время (МСК): {now_msk().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)
    
    results, errors = [], []
    
    for item in expenses:
        num = item.get("demandNumber", "").strip()
        val = float(item.get("expense", 0))
        
        if not num or val <= 0:
            continue
        
        demand = await search_demand(token, num)
        if not demand:
            errors.append({"demandNumber": num, "error": "Не найдена"})
            continue
        
        r = await update_demand_overhead(token, demand["id"], val, category)
        if r["success"]:
            results.append({
                "demandNumber": num,
                "added": val,
                "total": r.get("total"),
                "status": "success"
            })
        else:
            errors.append({"demandNumber": num, "error": r.get("error")})
    
    logger.info("=" * 70)
    logger.info(f"✅ Успешно: {len(results)}, ❌ Ошибок: {len(errors)}")
    logger.info("=" * 70)
    
    return JSONResponse({
        "success": True,
        "processed": len(results),
        "errors": len(errors),
        "results": results,
        "errorDetails": errors
    })


# ============== Отладка ==============

@app.get("/api/debug")
async def debug(request: Request):
    acc = get_account_from_request(request)
    accounts = load_accounts()
    settings = load_settings()
    
    return JSONResponse({
        "current_account": acc.get("account_id") if acc else None,
        "has_token": bool(acc.get("access_token")) if acc else False,
        "total_accounts": len(accounts.get("accounts", {})),
        "accounts_list": list(accounts.get("accounts", {}).keys()),
        "settings": settings,
        "server_time_msk": now_msk().strftime("%Y-%m-%d %H:%M:%S"),
        "root_path": ROOT_PATH
    })


@app.get("/api/test-demand/{demand_name}")
async def test_demand(demand_name: str, request: Request):
    acc = get_account_from_request(request)
    if not acc or not acc.get("access_token"):
        return JSONResponse({"error": "Нет токена"})
    
    token = acc["access_token"]
    
    demand = await search_demand(token, demand_name)
    if not demand:
        return JSONResponse({"error": "Не найдена"})
    
    full = await ms_api("GET", f"/entity/demand/{demand['id']}", token)
    
    return JSONResponse({
        "id": full.get("id"),
        "name": full.get("name"),
        "overhead": full.get("overhead"),
        "description": full.get("description"),
        "sum": full.get("sum")
    })


# ============== Iframe ==============

@app.get("/iframe", response_class=HTMLResponse)
async def iframe_page(request: Request):
    return templates.TemplateResponse("iframe.html", {"request": request})


@app.get("/widget-demand", response_class=HTMLResponse)
async def widget_demand(request: Request):
    return templates.TemplateResponse("widget_demand.html", {"request": request})


@app.get("/")
async def root():
    accounts = load_accounts()
    return {
        "app": "Накладные расходы",
        "version": "3.1",
        "distribution": "price",
        "timezone": "MSK (UTC+3)",
        "server_time": now_msk().strftime("%Y-%m-%d %H:%M:%S"),
        "root_path": ROOT_PATH,
        "base_url": "https://kulps.ru/expensesms",
        "active_accounts": len([a for a in accounts.get("accounts", {}).values() if a.get("status") == "active"])
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.middleware("http")
async def mw(request: Request, call_next):
    logger.info(f"➡️ {request.method} {request.url.path}")
    response = await call_next(request)
    response.headers["X-Frame-Options"] = "ALLOWALL"
    response.headers["Content-Security-Policy"] = "frame-ancestors *"
    response.headers["Access-Control-Allow-Origin"] = "*"
    logger.info(f"⬅️ {response.status_code}")
    return response