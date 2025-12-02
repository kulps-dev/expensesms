import os
import json
import logging
import base64
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
    logger.info(f"💾 Сохранён аккаунт: {account_id} ({account_data.get('account_name')})")


def get_account(account_id: str) -> Optional[dict]:
    acc = load_accounts().get("accounts", {}).get(account_id)
    if acc:
        acc["account_id"] = account_id
    return acc


def get_account_by_name(account_name: str) -> Optional[dict]:
    """Найти аккаунт по имени (accountName)"""
    for acc_id, acc in load_accounts().get("accounts", {}).items():
        if acc.get("account_name") == account_name and acc.get("status") == "active":
            acc["account_id"] = acc_id
            return acc
    return None


def get_all_active_accounts() -> List[dict]:
    accounts = []
    for acc_id, acc in load_accounts().get("accounts", {}).items():
        if acc.get("status") == "active" and acc.get("access_token"):
            acc["account_id"] = acc_id
            accounts.append(acc)
    return accounts


def get_dictionary_id(account_id: str) -> Optional[str]:
    settings = load_settings()
    return settings.get("accounts_settings", {}).get(account_id, {}).get("dictionary_id")


def save_dictionary_id(account_id: str, dict_id: str):
    settings = load_settings()
    if "accounts_settings" not in settings:
        settings["accounts_settings"] = {}
    if account_id not in settings["accounts_settings"]:
        settings["accounts_settings"][account_id] = {}
    settings["accounts_settings"][account_id]["dictionary_id"] = dict_id
    settings["accounts_settings"][account_id]["updated_at"] = now_msk().isoformat()
    save_settings(settings)


# ============== Декодирование contextKey ==============

def decode_context_key(context_key: str) -> Optional[dict]:
    """
    Декодировать contextKey от МойСклад.
    contextKey - это base64 encoded JSON с информацией об аккаунте.
    """
    if not context_key:
        return None
    
    try:
        # Добавляем padding если нужно
        padding = 4 - len(context_key) % 4
        if padding != 4:
            padded = context_key + '=' * padding
        else:
            padded = context_key
        
        # Пробуем URL-safe base64
        try:
            decoded_bytes = base64.urlsafe_b64decode(padded)
        except:
            # Пробуем обычный base64
            decoded_bytes = base64.b64decode(padded)
        
        decoded_str = decoded_bytes.decode('utf-8')
        data = json.loads(decoded_str)
        
        logger.info(f"🔓 Декодирован contextKey: {json.dumps(data, ensure_ascii=False)}")
        return data
        
    except Exception as e:
        logger.debug(f"Не удалось декодировать contextKey как JSON: {e}")
        
        # Пробуем извлечь accountId из строки
        try:
            decoded_bytes = base64.urlsafe_b64decode(context_key + '==')
            decoded_str = decoded_bytes.decode('utf-8', errors='ignore')
            logger.debug(f"Декодированная строка: {decoded_str[:200]}")
        except:
            pass
        
        return None


def extract_account_from_context(context_key: str) -> Optional[dict]:
    """
    Извлечь аккаунт из contextKey.
    МойСклад передаёт в contextKey информацию включая accountId или accountName.
    """
    if not context_key:
        logger.warning("⚠️ contextKey пустой")
        return None
    
    logger.info(f"🔍 Анализ contextKey: {context_key[:50]}...")
    
    # 1. Пробуем декодировать contextKey
    decoded = decode_context_key(context_key)
    
    if decoded:
        # Ищем accountId в разных форматах
        account_id = None
        account_name = None
        
        # Возможные ключи для ID аккаунта
        for key in ["accountId", "account_id", "accountUuid", "id"]:
            if key in decoded:
                account_id = decoded[key]
                break
        
        # Возможные ключи для имени аккаунта
        for key in ["accountName", "account_name", "name"]:
            if key in decoded:
                account_name = decoded[key]
                break
        
        # Пробуем найти по ID
        if account_id:
            acc = get_account(account_id)
            if acc and acc.get("status") == "active" and acc.get("access_token"):
                logger.info(f"✅ Найден аккаунт по ID: {acc.get('account_name')}")
                return acc
        
        # Пробуем найти по имени
        if account_name:
            acc = get_account_by_name(account_name)
            if acc:
                logger.info(f"✅ Найден аккаунт по имени: {account_name}")
                return acc
    
    # 2. Если не удалось декодировать - ищем единственный активный аккаунт
    all_accounts = get_all_active_accounts()
    
    if len(all_accounts) == 1:
        acc = all_accounts[0]
        logger.info(f"✅ Используется единственный активный аккаунт: {acc.get('account_name')}")
        return acc
    
    if len(all_accounts) == 0:
        logger.error("❌ Нет активных аккаунтов!")
        return None
    
    # 3. Несколько аккаунтов - пробуем сопоставить по хешу contextKey
    # (для случаев когда один пользователь всегда получает один и тот же contextKey)
    logger.warning(f"⚠️ Несколько активных аккаунтов ({len(all_accounts)}), не удалось определить нужный")
    logger.warning(f"   Аккаунты: {[a.get('account_name') for a in all_accounts]}")
    
    # Возвращаем None - пусть пользователь увидит ошибку
    return None


# ============== API МойСклад ==============

async def ms_api(method: str, endpoint: str, token: str, data: dict = None) -> dict:
    url = f"{BASE_API_URL}{endpoint}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip"
    }
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        logger.info(f"🔵 {method} {endpoint}")
        
        if method == "GET":
            resp = await client.get(url, headers=headers)
        elif method == "POST":
            resp = await client.post(url, headers=headers, json=data)
        elif method == "PUT":
            resp = await client.put(url, headers=headers, json=data)
        else:
            return {"_error": "Unknown method"}
        
        try:
            result = resp.json()
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
    
    result = await ms_api("PUT", f"/entity/demand/{demand_id}", token, update_data)
    
    if result.get("_status") == 200:
        return {
            "success": True,
            "demand_name": demand_name,
            "added": add_sum,
            "total": new_overhead / 100
        }
    
    return {"success": False, "error": str(result)}


# ============== Получение аккаунта из запроса ==============

def get_account_from_request(request: Request) -> Optional[dict]:
    """Получить аккаунт из запроса - автоматически по contextKey"""
    context_key = request.query_params.get("contextKey", "")
    return extract_account_from_context(context_key)


# ============== Vendor API ==============

@app.put("/api/moysklad/vendor/1.0/apps/{app_id}/{account_id}")
async def activate_app(app_id: str, account_id: str, request: Request):
    body = await request.json()
    account_name = body.get("accountName", "")
    
    logger.info("=" * 70)
    logger.info(f"🟢 АКТИВАЦИЯ: {account_name} ({account_id})")
    logger.info("=" * 70)
    
    token = None
    for acc in body.get("access", []):
        if acc.get("access_token"):
            token = acc["access_token"]
            break
    
    save_account(account_id, {
        "app_id": app_id,
        "account_id": account_id,
        "account_name": account_name,
        "status": "active",
        "access_token": token,
        "activated_at": now_msk().isoformat(),
    })
    
    if token:
        dict_id = await ensure_dictionary(token, account_id)
        logger.info(f"📚 Справочник: {dict_id}")
    
    return JSONResponse({"status": "Activated"})


@app.delete("/api/moysklad/vendor/1.0/apps/{app_id}/{account_id}")
async def deactivate_app(app_id: str, account_id: str, request: Request):
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
    status = "Activated" if acc and acc.get("status") == "active" else "SettingsRequired"
    return JSONResponse({"status": status})


# ============== API ==============

@app.get("/api/expense-categories")
async def api_get_categories(request: Request):
    acc = get_account_from_request(request)
    
    if not acc:
        return JSONResponse({
            "categories": [], 
            "error": "Не удалось определить аккаунт. Убедитесь, что приложение активировано."
        }, status_code=400)
    
    if not acc.get("access_token"):
        return JSONResponse({"categories": [], "error": "Нет токена доступа"}, status_code=401)
    
    token = acc["access_token"]
    account_id = acc["account_id"]
    
    dict_id = await ensure_dictionary(token, account_id)
    if not dict_id:
        return JSONResponse({"categories": [], "error": "Не удалось создать справочник"})
    
    categories = await get_expense_categories(token, dict_id)
    return JSONResponse({
        "categories": categories,
        "accountName": acc.get("account_name")
    })


@app.post("/api/expense-categories")
async def api_add_category(request: Request):
    body = await request.json()
    name = body.get("name", "").strip()
    if not name:
        return JSONResponse({"success": False, "error": "Название не указано"})
    
    acc = get_account_from_request(request)
    if not acc or not acc.get("access_token"):
        return JSONResponse({"success": False, "error": "Аккаунт не определён"}, status_code=400)
    
    token = acc["access_token"]
    account_id = acc["account_id"]
    
    dict_id = await ensure_dictionary(token, account_id)
    if not dict_id:
        return JSONResponse({"success": False, "error": "Нет справочника"})
    
    cat = await add_expense_category(token, dict_id, name)
    if cat:
        return JSONResponse({"success": True, "category": cat})
    return JSONResponse({"success": False, "error": "Ошибка создания"})


@app.post("/api/process-expenses")
async def process_expenses(request: Request):
    body = await request.json()
    expenses = body.get("expenses", [])
    category = body.get("category", "Накладные расходы")
    
    acc = get_account_from_request(request)
    if not acc or not acc.get("access_token"):
        return JSONResponse({"success": False, "error": "Аккаунт не определён"}, status_code=400)
    
    token = acc["access_token"]
    account_name = acc.get("account_name", "")
    
    logger.info(f"📊 Обработка {len(expenses)} расходов для {account_name}")
    
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
    
    logger.info(f"✅ Успешно: {len(results)}, ❌ Ошибок: {len(errors)}")
    
    return JSONResponse({
        "success": True,
        "processed": len(results),
        "errors": len(errors),
        "results": results,
        "errorDetails": errors,
        "accountName": account_name
    })


# ============== Отладка ==============

@app.get("/api/debug")
async def debug(request: Request):
    context_key = request.query_params.get("contextKey", "")
    acc = get_account_from_request(request)
    all_accounts = get_all_active_accounts()
    
    # Пробуем декодировать contextKey для отладки
    decoded = decode_context_key(context_key) if context_key else None
    
    return JSONResponse({
        "context_key_provided": bool(context_key),
        "context_key_preview": context_key[:80] + "..." if len(context_key) > 80 else context_key,
        "context_key_decoded": decoded,
        "resolved_account": {
            "id": acc.get("account_id"),
            "name": acc.get("account_name"),
            "has_token": bool(acc.get("access_token"))
        } if acc else None,
        "all_active_accounts": [
            {"id": a.get("account_id"), "name": a.get("account_name")} 
            for a in all_accounts
        ],
        "total_active": len(all_accounts),
        "server_time": now_msk().strftime("%Y-%m-%d %H:%M:%S")
    })


@app.get("/api/accounts")
async def list_accounts():
    """Список всех аккаунтов"""
    accounts_data = load_accounts()
    result = []
    for acc_id, acc in accounts_data.get("accounts", {}).items():
        result.append({
            "id": acc_id,
            "name": acc.get("account_name"),
            "status": acc.get("status"),
            "has_token": bool(acc.get("access_token")),
            "activated_at": acc.get("activated_at")
        })
    return JSONResponse({"accounts": result})


# ============== Iframe ==============

@app.get("/iframe", response_class=HTMLResponse)
async def iframe_page(request: Request):
    return templates.TemplateResponse("iframe.html", {"request": request})


@app.get("/widget-demand", response_class=HTMLResponse)
async def widget_demand(request: Request):
    return templates.TemplateResponse("widget_demand.html", {"request": request})


@app.get("/")
async def root():
    all_accounts = get_all_active_accounts()
    return {
        "app": "Накладные расходы",
        "version": "3.3",
        "active_accounts": len(all_accounts),
        "server_time": now_msk().strftime("%Y-%m-%d %H:%M:%S")
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.middleware("http")
async def mw(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Frame-Options"] = "ALLOWALL"
    response.headers["Content-Security-Policy"] = "frame-ancestors *"
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response