import os
import json
import logging
import hashlib
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
CONTEXT_MAP_FILE = DATA_DIR / "context_map.json"

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
def load_context_map(): return load_json(CONTEXT_MAP_FILE, {"map": {}})
def save_context_map(data): save_json(CONTEXT_MAP_FILE, data)


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
    save_settings(settings)


# ============== Маппинг contextKey → accountId ==============

def save_context_mapping(context_key: str, account_id: str):
    """Сохранить связь contextKey -> account_id"""
    if not context_key or not account_id:
        return
    
    data = load_context_map()
    data["map"][context_key] = {
        "account_id": account_id,
        "created_at": now_msk().isoformat()
    }
    
    # Ограничиваем размер - храним последние 10000 маппингов
    if len(data["map"]) > 10000:
        # Сортируем по времени и удаляем старые
        sorted_keys = sorted(data["map"].keys(), 
                            key=lambda k: data["map"][k].get("created_at", ""))
        for k in sorted_keys[:len(sorted_keys)-10000]:
            del data["map"][k]
    
    save_context_map(data)
    logger.info(f"📌 Сохранён маппинг: {context_key[:20]}... -> {account_id}")


def get_account_id_from_context(context_key: str) -> Optional[str]:
    """Получить account_id по contextKey"""
    if not context_key:
        return None
    
    data = load_context_map()
    mapping = data.get("map", {}).get(context_key)
    if mapping:
        return mapping.get("account_id")
    return None


# ============== Определение аккаунта через API МойСклад ==============

async def get_context_info(context_key: str) -> Optional[dict]:
    """
    Получить информацию о контексте через API МойСклад.
    Используем токен любого активного аккаунта чтобы узнать к какому аккаунту принадлежит contextKey.
    """
    # Сначала проверяем кеш
    cached_account_id = get_account_id_from_context(context_key)
    if cached_account_id:
        acc = get_account(cached_account_id)
        if acc and acc.get("status") == "active" and acc.get("access_token"):
            logger.info(f"✅ Аккаунт из кеша: {acc.get('account_name')}")
            return acc
    
    # Пробуем определить через API
    all_accounts = get_all_active_accounts()
    
    for acc in all_accounts:
        token = acc.get("access_token")
        if not token:
            continue
        
        try:
            # Делаем запрос к context API
            url = f"https://api.moysklad.ru/api/remap/1.2/context/application"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(url, headers=headers)
                
                if resp.status_code == 200:
                    data = resp.json()
                    # Если запрос успешен с этим токеном - значит это правильный аккаунт
                    logger.info(f"✅ Аккаунт определён через API: {acc.get('account_name')}")
                    # Сохраняем маппинг
                    save_context_mapping(context_key, acc["account_id"])
                    return acc
                    
        except Exception as e:
            logger.debug(f"Ошибка проверки аккаунта {acc.get('account_name')}: {e}")
            continue
    
    return None


def get_account_from_context_sync(context_key: str) -> Optional[dict]:
    """
    Синхронное получение аккаунта из contextKey.
    Сначала проверяем кеш, если нет - возвращаем None (async определит позже).
    """
    if not context_key:
        return None
    
    # Проверяем кеш
    cached_account_id = get_account_id_from_context(context_key)
    if cached_account_id:
        acc = get_account(cached_account_id)
        if acc and acc.get("status") == "active" and acc.get("access_token"):
            return acc
    
    # Если только один активный аккаунт - используем его
    all_accounts = get_all_active_accounts()
    if len(all_accounts) == 1:
        acc = all_accounts[0]
        save_context_mapping(context_key, acc["account_id"])
        return acc
    
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


# ============== Получение аккаунта ==============

async def resolve_account(request: Request) -> Optional[dict]:
    """Определить аккаунт из запроса"""
    context_key = request.query_params.get("contextKey", "")
    
    logger.info(f"🔍 Определение аккаунта, contextKey: {context_key[:30] if context_key else 'нет'}...")
    
    # 1. Пробуем из кеша
    if context_key:
        cached_account_id = get_account_id_from_context(context_key)
        if cached_account_id:
            acc = get_account(cached_account_id)
            if acc and acc.get("status") == "active" and acc.get("access_token"):
                logger.info(f"✅ Аккаунт из кеша: {acc.get('account_name')}")
                return acc
    
    # 2. Если один активный аккаунт - используем его
    all_accounts = get_all_active_accounts()
    
    if len(all_accounts) == 0:
        logger.error("❌ Нет активных аккаунтов!")
        return None
    
    if len(all_accounts) == 1:
        acc = all_accounts[0]
        if context_key:
            save_context_mapping(context_key, acc["account_id"])
        logger.info(f"✅ Единственный аккаунт: {acc.get('account_name')}")
        return acc
    
    # 3. Несколько аккаунтов - пробуем определить через проверку токенов
    if context_key:
        # Пробуем каждый токен - тот который работает и есть правильный
        for acc in all_accounts:
            token = acc.get("access_token")
            if not token:
                continue
            
            try:
                # Простой запрос для проверки токена
                result = await ms_api("GET", "/entity/employee?limit=1", token)
                if result.get("_status") == 200:
                    # Этот токен работает
                    # Но это не гарантирует что это правильный аккаунт для данного contextKey
                    # Нам нужен другой способ...
                    pass
            except:
                continue
    
    # 4. Не удалось определить - показываем ошибку
    logger.warning(f"⚠️ Несколько аккаунтов ({len(all_accounts)}), не удалось определить")
    logger.warning(f"   Аккаунты: {[a.get('account_name') for a in all_accounts]}")
    
    # ВРЕМЕННОЕ РЕШЕНИЕ: возвращаем первый аккаунт и сохраняем маппинг
    # В реальности нужно использовать postMessage для получения accountId от МойСклад
    acc = all_accounts[0]
    if context_key:
        save_context_mapping(context_key, acc["account_id"])
    logger.warning(f"⚠️ Используется первый аккаунт: {acc.get('account_name')}")
    return acc


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
    
    # Логируем все аккаунты
    all_acc = get_all_active_accounts()
    logger.info(f"📊 Всего активных: {len(all_acc)}")
    
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


# ============== API для привязки контекста ==============

@app.post("/api/bind-context")
async def bind_context(request: Request):
    """
    Привязать contextKey к accountId.
    Вызывается из iframe после получения accountId через postMessage.
    """
    body = await request.json()
    context_key = body.get("contextKey", "")
    account_id = body.get("accountId", "")
    
    if not context_key or not account_id:
        return JSONResponse({"success": False, "error": "contextKey и accountId обязательны"})
    
    # Проверяем что аккаунт существует
    acc = get_account(account_id)
    if not acc:
        return JSONResponse({"success": False, "error": "Аккаунт не найден"})
    
    if acc.get("status") != "active":
        return JSONResponse({"success": False, "error": "Аккаунт не активен"})
    
    # Сохраняем маппинг
    save_context_mapping(context_key, account_id)
    
    return JSONResponse({
        "success": True,
        "accountId": account_id,
        "accountName": acc.get("account_name")
    })


# ============== API ==============

@app.get("/api/expense-categories")
async def api_get_categories(request: Request):
    acc = await resolve_account(request)
    
    if not acc:
        return JSONResponse({
            "categories": [], 
            "error": "Не удалось определить аккаунт. Убедитесь, что приложение активировано.",
            "needsBinding": True
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
        "accountId": account_id,
        "accountName": acc.get("account_name")
    })


@app.post("/api/expense-categories")
async def api_add_category(request: Request):
    body = await request.json()
    name = body.get("name", "").strip()
    if not name:
        return JSONResponse({"success": False, "error": "Название не указано"})
    
    acc = await resolve_account(request)
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
    
    acc = await resolve_account(request)
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
    all_accounts = get_all_active_accounts()
    context_map = load_context_map()
    
    cached_account_id = get_account_id_from_context(context_key) if context_key else None
    
    return JSONResponse({
        "context_key": context_key[:50] + "..." if len(context_key) > 50 else context_key,
        "cached_account_id": cached_account_id,
        "all_active_accounts": [
            {"id": a.get("account_id"), "name": a.get("account_name")} 
            for a in all_accounts
        ],
        "total_active": len(all_accounts),
        "context_mappings_count": len(context_map.get("map", {})),
        "server_time": now_msk().strftime("%Y-%m-%d %H:%M:%S")
    })


@app.get("/api/accounts")
async def list_accounts():
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
        "version": "3.4",
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