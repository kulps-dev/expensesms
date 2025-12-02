# main.py - исправленная версия

import os
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, List
from pathlib import Path
import hashlib

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
CONTEXTS_FILE = DATA_DIR / "contexts.json"  # Новый файл для контекстов

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

def load_accounts(): return load_json(ACCOUNTS_FILE, {"accounts": {}, "history": []})
def save_accounts(data): save_json(ACCOUNTS_FILE, data)
def load_settings(): return load_json(SETTINGS_FILE, {"accounts_settings": {}})
def save_settings(data): save_json(SETTINGS_FILE, data)
def load_contexts(): return load_json(CONTEXTS_FILE, {"mappings": {}})
def save_contexts(data): save_json(CONTEXTS_FILE, data)


def save_account(account_id: str, account_data: dict):
    data = load_accounts()
    account_data["updated_at"] = now_msk().isoformat()
    if "accounts" not in data:
        data["accounts"] = {}
    data["accounts"][account_id] = account_data
    
    # История
    if "history" not in data:
        data["history"] = []
    data["history"].append({
        "timestamp": now_msk().isoformat(),
        "action": "update",
        "account_id": account_id
    })
    data["history"] = data["history"][-100:]  # Последние 100 записей
    
    save_accounts(data)
    logger.info(f"💾 Сохранён аккаунт: {account_id} ({account_data.get('account_name')})")


def get_account(account_id: str) -> Optional[dict]:
    acc = load_accounts().get("accounts", {}).get(account_id)
    if acc:
        acc["account_id"] = account_id  # Гарантируем наличие ID
    return acc


def get_all_active_accounts() -> List[dict]:
    """Получить все активные аккаунты"""
    accounts = []
    for acc_id, acc in load_accounts().get("accounts", {}).items():
        if acc.get("status") == "active" and acc.get("access_token"):
            acc["account_id"] = acc_id
            accounts.append(acc)
    return accounts


# ============== Работа с контекстами ==============

def decode_context_key(context_key: str) -> Optional[dict]:
    """
    Декодировать contextKey для извлечения account_id
    contextKey в МойСклад содержит закодированную информацию
    """
    try:
        import base64
        # contextKey обычно base64 encoded JSON
        # Пробуем декодировать
        padding = 4 - len(context_key) % 4
        if padding != 4:
            context_key += '=' * padding
        decoded = base64.urlsafe_b64decode(context_key)
        data = json.loads(decoded)
        logger.info(f"🔓 Декодирован contextKey: {data}")
        return data
    except Exception as e:
        logger.debug(f"Не удалось декодировать contextKey: {e}")
        return None


def get_account_id_from_context(context_key: str) -> Optional[str]:
    """Получить account_id из contextKey"""
    if not context_key:
        return None
    
    # 1. Проверяем сохранённые маппинги
    contexts = load_contexts()
    if context_key in contexts.get("mappings", {}):
        account_id = contexts["mappings"][context_key]
        logger.info(f"📎 Найден маппинг: {context_key[:20]}... -> {account_id}")
        return account_id
    
    # 2. Пробуем декодировать contextKey
    decoded = decode_context_key(context_key)
    if decoded:
        # МойСклад может передавать accountId в разных форматах
        for key in ["accountId", "account_id", "accountUuid"]:
            if key in decoded:
                return decoded[key]
    
    return None


def save_context_mapping(context_key: str, account_id: str):
    """Сохранить связь contextKey -> account_id"""
    if not context_key or not account_id:
        return
    
    contexts = load_contexts()
    if "mappings" not in contexts:
        contexts["mappings"] = {}
    
    contexts["mappings"][context_key] = account_id
    
    # Ограничиваем размер (последние 1000 контекстов)
    if len(contexts["mappings"]) > 1000:
        # Удаляем старые
        keys = list(contexts["mappings"].keys())
        for k in keys[:len(keys)-1000]:
            del contexts["mappings"][k]
    
    save_contexts(contexts)
    logger.info(f"📌 Сохранён маппинг: {context_key[:20]}... -> {account_id}")


def get_account_by_context(context_key: str) -> Optional[dict]:
    """Получить аккаунт по contextKey"""
    account_id = get_account_id_from_context(context_key)
    
    if account_id:
        acc = get_account(account_id)
        if acc and acc.get("status") == "active" and acc.get("access_token"):
            return acc
        logger.warning(f"⚠️ Аккаунт {account_id} найден, но неактивен или без токена")
    
    # НЕ возвращаем fallback - это вызывает проблемы
    logger.warning(f"⚠️ Не найден аккаунт для contextKey: {context_key[:30]}...")
    return None


def get_any_active_account() -> Optional[dict]:
    """Получить любой активный аккаунт (только для отладки!)"""
    accounts = get_all_active_accounts()
    if accounts:
        logger.warning(f"⚠️ Используется fallback аккаунт: {accounts[0].get('account_id')}")
        return accounts[0]
    return None


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
            logger.info(f"🔵 BODY: {json.dumps(data, ensure_ascii=False)[:300]}")
        
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
    
    result = await ms_api("PUT", f"/entity/demand/{demand_id}", token, update_data)
    
    if result.get("_status") == 200:
        return {
            "success": True,
            "demand_name": demand_name,
            "added": add_sum,
            "total": new_overhead / 100,
            "result_overhead": result.get("overhead")
        }
    
    return {"success": False, "error": str(result)}


# ============== Vendor API ==============

@app.put("/api/moysklad/vendor/1.0/apps/{app_id}/{account_id}")
async def activate_app(app_id: str, account_id: str, request: Request):
    body = await request.json()
    account_name = body.get("accountName", "")
    
    logger.info("=" * 70)
    logger.info(f"🟢 АКТИВАЦИЯ АККАУНТА")
    logger.info(f"   Account ID: {account_id}")
    logger.info(f"   Account Name: {account_name}")
    logger.info(f"   App ID: {app_id}")
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
        logger.info(f"📚 Справочник для {account_name}: {dict_id}")
    
    # Логируем текущее состояние
    all_accounts = get_all_active_accounts()
    logger.info(f"📊 Всего активных аккаунтов: {len(all_accounts)}")
    for acc in all_accounts:
        logger.info(f"   - {acc.get('account_name')} ({acc.get('account_id')})")
    
    return JSONResponse({"status": "Activated"})


@app.delete("/api/moysklad/vendor/1.0/apps/{app_id}/{account_id}")
async def deactivate_app(app_id: str, account_id: str, request: Request):
    body = {}
    try:
        body = await request.json()
    except:
        pass
    
    logger.info("=" * 70)
    logger.info(f"🔴 ДЕАКТИВАЦИЯ АККАУНТА")
    logger.info(f"   Account ID: {account_id}")
    logger.info(f"   Причина: {body.get('cause', 'unknown')}")
    logger.info("=" * 70)
    
    acc = get_account(account_id)
    if acc:
        acc["status"] = "inactive"
        acc["access_token"] = None
        acc["deactivated_at"] = now_msk().isoformat()
        save_account(account_id, acc)
    
    return JSONResponse(status_code=200, content={})


@app.get("/api/moysklad/vendor/1.0/apps/{app_id}/{account_id}/status")
async def get_status(app_id: str, account_id: str):
    acc = get_account(account_id)
    status = "Activated" if acc and acc.get("status") == "active" else "SettingsRequired"
    logger.info(f"📋 Статус {account_id}: {status}")
    return JSONResponse({"status": status})


# ============== Определение аккаунта из запроса ==============

async def resolve_account_from_request(request: Request) -> Optional[dict]:
    """
    Определить аккаунт из запроса.
    Приоритет:
    1. contextKey в query params
    2. X-Account-Id header
    3. account_id в body (для POST)
    """
    context_key = request.query_params.get("contextKey", "")
    
    logger.info(f"🔍 Определение аккаунта...")
    logger.info(f"   contextKey: {context_key[:50] if context_key else 'отсутствует'}...")
    
    # 1. Пробуем по contextKey
    if context_key:
        acc = get_account_by_context(context_key)
        if acc:
            logger.info(f"✅ Аккаунт из contextKey: {acc.get('account_name')} ({acc.get('account_id')})")
            return acc
    
    # 2. Пробуем по header
    account_id_header = request.headers.get("X-Account-Id")
    if account_id_header:
        acc = get_account(account_id_header)
        if acc and acc.get("status") == "active":
            # Сохраняем маппинг если есть contextKey
            if context_key:
                save_context_mapping(context_key, account_id_header)
            logger.info(f"✅ Аккаунт из header: {acc.get('account_name')}")
            return acc
    
    # 3. Если только один активный аккаунт - используем его
    all_accounts = get_all_active_accounts()
    if len(all_accounts) == 1:
        acc = all_accounts[0]
        if context_key:
            save_context_mapping(context_key, acc["account_id"])
        logger.info(f"✅ Единственный аккаунт: {acc.get('account_name')}")
        return acc
    
    # 4. Несколько аккаунтов - не можем определить
    if len(all_accounts) > 1:
        logger.error(f"❌ Несколько аккаунтов ({len(all_accounts)}), невозможно определить нужный!")
        logger.error(f"   Аккаунты: {[a.get('account_name') for a in all_accounts]}")
        return None
    
    logger.error("❌ Нет активных аккаунтов")
    return None


# ============== API для регистрации контекста ==============

@app.post("/api/register-context")
async def register_context(request: Request):
    """
    Регистрация связи contextKey -> account_id
    Вызывается из iframe при загрузке
    """
    body = await request.json()
    context_key = body.get("contextKey", "")
    account_id = body.get("accountId", "")
    
    if not context_key:
        return JSONResponse({"success": False, "error": "contextKey не указан"})
    
    if account_id:
        # Явно указан account_id
        acc = get_account(account_id)
        if acc and acc.get("status") == "active":
            save_context_mapping(context_key, account_id)
            return JSONResponse({
                "success": True, 
                "accountId": account_id,
                "accountName": acc.get("account_name")
            })
    
    # Пробуем определить из contextKey
    decoded = decode_context_key(context_key)
    if decoded:
        for key in ["accountId", "account_id", "accountUuid"]:
            if key in decoded:
                acc_id = decoded[key]
                acc = get_account(acc_id)
                if acc and acc.get("status") == "active":
                    save_context_mapping(context_key, acc_id)
                    return JSONResponse({
                        "success": True,
                        "accountId": acc_id,
                        "accountName": acc.get("account_name"),
                        "source": "decoded"
                    })
    
    # Если один аккаунт - привязываем к нему
    all_accounts = get_all_active_accounts()
    if len(all_accounts) == 1:
        acc = all_accounts[0]
        save_context_mapping(context_key, acc["account_id"])
        return JSONResponse({
            "success": True,
            "accountId": acc["account_id"],
            "accountName": acc.get("account_name"),
            "source": "single_account"
        })
    
    return JSONResponse({
        "success": False, 
        "error": "Не удалось определить аккаунт",
        "availableAccounts": [{"id": a["account_id"], "name": a.get("account_name")} for a in all_accounts]
    })


# ============== API ==============

@app.get("/api/expense-categories")
async def api_get_categories(request: Request):
    acc = await resolve_account_from_request(request)
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
        "accountId": account_id,
        "accountName": acc.get("account_name")
    })


@app.post("/api/expense-categories")
async def api_add_category(request: Request):
    body = await request.json()
    name = body.get("name", "").strip()
    if not name:
        return JSONResponse({"success": False, "error": "Название не указано"})
    
    acc = await resolve_account_from_request(request)
    if not acc or not acc.get("access_token"):
        return JSONResponse({"success": False, "error": "Не удалось определить аккаунт"}, status_code=400)
    
    token = acc["access_token"]
    account_id = acc["account_id"]
    
    dict_id = await ensure_dictionary(token, account_id)
    if not dict_id:
        return JSONResponse({"success": False, "error": "Нет справочника"})
    
    cat = await add_expense_category(token, dict_id, name)
    if cat:
        return JSONResponse({"success": True, "category": cat})
    return JSONResponse({"success": False, "error": "Ошибка создания категории"})


@app.post("/api/process-expenses")
async def process_expenses(request: Request):
    body = await request.json()
    expenses = body.get("expenses", [])
    category = body.get("category", "Накладные расходы")
    
    acc = await resolve_account_from_request(request)
    if not acc or not acc.get("access_token"):
        return JSONResponse({"success": False, "error": "Не удалось определить аккаунт"}, status_code=400)
    
    token = acc["access_token"]
    account_id = acc["account_id"]
    account_name = acc.get("account_name", "")
    
    logger.info("=" * 70)
    logger.info(f"📊 ОБРАБОТКА РАСХОДОВ")
    logger.info(f"   Аккаунт: {account_name} ({account_id})")
    logger.info(f"   Записей: {len(expenses)}")
    logger.info(f"   Категория: {category}")
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
    
    logger.info(f"✅ Успешно: {len(results)}, ❌ Ошибок: {len(errors)}")
    
    return JSONResponse({
        "success": True,
        "processed": len(results),
        "errors": len(errors),
        "results": results,
        "errorDetails": errors,
        "accountId": account_id,
        "accountName": account_name
    })


# ============== Отладка ==============

@app.get("/api/debug")
async def debug(request: Request):
    context_key = request.query_params.get("contextKey", "")
    acc = await resolve_account_from_request(request)
    
    accounts_data = load_accounts()
    settings = load_settings()
    contexts = load_contexts()
    
    all_accounts = []
    for acc_id, acc_data in accounts_data.get("accounts", {}).items():
        all_accounts.append({
            "id": acc_id,
            "name": acc_data.get("account_name"),
            "status": acc_data.get("status"),
            "has_token": bool(acc_data.get("access_token")),
            "activated_at": acc_data.get("activated_at")
        })
    
    return JSONResponse({
        "resolved_account": {
            "id": acc.get("account_id") if acc else None,
            "name": acc.get("account_name") if acc else None,
            "has_token": bool(acc.get("access_token")) if acc else False
        } if acc else None,
        "context_key_provided": bool(context_key),
        "context_key_preview": context_key[:50] + "..." if len(context_key) > 50 else context_key,
        "all_accounts": all_accounts,
        "total_accounts": len(all_accounts),
        "active_accounts": len([a for a in all_accounts if a["status"] == "active"]),
        "context_mappings_count": len(contexts.get("mappings", {})),
        "server_time_msk": now_msk().strftime("%Y-%m-%d %H:%M:%S"),
        "root_path": ROOT_PATH
    })


@app.get("/api/accounts")
async def list_accounts():
    """Список всех аккаунтов (для отладки)"""
    accounts_data = load_accounts()
    result = []
    for acc_id, acc in accounts_data.get("accounts", {}).items():
        result.append({
            "id": acc_id,
            "name": acc.get("account_name"),
            "status": acc.get("status"),
            "has_token": bool(acc.get("access_token")),
            "activated_at": acc.get("activated_at"),
            "dictionary_id": get_dictionary_id(acc_id)
        })
    return JSONResponse({"accounts": result})


@app.get("/api/test-demand/{demand_name}")
async def test_demand(demand_name: str, request: Request):
    acc = await resolve_account_from_request(request)
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
        "sum": full.get("sum"),
        "accountId": acc.get("account_id"),
        "accountName": acc.get("account_name")
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
    all_accounts = get_all_active_accounts()
    return {
        "app": "Накладные расходы",
        "version": "3.2",
        "distribution": "price",
        "timezone": "MSK (UTC+3)",
        "server_time": now_msk().strftime("%Y-%m-%d %H:%M:%S"),
        "root_path": ROOT_PATH,
        "base_url": "https://kulps.ru/expensesms",
        "active_accounts": len(all_accounts),
        "accounts": [{"name": a.get("account_name"), "id": a.get("account_id")} for a in all_accounts]
    }


@app.get("/health")
async def health():
    return {"status": "healthy", "accounts": len(get_all_active_accounts())}


@app.middleware("http")
async def mw(request: Request, call_next):
    logger.info(f"➡️ {request.method} {request.url.path}")
    response = await call_next(request)
    response.headers["X-Frame-Options"] = "ALLOWALL"
    response.headers["Content-Security-Policy"] = "frame-ancestors *"
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response