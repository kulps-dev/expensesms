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
import jwt  # PyJWT
import uuid

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

# JSON API МоегоСклада
BASE_API_URL = "https://api.moysklad.ru/api/remap/1.2"
DICTIONARY_NAME = "Статьи накладных расходов"

# Vendor API МоегоСклада
VENDOR_BASE_URL = "https://apps-api.moysklad.ru/api/vendor/1.0"
APP_UID = os.getenv("MS_APP_UID", "expenses-1-snjph.kulps")  # appUid решения
APP_ID = os.getenv("MS_APP_ID", "b3e6c54d-d4b4-4694-9ee2-3701c3aea973")  # UUID приложения
SECRET_KEY = os.getenv("MS_SECRET_KEY", "")  # Секрет из ЛК разработчика

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
        except Exception as e:
            logger.warning(f"Ошибка чтения {path}: {e}")
    return default


def save_json(path: Path, data: dict):
    ensure_data_dir()
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_accounts():
    return load_json(ACCOUNTS_FILE, {"accounts": {}})


def save_accounts(data):
    save_json(ACCOUNTS_FILE, data)


def load_settings():
    return load_json(SETTINGS_FILE, {"accounts_settings": {}})


def save_settings(data):
    save_json(SETTINGS_FILE, data)


def load_context_map():
    return load_json(CONTEXT_MAP_FILE, {"map": {}})


def save_context_map(data):
    save_json(CONTEXT_MAP_FILE, data)


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
    if not context_key or not account_id:
        return

    acc = get_account(account_id)
    if not acc or acc.get("status") != "active" or not acc.get("access_token"):
        logger.warning(f"⚠️ Попытка сохранить маппинг для неактивного аккаунта: {account_id}")
        return

    data = load_context_map()
    data["map"][context_key] = {
        "account_id": account_id,
        "account_name": acc.get("account_name", ""),
        "created_at": now_msk().isoformat()
    }

    if len(data["map"]) > 10000:
        sorted_keys = sorted(
            data["map"].keys(),
            key=lambda k: data["map"][k].get("created_at", "")
        )
        for k in sorted_keys[:len(sorted_keys) - 10000]:
            del data["map"][k]

    save_context_map(data)
    logger.info(f"📌 Маппинг: {context_key[:20]}... -> {account_id} ({acc.get('account_name')})")


def get_account_id_from_context(context_key: str) -> Optional[str]:
    if not context_key:
        return None

    data = load_context_map()
    mapping = data.get("map", {}).get(context_key)
    if not mapping:
        return None

    account_id = mapping.get("account_id")
    acc = get_account(account_id)
    if not acc or acc.get("status") != "active" or not acc.get("access_token"):
        logger.warning(f"⚠️ Кешированный аккаунт {account_id} неактивен, удаляем маппинг")
        del data["map"][context_key]
        save_context_map(data)
        return None

    return account_id


# ============== JWT для Vendor API ==============

def make_vendor_jwt() -> str:
    """
    Генерирует одноразовый JWT для Vendor API МоегоСклада.
    sub = appUid, alg=HS256, jti=uuid, iat/exp по документации.
    """
    if not SECRET_KEY or not APP_UID:
        raise RuntimeError("Не настроены MS_SECRET_KEY и/или MS_APP_UID")

    now = int(datetime.utcnow().timestamp())
    payload = {
        "sub": APP_UID,
        "iat": now,
        "exp": now + 60 * 5,  # 5 минут
        "jti": str(uuid.uuid4()),
    }

    token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
    return token


async def ms_get_context_by_context_key(context_key: str) -> Optional[dict]:
    """
    Получаем контекст пользователя/аккаунта по contextKey через Vendor API.
    Использует JWT на основе SECRET_KEY и APP_UID.
    Ожидаем, что в ответе будет accountId.
    """
    if not context_key:
        return None

    if not APP_ID:
        logger.error("❌ Не задан MS_APP_ID (UUID приложения), не могу вызвать Vendor context API")
        return None

    try:
        token = make_vendor_jwt()
    except Exception as e:
        logger.error(f"❌ Не удалось сгенерировать JWT для Vendor API: {e}")
        return None

    url = f"{VENDOR_BASE_URL}/apps/{APP_ID}/context"
    params = {"contextKey": context_key}
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Encoding": "gzip",
    }

    logger.info(f"🌐 Вызов Vendor context API: {url} ? contextKey={context_key[:20]}...")
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.get(url, headers=headers, params=params)
        except Exception as e:
            logger.error(f"❌ Ошибка запроса контекста Vendor API: {e}")
            return None

    try:
        data = resp.json()
    except Exception:
        logger.error(f"❌ Не удалось распарсить JSON контекста: {resp.text[:500]}")
        return None

    if resp.status_code != 200:
        logger.error(f"❌ Ошибка Vendor context API: {resp.status_code} {data}")
        return None

    logger.info(f"📥 Контекст от Vendor API по contextKey: {context_key[:20]}... -> {data}")
    return data


# ============== API МойСклад (JSON API) ==============

async def ms_api(method: str, endpoint: str, token: str, data: dict = None) -> dict:
    url = f"{BASE_API_URL}{endpoint}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip"
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
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
            except Exception:
                result = {"_text": resp.text[:1000]}

            result["_status"] = resp.status_code
            return result
        except Exception as e:
            return {"_error": str(e), "_status": 0}


# ============== Определение аккаунта ==============

async def resolve_account(request: Request) -> Optional[dict]:
    """
    Строгая схема:
    1) Если явно передан accountId в query → используем его.
    2) Иначе пробуем contextKey → accountId из кеша.
    3) Иначе пробуем получить accountId по contextKey через Vendor API (apps/{appId}/context)
       и сразу сохраняем маппинг.
    4) Если в хранилище всего один активный аккаунт → используем его как fallback.
    5) Если активных аккаунтов >1 и ничего не определили → возвращаем None.
    """
    context_key = request.query_params.get("contextKey", "")
    account_id_hint = request.query_params.get("accountId", "")

    logger.info("🔍 Определение аккаунта...")
    logger.info(f"   contextKey: {context_key[:30] + '...' if context_key else 'нет'}")
    logger.info(f"   accountId hint: {account_id_hint or 'нет'}")

    # 1. Прямой accountId из query
    if account_id_hint:
        acc = get_account(account_id_hint)
        if acc and acc.get("status") == "active" and acc.get("access_token"):
            logger.info(f"✅ Аккаунт по hint: {acc.get('account_name')} ({account_id_hint})")
            if context_key:
                save_context_mapping(context_key, account_id_hint)
            return acc
        else:
            logger.warning(f"⚠️ Hint accountId {account_id_hint} неактивен или нет токена")

    # 2. contextKey → accountId из кеша
    if context_key:
        cached_account_id = get_account_id_from_context(context_key)
        if cached_account_id:
            acc = get_account(cached_account_id)
            if acc:
                logger.info(f"✅ Аккаунт из кеша по contextKey: {acc.get('account_name')} ({cached_account_id})")
                return acc
            else:
                logger.warning(f"⚠️ В кеше есть account_id {cached_account_id}, но аккаунт не найден")

    # 3. Пробуем получить контекст через Vendor API по contextKey
    if context_key:
        ctx = await ms_get_context_by_context_key(context_key)
        if ctx:
            vendor_account_id = (
                ctx.get("accountId")
                or (ctx.get("account") or {}).get("id")
                or ctx.get("accountUuid")
            )
            if vendor_account_id:
                acc = get_account(vendor_account_id)
                if acc and acc.get("status") == "active" and acc.get("access_token"):
                    logger.info(
                        f"✅ Аккаунт по Vendor context API: "
                        f"{acc.get('account_name')} ({vendor_account_id})"
                    )
                    save_context_mapping(context_key, vendor_account_id)
                    return acc
                else:
                    logger.warning(
                        f"⚠️ Vendor context дал accountId {vendor_account_id}, "
                        f"но в локальном хранилище этот аккаунт неактивен или без токена"
                    )
            else:
                logger.warning(f"⚠️ В ответе Vendor context нет accountId: {ctx}")

    # 4. Fallback: если активный аккаунт только один — используем его
    all_accounts = get_all_active_accounts()
    logger.info(f"📊 Активных аккаунтов: {len(all_accounts)}")

    if len(all_accounts) == 0:
        logger.error("❌ Нет активных аккаунтов вообще")
        return None

    if len(all_accounts) == 1:
        acc = all_accounts[0]
        logger.info(f"✅ Единственный активный аккаунт: {acc.get('account_name')} ({acc.get('account_id')})")
        if context_key:
            save_context_mapping(context_key, acc["account_id"])
        return acc

    # 5. Несколько активных аккаунтов и нет однозначного accountId/contextKey/VendorContext
    logger.error(
        "❌ Несколько активных аккаунтов и нет однозначного accountId/contextKey/VendorContext. "
        "Возвращаем None, чтобы не использовать чужой токен."
    )
    return None


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

    logger.error(f"❌ Не удалось создать/получить справочник: {result}")
    return None


async def get_expense_categories(token: str, dict_id: str) -> List[dict]:
    result = await ms_api("GET", f"/entity/customentity/{dict_id}", token)
    categories = []
    if result.get("_status") == 200 and "rows" in result:
        for elem in result["rows"]:
            categories.append({"id": elem.get("id"), "name": elem.get("name")})
    else:
        logger.warning(f"⚠️ Не удалось получить элементы справочника: {result}")
    return categories


async def add_expense_category(token: str, dict_id: str, name: str) -> Optional[dict]:
    result = await ms_api("POST", f"/entity/customentity/{dict_id}", token, {"name": name})
    if result.get("_status") in [200, 201] and result.get("id"):
        return {"id": result["id"], "name": result.get("name", name)}
    if result.get("_status") == 412:
        return {"id": "exists", "name": name}
    logger.error(f"❌ Ошибка создания элемента справочника: {result}")
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


# ============== Vendor API входящие (активация/деактивация) ==============

@app.put("/api/moysklad/vendor/1.0/apps/{app_id}/{account_id}")
async def activate_app(app_id: str, account_id: str, request: Request):
    body = await request.json()
    account_name = body.get("accountName", "")

    logger.info("=" * 70)
    logger.info(f"🟢 АКТИВАЦИЯ: {account_name} ({account_id})")
    logger.info("=" * 70)

    token = None    # access_token от МойСклад JSON API
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

    all_acc = get_all_active_accounts()
    logger.info(f"📊 Всего активных: {len(all_acc)}")
    for a in all_acc:
        logger.info(f"   - {a.get('account_name')} ({a.get('account_id')})")

    return JSONResponse({"status": "Activated"})


@app.delete("/api/moysklad/vendor/1.0/apps/{app_id}/{account_id}")
async def deactivate_app(app_id: str, account_id: str, request: Request):
    body = await request.json()
    account_name = body.get("accountName", "")

    logger.info("=" * 70)
    logger.info(f"🔴 ДЕАКТИВАЦИЯ: {account_name} ({account_id})")
    logger.info("=" * 70)

    acc = get_account(account_id)
    if acc:
        acc["status"] = "inactive"
        acc["access_token"] = None
        acc["deactivated_at"] = now_msk().isoformat()
        save_account(account_id, acc)

    context_map = load_context_map()
    keys_to_remove = [k for k, v in context_map.get("map", {}).items()
                      if v.get("account_id") == account_id]
    for k in keys_to_remove:
        del context_map["map"][k]
    save_context_map(context_map)
    logger.info(f"🧹 Очищено {len(keys_to_remove)} маппингов")

    return JSONResponse(status_code=200, content={})


@app.get("/api/moysklad/vendor/1.0/apps/{app_id}/{account_id}/status")
async def get_status(app_id: str, account_id: str):
    acc = get_account(account_id)
    status = "Activated" if acc and acc.get("status") == "active" else "SettingsRequired"
    return JSONResponse({"status": status})


# ============== API приложения ==============

@app.get("/api/expense-categories")
async def api_get_categories(request: Request):
    acc = await resolve_account(request)

    if not acc:
        return JSONResponse({
            "categories": [],
            "error": "Не удалось определить аккаунт. Переустановите приложение или откройте его из виджета/маркетплейса.",
            "needsReinstall": True
        }, status_code=400)

    if not acc.get("access_token"):
        return JSONResponse({"categories": [], "error": "Нет токена доступа"}, status_code=401)

    token = acc["access_token"]
    account_id = acc["account_id"]

    dict_id = await ensure_dictionary(token, account_id)
    if not dict_id:
        return JSONResponse({"categories": [], "error": "Не удалось создать или получить справочник"}, status_code=500)

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
        return JSONResponse({"success": False, "error": "Не удалось получить справочник"}, status_code=500)

    cat = await add_expense_category(token, dict_id, name)
    if cat:
        return JSONResponse({"success": True, "category": cat})
    return JSONResponse({"success": False, "error": "Ошибка создания элемента"})


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

    results = []
    errors = []

    for item in expenses:
        num = (item.get("demandNumber") or "").strip()
        try:
            val = float(item.get("expense", 0))
        except Exception:
            val = 0

        if not num or val <= 0:
            continue

        demand = await search_demand(token, num)
        if not demand:
            errors.append({"demandNumber": num, "error": "Не найдена отгрузка"})
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


# ============== Iframe / Виджет ==============

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
        "version": "5.3",
        "active_accounts": len(all_accounts),
        "accounts": [a.get("account_name") for a in all_accounts],
        "server_time": now_msk().strftime("%Y-%m-%d %H:%M:%S")
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}


# ============== Middleware ==============

@app.middleware("http")
async def mw(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Frame-Options"] = "ALLOWALL"
    response.headers["Content-Security-Policy"] = "frame-ancestors *"
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "*"
    return response