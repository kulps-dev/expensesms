import json
import logging
from datetime import datetime
from typing import Optional, List
from pathlib import Path

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import httpx

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="Накладные расходы - МойСклад")
templates = Jinja2Templates(directory="templates")

DATA_DIR = Path("/app/data")
ACCOUNTS_FILE = DATA_DIR / "accounts.json"
SETTINGS_FILE = DATA_DIR / "settings.json"

BASE_API_URL = "https://api.moysklad.ru/api/remap/1.2"
DICTIONARY_NAME = "Статьи накладных расходов"


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
def load_settings(): return load_json(SETTINGS_FILE, {})
def save_settings(data): save_json(SETTINGS_FILE, data)

def save_account(account_id: str, account_data: dict):
    data = load_accounts()
    account_data["updated_at"] = datetime.now().isoformat()
    if "accounts" not in data:
        data["accounts"] = {}
    data["accounts"][account_id] = account_data
    save_accounts(data)

def get_account(account_id: str):
    return load_accounts().get("accounts", {}).get(account_id)

def get_any_token():
    for acc in load_accounts().get("accounts", {}).values():
        if acc.get("status") == "active" and acc.get("access_token"):
            return acc["access_token"]
    return None

def get_dictionary_id():
    return load_settings().get("expense_dictionary_id")

def save_dictionary_id(dict_id: str):
    settings = load_settings()
    settings["expense_dictionary_id"] = dict_id
    settings["dictionary_saved_at"] = datetime.now().isoformat()
    save_settings(settings)
    logger.info(f"💾 Сохранён ID справочника: {dict_id}")


# ============== API МойСклад ==============

async def ms_api(method: str, endpoint: str, token: str, data: dict = None) -> dict:
    url = f"{BASE_API_URL}{endpoint}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip"
    }
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        if method == "GET":
            resp = await client.get(url, headers=headers)
        elif method == "POST":
            resp = await client.post(url, headers=headers, json=data)
        elif method == "PUT":
            resp = await client.put(url, headers=headers, json=data)
        else:
            return {"_error": "Unknown method"}
        
        logger.info(f"API {method} {endpoint} -> {resp.status_code}")
        
        try:
            result = resp.json()
        except:
            result = {"_text": resp.text[:1000]}
        
        result["_status"] = resp.status_code
        return result


# ============== Справочник ==============

async def create_dictionary(token: str) -> Optional[str]:
    """Создать справочник и вернуть его ID"""
    logger.info(f"📝 Создание справочника: {DICTIONARY_NAME}")
    
    result = await ms_api("POST", "/entity/customentity", token, {
        "name": DICTIONARY_NAME
    })
    
    if result.get("_status") in [200, 201] and result.get("id"):
        dict_id = result["id"]
        save_dictionary_id(dict_id)
        logger.info(f"✅ Справочник создан: {dict_id}")
        return dict_id
    
    if result.get("_status") == 412:
        logger.info("⚠️ Справочник уже существует")
        return get_dictionary_id()
    
    return None


async def ensure_dictionary(token: str) -> Optional[str]:
    """Получить ID справочника или создать новый"""
    dict_id = get_dictionary_id()
    
    if dict_id:
        check = await ms_api("GET", f"/entity/customentity/{dict_id}", token)
        if check.get("_status") == 200:
            return dict_id
    
    return await create_dictionary(token)


async def get_expense_categories(token: str, dict_id: str) -> List[dict]:
    """Получить элементы справочника"""
    result = await ms_api("GET", f"/entity/customentity/{dict_id}", token)
    
    categories = []
    if result.get("_status") == 200 and "rows" in result:
        for elem in result["rows"]:
            categories.append({
                "id": elem.get("id"),
                "name": elem.get("name")
            })
    
    return categories


async def add_expense_category(token: str, dict_id: str, name: str) -> Optional[dict]:
    """Добавить элемент в справочник"""
    result = await ms_api("POST", f"/entity/customentity/{dict_id}", token, {
        "name": name
    })
    
    if result.get("_status") in [200, 201] and result.get("id"):
        return {"id": result["id"], "name": result.get("name", name)}
    
    if result.get("_status") == 412:
        return {"id": "exists", "name": name}
    
    return None


# ============== Отгрузки ==============

async def search_demand(token: str, name: str):
    """Поиск отгрузки по номеру"""
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
    """
    Обновить накладные расходы в отгрузке
    - Суммирует с существующими расходами
    - Добавляет запись в комментарий
    - НЕ блокирует ручное редактирование
    """
    # Получаем текущую отгрузку
    demand = await ms_api("GET", f"/entity/demand/{demand_id}", token)
    if demand.get("_status") != 200:
        return {"success": False, "error": "Отгрузка не найдена"}
    
    demand_name = demand.get("name", "")
    
    # Получаем текущие накладные расходы (в копейках)
    current_overhead = 0
    if demand.get("overhead") and demand["overhead"].get("sum"):
        current_overhead = demand["overhead"]["sum"]
    
    # Новая сумма = текущая + добавляемая (переводим в копейки)
    new_overhead = current_overhead + int(add_sum * 100)
    
    logger.info(f"📊 {demand_name}: было {current_overhead/100:.2f}, добавляем {add_sum:.2f}, будет {new_overhead/100:.2f}")
    
    # Формируем комментарий
    timestamp = datetime.now().strftime("%d.%m.%Y %H:%M")
    new_comment = f"[{timestamp}] +{add_sum:.2f} руб - {category}"
    
    current_desc = demand.get("description") or ""
    if current_desc:
        new_desc = f"{current_desc}\n{new_comment}"
    else:
        new_desc = new_comment
    
    # Обновляем отгрузку
    # Используем только sum без distribution чтобы не блокировать поле
    update_data = {
        "description": new_desc,
        "overhead": {
            "sum": new_overhead
        }
    }
    
    result = await ms_api("PUT", f"/entity/demand/{demand_id}", token, update_data)
    
    if result.get("_status") == 200:
        logger.info(f"✅ Обновлено: {demand_name}")
        return {
            "success": True,
            "demand_name": demand_name,
            "previous_overhead": current_overhead / 100,
            "added": add_sum,
            "new_overhead": new_overhead / 100
        }
    else:
        logger.error(f"❌ Ошибка: {result}")
        return {"success": False, "error": str(result)}


# ============== Vendor API ==============

@app.put("/api/moysklad/vendor/1.0/apps/{app_id}/{account_id}")
async def activate_app(app_id: str, account_id: str, request: Request):
    body = await request.json()
    
    logger.info("=" * 60)
    logger.info(f"🟢 АКТИВАЦИЯ: {account_id}")
    logger.info("=" * 60)
    
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
        "activated_at": datetime.now().isoformat()
    })
    
    logger.info(f"✅ Аккаунт сохранён, токен: {'✓' if token else '✗'}")
    
    if token:
        dict_id = await ensure_dictionary(token)
        logger.info(f"📚 Справочник: {dict_id or 'НЕ СОЗДАН'}")
    
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


# ============== API категорий ==============

@app.get("/api/expense-categories")
async def api_get_categories():
    token = get_any_token()
    if not token:
        return JSONResponse({"categories": [], "error": "Нет токена"})
    
    dict_id = await ensure_dictionary(token)
    if not dict_id:
        return JSONResponse({"categories": [], "error": "Не удалось получить справочник"})
    
    categories = await get_expense_categories(token, dict_id)
    return JSONResponse({"categories": categories})


@app.post("/api/expense-categories")
async def api_add_category(request: Request):
    body = await request.json()
    name = body.get("name", "").strip()
    
    if not name:
        return JSONResponse({"success": False, "error": "Название не указано"})
    
    token = get_any_token()
    if not token:
        return JSONResponse({"success": False, "error": "Нет токена"})
    
    dict_id = await ensure_dictionary(token)
    if not dict_id:
        return JSONResponse({"success": False, "error": "Не удалось получить справочник"})
    
    cat = await add_expense_category(token, dict_id, name)
    if cat:
        return JSONResponse({"success": True, "category": cat})
    return JSONResponse({"success": False, "error": "Не удалось добавить"})


# ============== API обработки расходов ==============

@app.post("/api/process-expenses")
async def process_expenses(request: Request):
    """Массовое занесение накладных расходов"""
    body = await request.json()
    expenses = body.get("expenses", [])
    category = body.get("category", "Накладные расходы")
    
    logger.info("=" * 60)
    logger.info(f"📊 ОБРАБОТКА РАСХОДОВ: {len(expenses)} записей")
    logger.info(f"📁 Категория: {category}")
    logger.info("=" * 60)
    
    token = get_any_token()
    if not token:
        return JSONResponse({"success": False, "error": "Нет токена"})
    
    results = []
    errors = []
    
    for item in expenses:
        demand_number = item.get("demandNumber", "").strip()
        expense_value = float(item.get("expense", 0))
        
        if not demand_number:
            continue
        
        if expense_value <= 0:
            errors.append({"demandNumber": demand_number, "error": "Сумма должна быть > 0"})
            continue
        
        # Ищем отгрузку
        demand = await search_demand(token, demand_number)
        if not demand:
            errors.append({"demandNumber": demand_number, "error": "Отгрузка не найдена"})
            logger.warning(f"⚠️ Не найдена: {demand_number}")
            continue
        
        # Обновляем накладные расходы (суммируем!)
        result = await update_demand_overhead(token, demand["id"], expense_value, category)
        
        if result["success"]:
            results.append({
                "demandNumber": demand_number,
                "demandName": result.get("demand_name"),
                "added": expense_value,
                "total": result.get("new_overhead"),
                "status": "success"
            })
        else:
            errors.append({"demandNumber": demand_number, "error": result.get("error", "Ошибка")})
    
    logger.info("=" * 60)
    logger.info(f"✅ Успешно: {len(results)}, ❌ Ошибок: {len(errors)}")
    logger.info("=" * 60)
    
    return JSONResponse({
        "success": True,
        "processed": len(results),
        "errors": len(errors),
        "results": results,
        "errorDetails": errors
    })


# ============== Отладка ==============

@app.get("/api/debug")
async def debug():
    token = get_any_token()
    dict_id = get_dictionary_id()
    
    result = {
        "has_token": bool(token),
        "dictionary_id": dict_id,
        "settings": load_settings()
    }
    
    if token and dict_id:
        categories = await get_expense_categories(token, dict_id)
        result["categories"] = categories
    
    return JSONResponse(result)


@app.get("/api/set-dictionary-id/{dict_id}")
async def set_dict_id(dict_id: str):
    save_dictionary_id(dict_id)
    return JSONResponse({"success": True, "dictionary_id": dict_id})


# ============== Iframe ==============

@app.get("/iframe", response_class=HTMLResponse)
async def iframe_page(request: Request):
    return templates.TemplateResponse("iframe.html", {"request": request})


@app.get("/widget-demand", response_class=HTMLResponse)
async def widget_demand(request: Request):
    return templates.TemplateResponse("widget_demand.html", {"request": request})


@app.get("/")
async def root():
    return {"app": "Накладные расходы", "version": "2.0", "dictionary_id": get_dictionary_id()}


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