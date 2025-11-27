import json
import logging
from datetime import datetime
from typing import Optional, List
from pathlib import Path

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import httpx

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Накладные расходы - МойСклад")
templates = Jinja2Templates(directory="templates")

DATA_DIR = Path("/app/data")
ACCOUNTS_FILE = DATA_DIR / "accounts.json"
SETTINGS_FILE = DATA_DIR / "settings.json"

BASE_API_URL = "https://api.moysklad.ru/api/remap/1.2"
EXPENSE_DICTIONARY_NAME = "Статьи накладных расходов"


# ============== Хранилище ==============

def ensure_data_dir():
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def load_accounts() -> dict:
    ensure_data_dir()
    if ACCOUNTS_FILE.exists():
        try:
            with open(ACCOUNTS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return {"accounts": {}}


def save_accounts(data: dict):
    ensure_data_dir()
    with open(ACCOUNTS_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def save_account(account_id: str, account_data: dict):
    data = load_accounts()
    account_data["updated_at"] = datetime.now().isoformat()
    if "accounts" not in data:
        data["accounts"] = {}
    if account_id not in data["accounts"]:
        account_data["created_at"] = datetime.now().isoformat()
    data["accounts"][account_id] = account_data
    save_accounts(data)


def get_account(account_id: str) -> Optional[dict]:
    return load_accounts().get("accounts", {}).get(account_id)


def get_any_token() -> Optional[str]:
    data = load_accounts()
    for acc_id, acc_data in data.get("accounts", {}).items():
        if acc_data.get("status") == "active" and acc_data.get("access_token"):
            return acc_data["access_token"]
    return None


# ============== Настройки (ID справочника и т.д.) ==============

def load_settings() -> dict:
    ensure_data_dir()
    if SETTINGS_FILE.exists():
        try:
            with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return {}


def save_settings(settings: dict):
    ensure_data_dir()
    with open(SETTINGS_FILE, 'w', encoding='utf-8') as f:
        json.dump(settings, f, ensure_ascii=False, indent=2)


def get_dictionary_id() -> Optional[str]:
    """Получить сохранённый ID справочника"""
    settings = load_settings()
    return settings.get("expense_dictionary_id")


def save_dictionary_id(dict_id: str):
    """Сохранить ID справочника"""
    settings = load_settings()
    settings["expense_dictionary_id"] = dict_id
    settings["dictionary_saved_at"] = datetime.now().isoformat()
    save_settings(settings)
    logger.info(f"💾 Сохранён ID справочника: {dict_id}")


# ============== API МойСклад ==============

async def ms_request(method: str, endpoint: str, token: str, data: dict = None) -> Optional[dict]:
    url = f"{BASE_API_URL}{endpoint}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip"
    }
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            if method == "GET":
                response = await client.get(url, headers=headers)
            elif method == "POST":
                response = await client.post(url, headers=headers, json=data)
            elif method == "PUT":
                response = await client.put(url, headers=headers, json=data)
            else:
                return {"error": f"Unknown method: {method}"}
            
            logger.info(f"MS API {method} {endpoint} -> {response.status_code}")
            
            if response.status_code in [200, 201]:
                return response.json()
            elif response.status_code == 412:
                return {"exists": True, "status_code": 412}
            else:
                error_text = response.text[:500]
                logger.error(f"MS API Error: {error_text}")
                return {"error": error_text, "status_code": response.status_code}
                
        except Exception as e:
            logger.error(f"MS API Exception: {e}")
            return {"error": str(e)}


# ============== Справочник статей расходов ==============

async def create_expense_dictionary(token: str) -> Optional[str]:
    """Создать справочник и вернуть его ID"""
    logger.info(f"📝 Создание справочника: {EXPENSE_DICTIONARY_NAME}")
    
    result = await ms_request("POST", "/entity/customentity", token, {
        "name": EXPENSE_DICTIONARY_NAME
    })
    
    if result:
        if "id" in result:
            dict_id = result["id"]
            save_dictionary_id(dict_id)
            logger.info(f"✅ Справочник создан: {dict_id}")
            return dict_id
        elif result.get("exists"):
            logger.info("⚠️ Справочник уже существует")
            # Возвращаем сохранённый ID если есть
            return get_dictionary_id()
    
    return None


async def get_or_create_dictionary_id(token: str) -> Optional[str]:
    """Получить ID справочника (из кэша или создать новый)"""
    # Сначала проверяем сохранённый ID
    dict_id = get_dictionary_id()
    
    if dict_id:
        # Проверяем что справочник существует
        result = await ms_request("GET", f"/entity/customentity/{dict_id}", token)
        if result and "id" in result:
            logger.info(f"✅ Справочник найден по ID: {dict_id}")
            return dict_id
        else:
            logger.warning(f"⚠️ Справочник {dict_id} не найден, создаём новый")
    
    # Создаём новый
    return await create_expense_dictionary(token)


async def get_expense_categories(token: str) -> List[dict]:
    """Получить элементы справочника"""
    dict_id = await get_or_create_dictionary_id(token)
    
    if not dict_id:
        logger.error("❌ Нет ID справочника")
        return []
    
    logger.info(f"📋 Загрузка элементов справочника: {dict_id}")
    
    # Получаем элементы
    result = await ms_request("GET", f"/entity/customentity/{dict_id}/element", token)
    
    categories = []
    if result and "rows" in result:
        for elem in result["rows"]:
            categories.append({
                "id": elem.get("id"),
                "name": elem.get("name")
            })
        logger.info(f"✅ Загружено {len(categories)} статей")
    
    return categories


async def add_expense_category(token: str, name: str) -> Optional[dict]:
    """Добавить новую статью расходов"""
    dict_id = await get_or_create_dictionary_id(token)
    
    if not dict_id:
        return None
    
    result = await ms_request("POST", f"/entity/customentity/{dict_id}/element", token, {
        "name": name
    })
    
    if result and "id" in result:
        logger.info(f"✅ Добавлена статья: {name}")
        return {"id": result["id"], "name": result["name"]}
    elif result and result.get("exists"):
        return {"id": "exists", "name": name}
    else:
        logger.error(f"❌ Ошибка: {result}")
        return None


# ============== Работа с отгрузками ==============

async def search_demand(token: str, demand_name: str) -> Optional[dict]:
    """Найти отгрузку по номеру"""
    # Пробуем разные варианты поиска
    for endpoint in [
        f"/entity/demand?filter=name={demand_name}",
        f"/entity/demand?filter=name~{demand_name}",
        f"/entity/demand?search={demand_name}",
    ]:
        result = await ms_request("GET", endpoint, token)
        if result and "rows" in result and result["rows"]:
            for row in result["rows"]:
                if demand_name in row.get("name", ""):
                    return row
            return result["rows"][0]
    return None


async def update_demand(token: str, demand_id: str, overhead_sum: float, comment: str) -> dict:
    """Обновить накладные расходы в отгрузке"""
    demand = await ms_request("GET", f"/entity/demand/{demand_id}", token)
    
    if not demand or "error" in demand:
        return {"success": False, "error": "Отгрузка не найдена"}
    
    current_desc = demand.get("description", "") or ""
    new_desc = f"{current_desc}\n{comment}".strip() if current_desc else comment
    
    update_data = {
        "description": new_desc,
        "overhead": {
            "sum": int(overhead_sum * 100),
            "distribution": "weight"
        }
    }
    
    result = await ms_request("PUT", f"/entity/demand/{demand_id}", token, update_data)
    
    if result and "id" in result:
        return {"success": True, "demand_name": demand.get("name")}
    return {"success": False, "error": result.get("error", "Ошибка")}


# ============== Vendor API ==============

@app.put("/api/moysklad/vendor/1.0/apps/{app_id}/{account_id}")
async def activate_app(app_id: str, account_id: str, request: Request):
    try:
        body = await request.json()
        
        logger.info("=" * 60)
        logger.info(f"🟢 АКТИВАЦИЯ: {account_id}")
        logger.info("=" * 60)
        
        access_token = None
        if body.get("access"):
            for access in body["access"]:
                if access.get("access_token"):
                    access_token = access["access_token"]
                    break
        
        subscription = body.get("subscription", {})
        
        account_data = {
            "app_id": app_id,
            "account_id": account_id,
            "app_uid": body.get("appUid", ""),
            "account_name": body.get("accountName", ""),
            "status": "active",
            "access_token": access_token,
            "tariff_name": subscription.get("tariffName"),
            "activated_at": datetime.now().isoformat(),
        }
        
        save_account(account_id, account_data)
        logger.info(f"✅ Сохранён: {account_data['account_name']}")
        
        # Создаём справочник
        if access_token:
            dict_id = await get_or_create_dictionary_id(access_token)
            if dict_id:
                logger.info(f"✅ Справочник готов: {dict_id}")
        
        return JSONResponse({"status": "Activated"})
        
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/moysklad/vendor/1.0/apps/{app_id}/{account_id}")
async def deactivate_app(app_id: str, account_id: str):
    logger.info(f"🔴 ДЕАКТИВАЦИЯ: {account_id}")
    account = get_account(account_id)
    if account:
        account["status"] = "inactive"
        account["access_token"] = None
        save_account(account_id, account)
    return JSONResponse(status_code=200, content={})


@app.get("/api/moysklad/vendor/1.0/apps/{app_id}/{account_id}/status")
async def get_status(app_id: str, account_id: str):
    account = get_account(account_id)
    if account and account.get("status") == "active":
        return JSONResponse({"status": "Activated"})
    return JSONResponse({"status": "SettingsRequired"})


# ============== API категорий ==============

@app.get("/api/expense-categories")
async def api_get_categories():
    token = get_any_token()
    if not token:
        return JSONResponse({"categories": [], "error": "Нет токена"})
    
    try:
        categories = await get_expense_categories(token)
        return JSONResponse({"categories": categories})
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        return JSONResponse({"categories": [], "error": str(e)})


@app.post("/api/expense-categories")
async def api_add_category(request: Request):
    body = await request.json()
    name = body.get("name", "").strip()
    
    if not name:
        return JSONResponse({"success": False, "error": "Название не указано"})
    
    token = get_any_token()
    if not token:
        return JSONResponse({"success": False, "error": "Нет токена"})
    
    category = await add_expense_category(token, name)
    if category:
        return JSONResponse({"success": True, "category": category})
    return JSONResponse({"success": False, "error": "Не удалось добавить"})


# ============== API обработки расходов ==============

@app.post("/api/process-expenses")
async def process_expenses(request: Request):
    try:
        body = await request.json()
        expenses_data = body.get("expenses", [])
        category = body.get("category", "Накладные расходы")
        
        logger.info(f"📊 ОБРАБОТКА: {len(expenses_data)} записей")
        
        token = get_any_token()
        if not token:
            return JSONResponse({"success": False, "error": "Нет токена"})
        
        results, errors = [], []
        
        for item in expenses_data:
            demand_number = item.get("demandNumber", "").strip()
            expense_value = float(item.get("expense", 0))
            comment = item.get("comment", f"{expense_value:.2f} руб - {category}")
            
            if not demand_number:
                continue
            
            demand = await search_demand(token, demand_number)
            if not demand:
                errors.append({"demandNumber": demand_number, "error": "Не найдена"})
                continue
            
            result = await update_demand(token, demand["id"], expense_value, comment)
            if result["success"]:
                results.append({"demandNumber": demand_number, "status": "success"})
                logger.info(f"✅ {demand_number} = {expense_value}")
            else:
                errors.append({"demandNumber": demand_number, "error": result["error"]})
        
        return JSONResponse({
            "success": True,
            "processed": len(results),
            "errors": len(errors),
            "results": results,
            "errorDetails": errors
        })
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})


# ============== Тест и отладка ==============

@app.get("/api/test-dictionary")
async def test_dictionary():
    """Тест справочника"""
    token = get_any_token()
    settings = load_settings()
    
    if not token:
        return JSONResponse({"success": False, "error": "Нет токена", "settings": settings})
    
    dict_id = await get_or_create_dictionary_id(token)
    categories = await get_expense_categories(token) if dict_id else []
    
    return JSONResponse({
        "success": bool(dict_id),
        "dictionary_id": dict_id,
        "categories": categories,
        "settings": settings
    })


@app.get("/api/set-dictionary-id/{dict_id}")
async def set_dictionary_id(dict_id: str):
    """Вручную установить ID справочника"""
    save_dictionary_id(dict_id)
    return JSONResponse({"success": True, "dictionary_id": dict_id})


# ============== Iframe и Widget ==============

@app.get("/iframe", response_class=HTMLResponse)
async def iframe_page(request: Request):
    return templates.TemplateResponse("iframe.html", {"request": request})


@app.get("/widget-demand", response_class=HTMLResponse)
async def widget_demand(request: Request):
    return templates.TemplateResponse("widget_demand.html", {"request": request})


# ============== Служебные ==============

@app.get("/api/accounts")
async def api_get_accounts():
    data = load_accounts()
    safe = {}
    for acc_id, acc in data.get("accounts", {}).items():
        s = acc.copy()
        if s.get("access_token"):
            s["access_token"] = "***" + s["access_token"][-8:]
        safe[acc_id] = s
    return JSONResponse({"accounts": safe, "settings": load_settings()})


@app.get("/")
async def root():
    return {"app": "Накладные расходы", "version": "1.4", "settings": load_settings()}


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.middleware("http")
async def middleware(request: Request, call_next):
    logger.info(f"➡️ {request.method} {request.url.path}")
    response = await call_next(request)
    response.headers["X-Frame-Options"] = "ALLOWALL"
    response.headers["Content-Security-Policy"] = "frame-ancestors *"
    response.headers["Access-Control-Allow-Origin"] = "*"
    logger.info(f"⬅️ {response.status_code}")
    return response