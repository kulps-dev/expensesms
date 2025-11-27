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


# ============== Настройки ==============

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
    return load_settings().get("expense_dictionary_id")


def save_dictionary_id(dict_id: str):
    settings = load_settings()
    settings["expense_dictionary_id"] = dict_id
    settings["dictionary_saved_at"] = datetime.now().isoformat()
    save_settings(settings)
    logger.info(f"💾 Сохранён ID справочника: {dict_id}")


# ============== API МойСклад ==============

async def ms_request(method: str, endpoint: str, token: str, data: dict = None) -> dict:
    """Запрос к API с полным логированием"""
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
                return {"_error": f"Unknown method: {method}"}
            
            status = response.status_code
            logger.info(f"MS API {method} {endpoint} -> {status}")
            
            # Парсим ответ
            try:
                result = response.json()
            except:
                result = {"_raw": response.text[:500]}
            
            # Добавляем статус в результат
            result["_status"] = status
            
            if status not in [200, 201]:
                logger.error(f"MS API Error: {json.dumps(result, ensure_ascii=False)[:300]}")
            
            return result
                
        except Exception as e:
            logger.error(f"MS API Exception: {e}")
            return {"_error": str(e)}


# ============== Справочник ==============

async def get_dictionary_id_from_api(token: str) -> Optional[str]:
    """Найти ID справочника через API метаданных"""
    # Попробуем получить через context
    result = await ms_request("GET", "/context/companysettings/metadata", token)
    
    if result.get("_status") == 200:
        logger.info(f"Context metadata: {json.dumps(result, ensure_ascii=False)[:500]}")
    
    # Попробуем получить все customentity через entity metadata  
    result = await ms_request("GET", "/entity/metadata", token)
    
    if result.get("_status") == 200 and "entities" in result:
        for entity in result.get("entities", []):
            if "customentity" in entity.get("type", ""):
                logger.info(f"Found customentity: {entity}")
    
    return None


async def ensure_dictionary(token: str) -> Optional[str]:
    """Получить или создать справочник, вернуть ID"""
    
    # 1. Проверяем сохранённый ID
    saved_id = get_dictionary_id()
    if saved_id:
        logger.info(f"📂 Используем сохранённый ID: {saved_id}")
        
        # Проверяем что он валидный
        check = await ms_request("GET", f"/entity/customentity/{saved_id}", token)
        if check.get("_status") == 200 and check.get("id"):
            logger.info(f"✅ Справочник валиден: {saved_id}")
            return saved_id
        else:
            logger.warning(f"⚠️ Сохранённый ID невалиден, ищем/создаём новый")
    
    # 2. Пробуем создать новый
    logger.info(f"📝 Создание справочника: {EXPENSE_DICTIONARY_NAME}")
    
    create_result = await ms_request("POST", "/entity/customentity", token, {
        "name": EXPENSE_DICTIONARY_NAME
    })
    
    if create_result.get("_status") in [200, 201] and create_result.get("id"):
        new_id = create_result["id"]
        save_dictionary_id(new_id)
        logger.info(f"✅ Создан новый справочник: {new_id}")
        return new_id
    
    # 3. Если 412 - справочник уже существует, нужно найти его ID
    if create_result.get("_status") == 412:
        logger.info("⚠️ Справочник существует, пробуем найти ID...")
        
        # Пробуем через прямой запрос с фильтром
        # К сожалению API не поддерживает фильтр по имени для customentity
        # Нужно получить ID вручную из МойСклад
        
        logger.error("❌ Не удалось найти ID существующего справочника")
        logger.error("👉 Зайдите в МойСклад -> Настройки -> Справочники")
        logger.error(f"👉 Найдите '{EXPENSE_DICTIONARY_NAME}' и скопируйте ID из URL")
        logger.error("👉 Затем вызовите: curl https://kulps.ru/api/set-dictionary-id/ВАШ_ID")
        
        return None
    
    logger.error(f"❌ Не удалось создать справочник: {create_result}")
    return None


async def get_expense_categories(token: str) -> List[dict]:
    """Получить элементы справочника"""
    dict_id = await ensure_dictionary(token)
    
    if not dict_id:
        return []
    
    logger.info(f"📋 Загрузка элементов: {dict_id}")
    
    result = await ms_request("GET", f"/entity/customentity/{dict_id}", token)
    
    # Логируем полный ответ для отладки
    logger.info(f"📋 Ответ справочника: {json.dumps(result, ensure_ascii=False)[:500]}")
    
    categories = []
    
    # Элементы могут быть в разных местах
    if result.get("_status") == 200:
        # Пробуем получить элементы отдельным запросом
        elements_result = await ms_request("GET", f"/entity/customentity/{dict_id}/element", token)
        
        logger.info(f"📋 Ответ элементов: {json.dumps(elements_result, ensure_ascii=False)[:500]}")
        
        if elements_result.get("_status") == 200 and "rows" in elements_result:
            for elem in elements_result["rows"]:
                categories.append({
                    "id": elem.get("id"),
                    "name": elem.get("name")
                })
    
    logger.info(f"✅ Загружено {len(categories)} статей")
    return categories


async def add_expense_category(token: str, name: str) -> Optional[dict]:
    """Добавить статью расходов"""
    dict_id = await ensure_dictionary(token)
    
    if not dict_id:
        return None
    
    logger.info(f"➕ Добавление статьи '{name}' в справочник {dict_id}")
    
    result = await ms_request("POST", f"/entity/customentity/{dict_id}/element", token, {
        "name": name
    })
    
    logger.info(f"➕ Результат: {json.dumps(result, ensure_ascii=False)[:300]}")
    
    if result.get("_status") in [200, 201] and result.get("id"):
        logger.info(f"✅ Статья добавлена: {name}")
        return {"id": result["id"], "name": result["name"]}
    
    if result.get("_status") == 412:
        # Возможно элемент уже существует
        logger.info(f"⚠️ Статья возможно уже существует: {name}")
        return {"id": "exists", "name": name}
    
    logger.error(f"❌ Ошибка добавления: {result}")
    return None


# ============== Отгрузки ==============

async def search_demand(token: str, demand_name: str) -> Optional[dict]:
    for endpoint in [
        f"/entity/demand?filter=name={demand_name}",
        f"/entity/demand?filter=name~{demand_name}",
        f"/entity/demand?search={demand_name}",
    ]:
        result = await ms_request("GET", endpoint, token)
        if result.get("_status") == 200 and result.get("rows"):
            for row in result["rows"]:
                if demand_name in row.get("name", ""):
                    return row
            return result["rows"][0]
    return None


async def update_demand(token: str, demand_id: str, overhead_sum: float, comment: str) -> dict:
    demand = await ms_request("GET", f"/entity/demand/{demand_id}", token)
    
    if demand.get("_status") != 200:
        return {"success": False, "error": "Отгрузка не найдена"}
    
    current_desc = demand.get("description", "") or ""
    new_desc = f"{current_desc}\n{comment}".strip() if current_desc else comment
    
    result = await ms_request("PUT", f"/entity/demand/{demand_id}", token, {
        "description": new_desc,
        "overhead": {"sum": int(overhead_sum * 100), "distribution": "weight"}
    })
    
    if result.get("_status") == 200:
        return {"success": True, "demand_name": demand.get("name")}
    return {"success": False, "error": str(result)}


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
        
        account_data = {
            "app_id": app_id,
            "account_id": account_id,
            "app_uid": body.get("appUid", ""),
            "account_name": body.get("accountName", ""),
            "status": "active",
            "access_token": access_token,
            "tariff_name": body.get("subscription", {}).get("tariffName"),
            "activated_at": datetime.now().isoformat(),
        }
        
        save_account(account_id, account_data)
        logger.info(f"✅ Сохранён: {account_data['account_name']}")
        
        if access_token:
            dict_id = await ensure_dictionary(access_token)
            logger.info(f"📚 Справочник: {dict_id or 'НЕ СОЗДАН'}")
        
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


# ============== API ==============

@app.get("/api/expense-categories")
async def api_get_categories():
    token = get_any_token()
    if not token:
        return JSONResponse({"categories": [], "error": "Нет токена"})
    categories = await get_expense_categories(token)
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
    
    category = await add_expense_category(token, name)
    if category:
        return JSONResponse({"success": True, "category": category})
    return JSONResponse({"success": False, "error": "Не удалось добавить"})


@app.post("/api/process-expenses")
async def process_expenses(request: Request):
    try:
        body = await request.json()
        expenses_data = body.get("expenses", [])
        category = body.get("category", "Накладные расходы")
        
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
            else:
                errors.append({"demandNumber": demand_number, "error": result["error"]})
        
        return JSONResponse({
            "success": True, "processed": len(results),
            "errors": len(errors), "errorDetails": errors
        })
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})


# ============== Отладка ==============

@app.get("/api/debug")
async def debug():
    """Полная отладочная информация"""
    token = get_any_token()
    settings = load_settings()
    
    result = {
        "settings": settings,
        "has_token": bool(token),
        "dictionary_id": get_dictionary_id()
    }
    
    if token:
        # Проверяем справочник
        dict_id = get_dictionary_id()
        if dict_id:
            check = await ms_request("GET", f"/entity/customentity/{dict_id}", token)
            result["dictionary_check"] = {
                "status": check.get("_status"),
                "id": check.get("id"),
                "name": check.get("name")
            }
            
            elements = await ms_request("GET", f"/entity/customentity/{dict_id}/element", token)
            result["elements_check"] = {
                "status": elements.get("_status"),
                "count": len(elements.get("rows", []))
            }
    
    return JSONResponse(result)


@app.get("/api/set-dictionary-id/{dict_id}")
async def set_dictionary_id_endpoint(dict_id: str):
    """Вручную установить ID справочника"""
    save_dictionary_id(dict_id)
    return JSONResponse({"success": True, "dictionary_id": dict_id})


# ============== Iframe ==============

@app.get("/iframe", response_class=HTMLResponse)
async def iframe_page(request: Request):
    return templates.TemplateResponse("iframe.html", {"request": request})


@app.get("/widget-demand", response_class=HTMLResponse)
async def widget_demand(request: Request):
    return templates.TemplateResponse("widget_demand.html", {"request": request})


@app.get("/api/accounts")
async def api_get_accounts():
    data = load_accounts()
    safe = {k: {**v, "access_token": "***" + v.get("access_token", "")[-8:]} 
            for k, v in data.get("accounts", {}).items()}
    return JSONResponse({"accounts": safe, "settings": load_settings()})


@app.get("/")
async def root():
    return {"app": "Накладные расходы", "version": "1.5", "settings": load_settings()}


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