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

BASE_API_URL = "https://api.moysklad.ru/api/remap/1.2"
EXPENSE_DICTIONARY_NAME = "Статьи накладных расходов"


# ============== Хранилище аккаунтов ==============

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
                # Объект уже существует
                return {"exists": True, "status_code": 412}
            else:
                return {"error": response.text[:500], "status_code": response.status_code}
                
        except Exception as e:
            logger.error(f"MS API Exception: {e}")
            return {"error": str(e)}


# ============== Справочник статей расходов ==============

async def get_all_custom_entities(token: str) -> List[dict]:
    """Получить список всех пользовательских справочников"""
    # Правильный endpoint для получения списка справочников
    result = await ms_request("GET", "/entity/customentity", token)
    
    if result and "rows" in result:
        return result["rows"]
    
    return []


async def find_expense_dictionary(token: str) -> Optional[dict]:
    """Найти справочник статей расходов по имени"""
    entities = await get_all_custom_entities(token)
    
    for entity in entities:
        if entity.get("name") == EXPENSE_DICTIONARY_NAME:
            logger.info(f"✅ Найден справочник: {entity['id']}")
            return entity
    
    return None


async def create_expense_dictionary(token: str) -> Optional[dict]:
    """Создать справочник статей расходов"""
    logger.info(f"📝 Создание справочника: {EXPENSE_DICTIONARY_NAME}")
    
    result = await ms_request("POST", "/entity/customentity", token, {
        "name": EXPENSE_DICTIONARY_NAME
    })
    
    if result:
        if result.get("exists"):
            # Справочник уже существует, ищем его
            logger.info("Справочник уже существует, ищем...")
            return await find_expense_dictionary(token)
        elif "id" in result:
            logger.info(f"✅ Справочник создан: {result['id']}")
            return result
    
    return None


async def get_or_create_expense_dictionary(token: str) -> Optional[dict]:
    """Получить или создать справочник"""
    # Сначала пробуем найти
    dictionary = await find_expense_dictionary(token)
    if dictionary:
        return dictionary
    
    # Если не нашли - создаём
    return await create_expense_dictionary(token)


async def get_expense_categories(token: str) -> List[dict]:
    """Получить элементы справочника (статьи расходов)"""
    dictionary = await get_or_create_expense_dictionary(token)
    
    if not dictionary:
        logger.error("Справочник не найден и не создан")
        return []
    
    dict_id = dictionary["id"]
    logger.info(f"📋 Загрузка элементов справочника: {dict_id}")
    
    # Получаем элементы справочника
    result = await ms_request("GET", f"/entity/customentity/{dict_id}", token)
    
    categories = []
    
    if result and "id" in result:
        # Элементы могут быть в meta.href
        # Запрашиваем элементы отдельно
        elements_result = await ms_request("GET", f"/entity/customentity/{dict_id}/element", token)
        
        if elements_result and "rows" in elements_result:
            for elem in elements_result["rows"]:
                categories.append({
                    "id": elem.get("id"),
                    "name": elem.get("name")
                })
    
    logger.info(f"📋 Загружено {len(categories)} статей расходов")
    return categories


async def add_expense_category(token: str, name: str) -> Optional[dict]:
    """Добавить новую статью расходов в справочник"""
    dictionary = await get_or_create_expense_dictionary(token)
    
    if not dictionary:
        return None
    
    dict_id = dictionary["id"]
    
    # Создаём элемент справочника
    result = await ms_request("POST", f"/entity/customentity/{dict_id}/element", token, {
        "name": name
    })
    
    if result and "id" in result:
        logger.info(f"✅ Добавлена статья: {name}")
        return {
            "id": result["id"],
            "name": result["name"]
        }
    elif result and result.get("exists"):
        logger.info(f"Статья уже существует: {name}")
        return {"id": "exists", "name": name}
    else:
        logger.error(f"❌ Ошибка добавления статьи: {result}")
        return None


# ============== Работа с отгрузками ==============

async def search_demand(token: str, demand_name: str) -> Optional[dict]:
    """Найти отгрузку по номеру"""
    search_variants = [
        f"/entity/demand?filter=name={demand_name}",
        f"/entity/demand?filter=name~{demand_name}",
        f"/entity/demand?search={demand_name}",
    ]
    
    for endpoint in search_variants:
        result = await ms_request("GET", endpoint, token)
        
        if result and "rows" in result and len(result["rows"]) > 0:
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
        return {
            "success": True,
            "demand_id": demand_id,
            "demand_name": demand.get("name"),
            "overhead_sum": overhead_sum
        }
    else:
        return {
            "success": False,
            "error": result.get("error", "Ошибка обновления") if result else "Нет ответа"
        }


# ============== Vendor API ==============

@app.put("/api/moysklad/vendor/1.0/apps/{app_id}/{account_id}")
async def activate_app(app_id: str, account_id: str, request: Request):
    try:
        body = await request.json()
        
        logger.info(f"{'='*60}")
        logger.info(f"🟢 АКТИВАЦИЯ: {account_id}")
        logger.info(f"{'='*60}")
        
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
        logger.info(f"✅ Сохранён: {account_data['account_name']}, токен: {'✓' if access_token else '✗'}")
        
        # Создаём/проверяем справочник
        if access_token:
            try:
                dictionary = await get_or_create_expense_dictionary(access_token)
                if dictionary:
                    logger.info(f"✅ Справочник готов: {dictionary.get('id')}")
            except Exception as e:
                logger.error(f"⚠️ Ошибка справочника: {e}")
        
        return JSONResponse({"status": "Activated"})
        
    except Exception as e:
        logger.error(f"❌ Ошибка активации: {e}")
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


# ============== API для категорий ==============

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
    
    try:
        category = await add_expense_category(token, name)
        if category:
            return JSONResponse({"success": True, "category": category})
        return JSONResponse({"success": False, "error": "Не удалось добавить"})
    except Exception as e:
        return JSONResponse({"success": False, "error": str(e)})


# ============== API для обработки расходов ==============

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
        
        results = []
        errors = []
        
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
                results.append({
                    "demandNumber": demand_number,
                    "expense": expense_value,
                    "status": "success"
                })
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


# ============== Тест ==============

@app.get("/api/test-dictionary")
async def test_dictionary():
    token = get_any_token()
    if not token:
        return JSONResponse({"success": False, "error": "Нет токена"})
    
    # Получаем все справочники
    entities = await get_all_custom_entities(token)
    
    # Ищем наш
    dictionary = await find_expense_dictionary(token)
    
    # Получаем категории
    categories = []
    if dictionary:
        categories = await get_expense_categories(token)
    
    return JSONResponse({
        "success": True,
        "all_entities": [{"id": e.get("id"), "name": e.get("name")} for e in entities],
        "our_dictionary": dictionary,
        "categories": categories
    })


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
    return JSONResponse({"accounts": safe})


@app.get("/")
async def root():
    return {"app": "Накладные расходы", "version": "1.3"}


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