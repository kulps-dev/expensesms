import json
import logging
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import httpx

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Накладные расходы - МойСклад")

templates = Jinja2Templates(directory="templates")

# Хранилище токенов аккаунтов
accounts_storage: dict = {}

# Название справочника статей расходов
EXPENSE_CATEGORY_ENTITY_NAME = "Статьи расходов"


# ============== Работа с API МойСклад ==============

async def get_access_token(account_id: str) -> Optional[str]:
    """Получить токен доступа для аккаунта"""
    account = accounts_storage.get(account_id)
    if account:
        return account.get("access_token")
    return None


async def moysklad_request(method: str, url: str, token: str, data: dict = None) -> dict:
    """Выполнить запрос к API МойСклад"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip"
    }
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        if method == "GET":
            response = await client.get(url, headers=headers)
        elif method == "POST":
            response = await client.post(url, headers=headers, json=data)
        elif method == "PUT":
            response = await client.put(url, headers=headers, json=data)
        else:
            raise ValueError(f"Unknown method: {method}")
        
        logger.info(f"API {method} {url} -> {response.status_code}")
        
        if response.status_code in [200, 201]:
            return response.json()
        elif response.status_code == 404:
            return None
        else:
            logger.error(f"API Error: {response.text}")
            raise HTTPException(status_code=response.status_code, detail=response.text)


async def get_or_create_expense_entity(token: str) -> dict:
    """Получить или создать справочник статей расходов"""
    base_url = "https://api.moysklad.ru/api/remap/1.2"
    
    # Ищем существующий справочник
    search_url = f"{base_url}/entity/customentity"
    entities = await moysklad_request("GET", search_url, token)
    
    if entities and "rows" in entities:
        for entity in entities["rows"]:
            if entity.get("name") == EXPENSE_CATEGORY_ENTITY_NAME:
                logger.info(f"Найден справочник: {entity['id']}")
                return entity
    
    # Создаём новый справочник
    create_url = f"{base_url}/entity/customentity"
    new_entity = await moysklad_request("POST", create_url, token, {
        "name": EXPENSE_CATEGORY_ENTITY_NAME
    })
    
    logger.info(f"Создан справочник: {new_entity['id']}")
    return new_entity


async def get_expense_categories(token: str) -> list:
    """Получить список статей расходов"""
    base_url = "https://api.moysklad.ru/api/remap/1.2"
    
    # Получаем справочник
    entity = await get_or_create_expense_entity(token)
    entity_id = entity["id"]
    
    # Получаем элементы справочника
    url = f"{base_url}/entity/customentity/{entity_id}"
    result = await moysklad_request("GET", url, token)
    
    # Получаем элементы
    elements_url = f"{base_url}/entity/customentity/{entity_id}/element"
    elements = await moysklad_request("GET", elements_url, token)
    
    categories = []
    if elements and "rows" in elements:
        for elem in elements["rows"]:
            categories.append({
                "id": elem["id"],
                "name": elem["name"]
            })
    
    return categories


async def add_expense_category(token: str, name: str) -> dict:
    """Добавить новую статью расходов"""
    base_url = "https://api.moysklad.ru/api/remap/1.2"
    
    # Получаем справочник
    entity = await get_or_create_expense_entity(token)
    entity_id = entity["id"]
    
    # Создаём элемент
    url = f"{base_url}/entity/customentity/{entity_id}/element"
    new_element = await moysklad_request("POST", url, token, {
        "name": name
    })
    
    logger.info(f"Добавлена статья: {name}")
    return {
        "id": new_element["id"],
        "name": new_element["name"]
    }


# ============== Vendor API ==============

@app.put("/api/moysklad/vendor/1.0/apps/{app_id}/{account_id}")
async def activate_app(app_id: str, account_id: str, request: Request):
    """Активация приложения"""
    try:
        body = await request.json()
        logger.info(f"🟢 Активация: account_id={account_id}")
        
        # Сохраняем токен доступа
        access_token = None
        if body.get("access"):
            for access in body["access"]:
                if access.get("access_token"):
                    access_token = access["access_token"]
                    break
        
        accounts_storage[account_id] = {
            "app_id": app_id,
            "account_name": body.get("accountName", ""),
            "access_token": access_token,
            "activated_at": datetime.now().isoformat()
        }
        
        logger.info(f"✅ Аккаунт сохранён: {account_id}, token: {'есть' if access_token else 'нет'}")
        
        # Создаём справочник при активации
        if access_token:
            try:
                await get_or_create_expense_entity(access_token)
            except Exception as e:
                logger.error(f"Ошибка создания справочника: {e}")
        
        return JSONResponse({"status": "Activated"})
    except Exception as e:
        logger.error(f"❌ Ошибка активации: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/moysklad/vendor/1.0/apps/{app_id}/{account_id}")
async def deactivate_app(app_id: str, account_id: str):
    """Деактивация приложения"""
    logger.info(f"🔴 Деактивация: {account_id}")
    accounts_storage.pop(account_id, None)
    return JSONResponse(status_code=200, content={})


@app.get("/api/moysklad/vendor/1.0/apps/{app_id}/{account_id}/status")
async def get_status(app_id: str, account_id: str):
    """Статус приложения"""
    if account_id in accounts_storage:
        return JSONResponse({"status": "Activated"})
    return JSONResponse({"status": "SettingsRequired"})


# ============== API для статей расходов ==============

@app.get("/api/expense-categories")
async def api_get_categories(request: Request):
    """Получить список статей расходов"""
    context_key = request.query_params.get("contextKey", "")
    
    # Находим токен по любому аккаунту (для простоты)
    token = None
    for acc_id, acc_data in accounts_storage.items():
        if acc_data.get("access_token"):
            token = acc_data["access_token"]
            break
    
    if not token:
        return JSONResponse({"categories": [], "error": "No token"})
    
    try:
        categories = await get_expense_categories(token)
        return JSONResponse({"categories": categories})
    except Exception as e:
        logger.error(f"Ошибка получения категорий: {e}")
        return JSONResponse({"categories": [], "error": str(e)})


@app.post("/api/expense-categories")
async def api_add_category(request: Request):
    """Добавить новую статью расходов"""
    body = await request.json()
    name = body.get("name", "").strip()
    
    if not name:
        return JSONResponse({"success": False, "error": "Название не указано"})
    
    # Находим токен
    token = None
    for acc_id, acc_data in accounts_storage.items():
        if acc_data.get("access_token"):
            token = acc_data["access_token"]
            break
    
    if not token:
        return JSONResponse({"success": False, "error": "No token"})
    
    try:
        category = await add_expense_category(token, name)
        return JSONResponse({"success": True, "category": category})
    except Exception as e:
        logger.error(f"Ошибка добавления категории: {e}")
        return JSONResponse({"success": False, "error": str(e)})


# ============== Iframe ==============

@app.get("/iframe", response_class=HTMLResponse)
async def iframe_page(request: Request):
    """Главный iframe приложения"""
    context_key = request.query_params.get("contextKey", "")
    return templates.TemplateResponse("iframe.html", {
        "request": request,
        "context_key": context_key
    })


# ============== Widget ==============

@app.get("/widget-demand", response_class=HTMLResponse)
async def widget_demand(request: Request):
    """Виджет в карточке отгрузки"""
    context_key = request.query_params.get("contextKey", "")
    return templates.TemplateResponse("widget_demand.html", {
        "request": request,
        "context_key": context_key
    })


@app.post("/widget-demand/open-feedback")
async def widget_open_feedback(request: Request):
    """Open feedback для виджета"""
    body = await request.json()
    logger.info(f"📬 Widget open-feedback: {json.dumps(body, ensure_ascii=False)}")
    return JSONResponse({"status": "ok"})


# ============== API для обработки расходов ==============

@app.post("/api/process-expenses")
async def process_expenses(request: Request):
    """Обработка и занесение расходов в отгрузки"""
    try:
        body = await request.json()
        expenses_data = body.get("expenses", [])
        category = body.get("category", "Накладные расходы")
        
        logger.info(f"📊 Получено {len(expenses_data)} записей, категория: {category}")
        
        results = []
        errors = []
        
        for item in expenses_data:
            demand_number = item.get("demandNumber", "").strip()
            expense_value = item.get("expense")
            comment = item.get("comment", "")
            
            if not demand_number:
                continue
            
            try:
                # TODO: Здесь будет логика обновления через API МойСклад
                results.append({
                    "demandNumber": demand_number,
                    "expense": expense_value,
                    "comment": comment,
                    "status": "success"
                })
                logger.info(f"✅ Обработано: {demand_number} = {expense_value} ({comment})")
                
            except Exception as e:
                errors.append({
                    "demandNumber": demand_number,
                    "error": str(e)
                })
                logger.error(f"❌ Ошибка {demand_number}: {e}")
        
        return JSONResponse({
            "success": True,
            "processed": len(results),
            "errors": len(errors),
            "results": results,
            "errorDetails": errors
        })
        
    except Exception as e:
        logger.error(f"❌ Ошибка обработки: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": str(e)}
        )


# ============== Служебные endpoints ==============

@app.get("/")
async def root():
    return {"app": "Накладные расходы", "version": "1.0", "status": "running"}


@app.get("/health")
async def health():
    return {"status": "healthy"}


# Middleware
@app.middleware("http")
async def add_headers_and_log(request: Request, call_next):
    logger.info(f"➡️ {request.method} {request.url.path}")
    
    response = await call_next(request)
    
    response.headers["X-Frame-Options"] = "ALLOWALL"
    response.headers["Content-Security-Policy"] = "frame-ancestors *"
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "*"
    
    logger.info(f"⬅️ {response.status_code}")
    return response