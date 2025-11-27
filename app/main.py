import json
import logging
import os
from datetime import datetime
from typing import Optional
from pathlib import Path

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

# Путь к файлу хранилища
DATA_DIR = Path("/app/data")
ACCOUNTS_FILE = DATA_DIR / "accounts.json"

# Название справочника статей расходов
EXPENSE_CATEGORY_ENTITY_NAME = "Статьи расходов"


# ============== Работа с хранилищем ==============

def ensure_data_dir():
    """Создать директорию для данных если не существует"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def load_accounts() -> dict:
    """Загрузить данные аккаунтов из файла"""
    ensure_data_dir()
    
    if ACCOUNTS_FILE.exists():
        try:
            with open(ACCOUNTS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                logger.info(f"📂 Загружено {len(data.get('accounts', {}))} аккаунтов из файла")
                return data
        except Exception as e:
            logger.error(f"❌ Ошибка чтения файла: {e}")
            return {"accounts": {}, "history": []}
    
    return {"accounts": {}, "history": []}


def save_accounts(data: dict):
    """Сохранить данные аккаунтов в файл"""
    ensure_data_dir()
    
    try:
        with open(ACCOUNTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"💾 Сохранено {len(data.get('accounts', {}))} аккаунтов в файл")
    except Exception as e:
        logger.error(f"❌ Ошибка записи файла: {e}")


def get_account(account_id: str) -> Optional[dict]:
    """Получить данные аккаунта"""
    data = load_accounts()
    return data.get("accounts", {}).get(account_id)


def save_account(account_id: str, account_data: dict):
    """Сохранить данные аккаунта"""
    data = load_accounts()
    
    # Обновляем или добавляем аккаунт
    if account_id in data["accounts"]:
        # Обновляем существующий
        data["accounts"][account_id].update(account_data)
        data["accounts"][account_id]["updated_at"] = datetime.now().isoformat()
    else:
        # Новый аккаунт
        account_data["created_at"] = datetime.now().isoformat()
        account_data["updated_at"] = datetime.now().isoformat()
        data["accounts"][account_id] = account_data
    
    # Добавляем в историю
    data["history"].append({
        "timestamp": datetime.now().isoformat(),
        "action": "update",
        "account_id": account_id
    })
    
    # Ограничиваем историю последними 100 записями
    data["history"] = data["history"][-100:]
    
    save_accounts(data)


def delete_account(account_id: str):
    """Удалить аккаунт"""
    data = load_accounts()
    
    if account_id in data["accounts"]:
        del data["accounts"][account_id]
        
        data["history"].append({
            "timestamp": datetime.now().isoformat(),
            "action": "delete",
            "account_id": account_id
        })
        
        save_accounts(data)
        logger.info(f"🗑️ Аккаунт {account_id} удалён")


def get_any_token() -> Optional[str]:
    """Получить любой активный токен"""
    data = load_accounts()
    for acc_id, acc_data in data.get("accounts", {}).items():
        if acc_data.get("status") == "active" and acc_data.get("access_token"):
            return acc_data["access_token"]
    return None


# ============== Работа с API МойСклад ==============

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
    
    entity = await get_or_create_expense_entity(token)
    entity_id = entity["id"]
    
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
    
    entity = await get_or_create_expense_entity(token)
    entity_id = entity["id"]
    
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
        
        logger.info(f"{'='*60}")
        logger.info(f"🟢 АКТИВАЦИЯ ПРИЛОЖЕНИЯ")
        logger.info(f"{'='*60}")
        logger.info(f"App ID: {app_id}")
        logger.info(f"Account ID: {account_id}")
        logger.info(f"Body: {json.dumps(body, ensure_ascii=False, indent=2)}")
        
        # Извлекаем все данные
        access_token = None
        resource = None
        scope = None
        permissions = None
        
        if body.get("access"):
            for access in body["access"]:
                if access.get("access_token"):
                    access_token = access["access_token"]
                    resource = access.get("resource")
                    scope = access.get("scope")
                    permissions = access.get("permissions")
                    break
        
        # Данные подписки
        subscription = body.get("subscription", {})
        
        # Формируем данные аккаунта
        account_data = {
            "app_id": app_id,
            "account_id": account_id,
            "app_uid": body.get("appUid", ""),
            "account_name": body.get("accountName", ""),
            "cause": body.get("cause", ""),
            "status": "active",
            
            # Токен и доступ
            "access_token": access_token,
            "resource": resource,
            "scope": scope,
            "permissions": permissions,
            
            # Подписка
            "tariff_id": subscription.get("tariffId"),
            "tariff_name": subscription.get("tariffName"),
            "is_trial": subscription.get("trial", False),
            "not_for_resale": subscription.get("notForResale", False),
            "is_partner": subscription.get("partner", False),
            
            # Метаданные
            "activated_at": datetime.now().isoformat(),
            "last_request_at": datetime.now().isoformat(),
        }
        
        # Сохраняем
        save_account(account_id, account_data)
        
        logger.info(f"✅ Аккаунт сохранён: {account_id}")
        logger.info(f"   Account Name: {account_data['account_name']}")
        logger.info(f"   Tariff: {account_data['tariff_name']}")
        logger.info(f"   Token: {'✓' if access_token else '✗'}")
        
        # Создаём справочник при активации
        if access_token:
            try:
                await get_or_create_expense_entity(access_token)
                logger.info("✅ Справочник статей расходов создан/найден")
            except Exception as e:
                logger.error(f"⚠️ Ошибка создания справочника: {e}")
        
        return JSONResponse({"status": "Activated"})
        
    except Exception as e:
        logger.error(f"❌ Ошибка активации: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/moysklad/vendor/1.0/apps/{app_id}/{account_id}")
async def deactivate_app(app_id: str, account_id: str):
    """Деактивация приложения"""
    logger.info(f"{'='*60}")
    logger.info(f"🔴 ДЕАКТИВАЦИЯ ПРИЛОЖЕНИЯ")
    logger.info(f"{'='*60}")
    logger.info(f"App ID: {app_id}")
    logger.info(f"Account ID: {account_id}")
    
    # Помечаем как неактивный (не удаляем данные)
    account = get_account(account_id)
    if account:
        account["status"] = "inactive"
        account["deactivated_at"] = datetime.now().isoformat()
        account["access_token"] = None  # Удаляем токен
        save_account(account_id, account)
    
    return JSONResponse(status_code=200, content={})


@app.get("/api/moysklad/vendor/1.0/apps/{app_id}/{account_id}/status")
async def get_status(app_id: str, account_id: str):
    """Статус приложения"""
    account = get_account(account_id)
    
    if account and account.get("status") == "active":
        # Обновляем время последнего запроса
        account["last_request_at"] = datetime.now().isoformat()
        save_account(account_id, account)
        return JSONResponse({"status": "Activated"})
    
    return JSONResponse({"status": "SettingsRequired"})


# ============== API для просмотра данных ==============

@app.get("/api/accounts")
async def api_get_accounts():
    """Получить список всех аккаунтов (для отладки)"""
    data = load_accounts()
    
    # Скрываем токены в ответе
    safe_accounts = {}
    for acc_id, acc_data in data.get("accounts", {}).items():
        safe_acc = acc_data.copy()
        if safe_acc.get("access_token"):
            safe_acc["access_token"] = "***" + safe_acc["access_token"][-8:]
        safe_accounts[acc_id] = safe_acc
    
    return JSONResponse({
        "accounts": safe_accounts,
        "total": len(safe_accounts),
        "history_count": len(data.get("history", []))
    })


@app.get("/api/accounts/{account_id}")
async def api_get_account(account_id: str):
    """Получить данные конкретного аккаунта"""
    account = get_account(account_id)
    
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    
    # Скрываем токен
    safe_acc = account.copy()
    if safe_acc.get("access_token"):
        safe_acc["access_token"] = "***" + safe_acc["access_token"][-8:]
    
    return JSONResponse(safe_acc)


# ============== API для статей расходов ==============

@app.get("/api/expense-categories")
async def api_get_categories(request: Request):
    """Получить список статей расходов"""
    token = get_any_token()
    
    if not token:
        return JSONResponse({"categories": [], "error": "No active accounts"})
    
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
    
    token = get_any_token()
    
    if not token:
        return JSONResponse({"success": False, "error": "No active accounts"})
    
    try:
        category = await add_expense_category(token, name)
        return JSONResponse({"success": True, "category": category})
    except Exception as e:
        logger.error(f"Ошибка добавления категории: {e}")
        return JSONResponse({"success": False, "error": str(e)})


# ============== Iframe и Widget ==============

@app.get("/iframe", response_class=HTMLResponse)
async def iframe_page(request: Request):
    """Главный iframe приложения"""
    context_key = request.query_params.get("contextKey", "")
    return templates.TemplateResponse("iframe.html", {
        "request": request,
        "context_key": context_key
    })


@app.get("/widget-demand", response_class=HTMLResponse)
async def widget_demand(request: Request):
    """Виджет в карточке отгрузки"""
    context_key = request.query_params.get("contextKey", "")
    return templates.TemplateResponse("widget_demand.html", {
        "request": request,
        "context_key": context_key
    })


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
    data = load_accounts()
    return {
        "app": "Накладные расходы",
        "version": "1.0",
        "status": "running",
        "accounts_count": len(data.get("accounts", {}))
    }


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