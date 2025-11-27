import json
import logging
import os
from datetime import datetime
from typing import Optional, List
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
CATEGORIES_FILE = DATA_DIR / "expense_categories.json"

BASE_API_URL = "https://api.moysklad.ru/api/remap/1.2"


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
                logger.info(f"📂 Загружено {len(data.get('accounts', {}))} аккаунтов")
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
        logger.info(f"💾 Сохранено {len(data.get('accounts', {}))} аккаунтов")
    except Exception as e:
        logger.error(f"❌ Ошибка записи файла: {e}")


def get_account(account_id: str) -> Optional[dict]:
    """Получить данные аккаунта"""
    data = load_accounts()
    return data.get("accounts", {}).get(account_id)


def save_account(account_id: str, account_data: dict):
    """Сохранить данные аккаунта"""
    data = load_accounts()
    
    if account_id in data["accounts"]:
        data["accounts"][account_id].update(account_data)
        data["accounts"][account_id]["updated_at"] = datetime.now().isoformat()
    else:
        account_data["created_at"] = datetime.now().isoformat()
        account_data["updated_at"] = datetime.now().isoformat()
        data["accounts"][account_id] = account_data
    
    data["history"].append({
        "timestamp": datetime.now().isoformat(),
        "action": "update",
        "account_id": account_id
    })
    data["history"] = data["history"][-100:]
    
    save_accounts(data)


def get_any_token() -> Optional[str]:
    """Получить любой активный токен"""
    data = load_accounts()
    for acc_id, acc_data in data.get("accounts", {}).items():
        if acc_data.get("status") == "active" and acc_data.get("access_token"):
            return acc_data["access_token"]
    return None


# ============== Локальное хранилище категорий ==============

def load_categories() -> List[dict]:
    """Загрузить категории из локального файла"""
    ensure_data_dir()
    
    if CATEGORIES_FILE.exists():
        try:
            with open(CATEGORIES_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    
    # Дефолтные категории
    return [
        {"id": "1", "name": "Доставка"},
        {"id": "2", "name": "Упаковка"},
        {"id": "3", "name": "Страховка"},
    ]


def save_categories(categories: List[dict]):
    """Сохранить категории в локальный файл"""
    ensure_data_dir()
    
    with open(CATEGORIES_FILE, 'w', encoding='utf-8') as f:
        json.dump(categories, f, ensure_ascii=False, indent=2)


def add_category(name: str) -> dict:
    """Добавить новую категорию"""
    categories = load_categories()
    
    # Проверяем дубликат
    for cat in categories:
        if cat["name"].lower() == name.lower():
            return cat
    
    # Генерируем ID
    max_id = max([int(c["id"]) for c in categories], default=0)
    new_cat = {"id": str(max_id + 1), "name": name}
    categories.append(new_cat)
    save_categories(categories)
    
    return new_cat


# ============== Работа с API МойСклад ==============

async def moysklad_api(method: str, endpoint: str, token: str, data: dict = None) -> Optional[dict]:
    """Универсальный метод для работы с API МойСклад"""
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
                raise ValueError(f"Unknown method: {method}")
            
            logger.info(f"API {method} {endpoint} -> {response.status_code}")
            
            if response.status_code in [200, 201]:
                return response.json()
            elif response.status_code == 404:
                logger.warning(f"API 404: {endpoint}")
                return None
            else:
                logger.error(f"API Error {response.status_code}: {response.text[:500]}")
                return {"error": response.text, "status_code": response.status_code}
                
        except Exception as e:
            logger.error(f"API Exception: {e}")
            return {"error": str(e)}


async def search_demand_by_name(token: str, demand_name: str) -> Optional[dict]:
    """Найти отгрузку по номеру/имени"""
    # Пробуем точный поиск
    endpoint = f"/entity/demand?filter=name={demand_name}"
    result = await moysklad_api("GET", endpoint, token)
    
    if result and "rows" in result and len(result["rows"]) > 0:
        return result["rows"][0]
    
    # Пробуем поиск с ~
    endpoint = f"/entity/demand?filter=name~{demand_name}"
    result = await moysklad_api("GET", endpoint, token)
    
    if result and "rows" in result and len(result["rows"]) > 0:
        return result["rows"][0]
    
    # Пробуем поиск по search
    endpoint = f"/entity/demand?search={demand_name}"
    result = await moysklad_api("GET", endpoint, token)
    
    if result and "rows" in result and len(result["rows"]) > 0:
        # Ищем точное совпадение в результатах
        for row in result["rows"]:
            if demand_name in row.get("name", ""):
                return row
        # Возвращаем первый результат
        return result["rows"][0]
    
    return None


async def update_demand_overhead(token: str, demand_id: str, overhead_sum: float, comment: str) -> dict:
    """Обновить накладные расходы в отгрузке"""
    
    # Сначала получаем текущую отгрузку
    endpoint = f"/entity/demand/{demand_id}"
    demand = await moysklad_api("GET", endpoint, token)
    
    if not demand or "error" in demand:
        return {"success": False, "error": "Отгрузка не найдена"}
    
    # Получаем текущий комментарий
    current_description = demand.get("description", "")
    
    # Добавляем новый комментарий
    if current_description:
        new_description = f"{current_description}\n{comment}"
    else:
        new_description = comment
    
    # Формируем данные для обновления
    update_data = {
        "description": new_description,
        "overhead": {
            "sum": int(overhead_sum * 100),  # В копейках
            "distribution": "weight"  # или "price" или "volume"
        }
    }
    
    # Обновляем отгрузку
    result = await moysklad_api("PUT", endpoint, token, update_data)
    
    if result and "error" not in result:
        return {
            "success": True,
            "demand_id": demand_id,
            "demand_name": demand.get("name"),
            "overhead_sum": overhead_sum,
            "comment": comment
        }
    else:
        return {
            "success": False,
            "error": result.get("error", "Неизвестная ошибка") if result else "Нет ответа"
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
        
        subscription = body.get("subscription", {})
        
        account_data = {
            "app_id": app_id,
            "account_id": account_id,
            "app_uid": body.get("appUid", ""),
            "account_name": body.get("accountName", ""),
            "cause": body.get("cause", ""),
            "status": "active",
            "access_token": access_token,
            "resource": resource,
            "scope": scope,
            "permissions": permissions,
            "tariff_id": subscription.get("tariffId"),
            "tariff_name": subscription.get("tariffName"),
            "is_trial": subscription.get("trial", False),
            "expiry_moment": subscription.get("expiryMoment"),
            "activated_at": datetime.now().isoformat(),
        }
        
        save_account(account_id, account_data)
        
        logger.info(f"✅ Аккаунт сохранён: {account_id}")
        logger.info(f"   Account Name: {account_data['account_name']}")
        logger.info(f"   Tariff: {account_data['tariff_name']}")
        logger.info(f"   Token: {'✓' if access_token else '✗'}")
        
        return JSONResponse({"status": "Activated"})
        
    except Exception as e:
        logger.error(f"❌ Ошибка активации: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/moysklad/vendor/1.0/apps/{app_id}/{account_id}")
async def deactivate_app(app_id: str, account_id: str):
    """Деактивация приложения"""
    logger.info(f"🔴 ДЕАКТИВАЦИЯ: {account_id}")
    
    account = get_account(account_id)
    if account:
        account["status"] = "inactive"
        account["deactivated_at"] = datetime.now().isoformat()
        account["access_token"] = None
        save_account(account_id, account)
    
    return JSONResponse(status_code=200, content={})


@app.get("/api/moysklad/vendor/1.0/apps/{app_id}/{account_id}/status")
async def get_status(app_id: str, account_id: str):
    """Статус приложения"""
    account = get_account(account_id)
    
    if account and account.get("status") == "active":
        return JSONResponse({"status": "Activated"})
    
    return JSONResponse({"status": "SettingsRequired"})


# ============== API для категорий (локальное хранилище) ==============

@app.get("/api/expense-categories")
async def api_get_categories():
    """Получить список категорий расходов"""
    categories = load_categories()
    return JSONResponse({"categories": categories})


@app.post("/api/expense-categories")
async def api_add_category(request: Request):
    """Добавить новую категорию"""
    body = await request.json()
    name = body.get("name", "").strip()
    
    if not name:
        return JSONResponse({"success": False, "error": "Название не указано"})
    
    category = add_category(name)
    logger.info(f"✅ Добавлена категория: {name}")
    
    return JSONResponse({"success": True, "category": category})


# ============== API для обработки расходов ==============

@app.post("/api/process-expenses")
async def process_expenses(request: Request):
    """Обработка и занесение расходов в отгрузки"""
    try:
        body = await request.json()
        expenses_data = body.get("expenses", [])
        category = body.get("category", "Накладные расходы")
        
        logger.info(f"{'='*60}")
        logger.info(f"📊 ОБРАБОТКА РАСХОДОВ")
        logger.info(f"{'='*60}")
        logger.info(f"Записей: {len(expenses_data)}, Категория: {category}")
        
        token = get_any_token()
        
        if not token:
            return JSONResponse({
                "success": False,
                "error": "Нет активного токена. Переустановите приложение в МойСклад."
            })
        
        results = []
        errors = []
        
        for item in expenses_data:
            demand_number = item.get("demandNumber", "").strip()
            expense_value = float(item.get("expense", 0))
            comment = item.get("comment", f"{expense_value} руб - {category}")
            
            if not demand_number:
                continue
            
            logger.info(f"🔍 Поиск отгрузки: {demand_number}")
            
            # Ищем отгрузку
            demand = await search_demand_by_name(token, demand_number)
            
            if not demand:
                error_msg = f"Отгрузка не найдена: {demand_number}"
                logger.warning(f"⚠️ {error_msg}")
                errors.append({
                    "demandNumber": demand_number,
                    "error": error_msg
                })
                continue
            
            demand_id = demand["id"]
            demand_name = demand.get("name", demand_number)
            
            logger.info(f"✓ Найдена: {demand_name} (ID: {demand_id})")
            
            # Обновляем отгрузку
            update_result = await update_demand_overhead(
                token, 
                demand_id, 
                expense_value, 
                comment
            )
            
            if update_result["success"]:
                logger.info(f"✅ Обновлено: {demand_name} = {expense_value} руб")
                results.append({
                    "demandNumber": demand_number,
                    "demandName": demand_name,
                    "expense": expense_value,
                    "comment": comment,
                    "status": "success"
                })
            else:
                logger.error(f"❌ Ошибка: {update_result['error']}")
                errors.append({
                    "demandNumber": demand_number,
                    "error": update_result["error"]
                })
        
        logger.info(f"{'='*60}")
        logger.info(f"📊 ИТОГО: Успешно {len(results)}, Ошибок {len(errors)}")
        logger.info(f"{'='*60}")
        
        return JSONResponse({
            "success": True,
            "processed": len(results),
            "errors": len(errors),
            "results": results,
            "errorDetails": errors
        })
        
    except Exception as e:
        logger.error(f"❌ Ошибка обработки: {e}")
        import traceback
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": str(e)}
        )


# ============== Тестовый endpoint для проверки API ==============

@app.get("/api/test-moysklad")
async def test_moysklad():
    """Тест подключения к МойСклад"""
    token = get_any_token()
    
    if not token:
        return JSONResponse({
            "success": False,
            "error": "Нет активного токена"
        })
    
    # Тестируем получение отгрузок
    result = await moysklad_api("GET", "/entity/demand?limit=1", token)
    
    if result and "rows" in result:
        return JSONResponse({
            "success": True,
            "message": "Подключение работает",
            "demands_count": result.get("meta", {}).get("size", 0)
        })
    else:
        return JSONResponse({
            "success": False,
            "error": result.get("error") if result else "Нет ответа"
        })


@app.get("/api/search-demand/{demand_name}")
async def api_search_demand(demand_name: str):
    """Поиск отгрузки по номеру (для тестирования)"""
    token = get_any_token()
    
    if not token:
        return JSONResponse({"success": False, "error": "Нет токена"})
    
    demand = await search_demand_by_name(token, demand_name)
    
    if demand:
        return JSONResponse({
            "success": True,
            "demand": {
                "id": demand["id"],
                "name": demand.get("name"),
                "description": demand.get("description"),
                "sum": demand.get("sum"),
                "overhead": demand.get("overhead")
            }
        })
    else:
        return JSONResponse({
            "success": False,
            "error": f"Отгрузка '{demand_name}' не найдена"
        })


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


# ============== API для просмотра данных ==============

@app.get("/api/accounts")
async def api_get_accounts():
    """Получить список аккаунтов (для отладки)"""
    data = load_accounts()
    
    safe_accounts = {}
    for acc_id, acc_data in data.get("accounts", {}).items():
        safe_acc = acc_data.copy()
        if safe_acc.get("access_token"):
            safe_acc["access_token"] = "***" + safe_acc["access_token"][-8:]
        safe_accounts[acc_id] = safe_acc
    
    return JSONResponse({
        "accounts": safe_accounts,
        "total": len(safe_accounts)
    })


# ============== Служебные endpoints ==============

@app.get("/")
async def root():
    data = load_accounts()
    return {
        "app": "Накладные расходы",
        "version": "1.1",
        "status": "running",
        "accounts_count": len(data.get("accounts", {}))
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}


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