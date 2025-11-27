"""
Приложение для МойСклад - ExpenseSMS
Обрабатывает Vendor API запросы и отображает iframe виджет
"""

import json
import base64
import hashlib
import hmac
import logging
from datetime import datetime
from typing import Optional
from pathlib import Path

from fastapi import FastAPI, Request, HTTPException, Response
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Создаём FastAPI приложение
app = FastAPI(
    title="ExpenseSMS - МойСклад App",
    description="Приложение для интеграции с МойСклад",
    version="1.0.0"
)

# Шаблоны для HTML страниц
templates = Jinja2Templates(directory="templates")

# Хранилище данных аккаунтов (в продакшене используйте базу данных!)
# Ключ: accountId, Значение: данные аккаунта
accounts_storage: dict = {}


# ============== Модели данных ==============

class AccessToken(BaseModel):
    """Токен доступа от МойСклад"""
    id: str
    accountId: str
    access_token: str


class AppStatus(BaseModel):
    """Статус приложения"""
    status: str


class AccountInfo(BaseModel):
    """Информация об аккаунте"""
    accountId: str
    infoVersion: int
    appUid: Optional[str] = None


# ============== Vendor API Endpoints ==============

@app.put("/api/moysklad/vendor/1.0/apps/{appId}/{accountId}")
async def activate_app(appId: str, accountId: str, request: Request):
    """
    Активация приложения (МойСклад вызывает при установке)
    
    МойСклад отправляет этот запрос когда пользователь устанавливает приложение.
    Мы должны сохранить access_token для дальнейших запросов к API МойСклад.
    """
    try:
        body = await request.json()
        logger.info(f"🟢 Активация приложения: appId={appId}, accountId={accountId}")
        logger.info(f"Полученные данные: {json.dumps(body, indent=2, ensure_ascii=False)}")
        
        # Сохраняем данные аккаунта
        accounts_storage[accountId] = {
            "appId": appId,
            "accountId": accountId,
            "access_token": body.get("access", [{}])[0].get("access_token") if body.get("access") else None,
            "activated_at": datetime.now().isoformat(),
            "status": "Activated"
        }
        
        # Возвращаем статус активации
        return JSONResponse(
            status_code=200,
            content={"status": "Activated"}
        )
        
    except Exception as e:
        logger.error(f"❌ Ошибка активации: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/moysklad/vendor/1.0/apps/{appId}/{accountId}")
async def deactivate_app(appId: str, accountId: str):
    """
    Деактивация приложения (МойСклад вызывает при удалении)
    
    Пользователь удалил приложение - очищаем данные.
    """
    logger.info(f"🔴 Деактивация приложения: appId={appId}, accountId={accountId}")
    
    # Удаляем данные аккаунта
    if accountId in accounts_storage:
        del accounts_storage[accountId]
    
    return Response(status_code=200)


@app.get("/api/moysklad/vendor/1.0/apps/{appId}/{accountId}/status")
async def get_app_status(appId: str, accountId: str):
    """
    Проверка статуса приложения
    
    МойСклад периодически проверяет, работает ли приложение.
    """
    logger.info(f"📊 Проверка статуса: appId={appId}, accountId={accountId}")
    
    if accountId in accounts_storage:
        return JSONResponse(
            status_code=200,
            content={"status": "Activated"}
        )
    else:
        return JSONResponse(
            status_code=200,
            content={"status": "SettingsRequired"}
        )


# ============== iframe Endpoints ==============

@app.get("/iframe/customer-order", response_class=HTMLResponse)
async def customer_order_iframe(request: Request):
    """
    iframe виджет для карточки заказа покупателя
    
    Этот HTML отображается внутри МойСклад в карточке заказа.
    """
    # Получаем параметры из URL
    context_key = request.query_params.get("contextKey", "")
    
    logger.info(f"📦 Загрузка iframe заказа: contextKey={context_key}")
    
    return templates.TemplateResponse(
        "iframe.html",
        {
            "request": request,
            "title": "ExpenseSMS - Заказ покупателя",
            "context_key": context_key,
            "widget_type": "customer_order"
        }
    )


@app.get("/iframe/settings", response_class=HTMLResponse)
async def settings_iframe(request: Request):
    """
    iframe для настроек приложения
    
    Здесь пользователь может настроить параметры приложения.
    """
    context_key = request.query_params.get("contextKey", "")
    
    logger.info(f"⚙️ Загрузка iframe настроек: contextKey={context_key}")
    
    return templates.TemplateResponse(
        "iframe.html",
        {
            "request": request,
            "title": "ExpenseSMS - Настройки",
            "context_key": context_key,
            "widget_type": "settings"
        }
    )


# ============== Служебные Endpoints ==============

@app.get("/")
async def root():
    """Корневой endpoint - проверка работоспособности"""
    return {
        "app": "ExpenseSMS",
        "status": "running",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat()
    }


@app.get("/health")
async def health_check():
    """Health check для мониторинга"""
    return {"status": "healthy"}


@app.get("/debug/accounts")
async def debug_accounts():
    """
    Отладочный endpoint - показывает все подключённые аккаунты
    ⚠️ В продакшене уберите или защитите паролем!
    """
    return {
        "total_accounts": len(accounts_storage),
        "accounts": list(accounts_storage.keys())
    }


# ============== Обработка ошибок ==============

@app.exception_handler(404)
async def not_found_handler(request: Request, exc):
    """Обработка 404 ошибок"""
    logger.warning(f"404 Not Found: {request.url}")
    return JSONResponse(
        status_code=404,
        content={"error": "Not Found", "path": str(request.url)}
    )


@app.exception_handler(500)
async def server_error_handler(request: Request, exc):
    """Обработка серверных ошибок"""
    logger.error(f"500 Server Error: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={"error": "Internal Server Error"}
    )


# ============== Middleware для логирования ==============

@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Логируем все входящие запросы"""
    logger.info(f"➡️ {request.method} {request.url}")
    
    response = await call_next(request)
    
    logger.info(f"⬅️ {request.method} {request.url} - Status: {response.status_code}")
    return response