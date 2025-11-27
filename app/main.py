"""
Приложение для МойСклад - Массовое занесение накладных расходов
"""

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


# ============== Vendor API ==============

@app.put("/api/moysklad/vendor/1.0/apps/{app_id}/{account_id}")
async def activate_app(app_id: str, account_id: str, request: Request):
    """Активация приложения"""
    try:
        body = await request.json()
        logger.info(f"🟢 Активация: account_id={account_id}")
        logger.info(f"📦 Body: {json.dumps(body, ensure_ascii=False, indent=2)}")
        
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
        
        logger.info(f"✅ Аккаунт сохранён: {account_id}")
        
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


# ============== Iframe (главное окно приложения) ==============

@app.get("/iframe", response_class=HTMLResponse)
async def iframe_page(request: Request):
    """
    Главный iframe приложения.
    Открывается при клике на приложение в меню МойСклад.
    С expand=true открывается как popup.
    """
    context_key = request.query_params.get("contextKey", "")
    logger.info(f"📱 Открыт iframe: contextKey={context_key[:50]}..." if context_key else "📱 Открыт iframe")
    
    return templates.TemplateResponse("iframe.html", {
        "request": request,
        "context_key": context_key
    })


# ============== Widget в карточке отгрузки ==============

@app.get("/widget-demand", response_class=HTMLResponse)
async def widget_demand(request: Request):
    """
    Виджет в карточке отгрузки.
    Показывает кнопку для открытия popup.
    """
    context_key = request.query_params.get("contextKey", "")
    logger.info(f"📦 Виджет отгрузки загружен")
    
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


# ============== Popup для занесения расходов ==============

@app.get("/popup-expenses", response_class=HTMLResponse)
async def popup_expenses(request: Request):
    """Popup окно для массового занесения расходов"""
    context_key = request.query_params.get("contextKey", "")
    logger.info(f"💰 Открыт popup расходов")
    
    return templates.TemplateResponse("popup_expenses.html", {
        "request": request,
        "context_key": context_key
    })


# ============== API для обработки расходов ==============

@app.post("/api/process-expenses")
async def process_expenses(request: Request):
    """
    Обработка и занесение расходов в отгрузки.
    Получает список {demandNumber, expense} и обновляет отгрузки через API МойСклад.
    """
    try:
        body = await request.json()
        expenses_data = body.get("expenses", [])
        context_key = body.get("contextKey", "")
        
        logger.info(f"📊 Получено {len(expenses_data)} записей для обработки")
        
        results = []
        errors = []
        
        for item in expenses_data:
            demand_number = item.get("demandNumber", "").strip()
            expense_value = item.get("expense")
            
            if not demand_number:
                continue
            
            try:
                # TODO: Здесь будет логика обновления через API МойСклад
                # 1. Найти отгрузку по номеру
                # 2. Обновить поле накладных расходов
                
                results.append({
                    "demandNumber": demand_number,
                    "expense": expense_value,
                    "status": "success"
                })
                logger.info(f"✅ Обработано: {demand_number} = {expense_value}")
                
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


# Middleware для логирования и заголовков
@app.middleware("http")
async def add_headers_and_log(request: Request, call_next):
    logger.info(f"➡️ {request.method} {request.url.path}")
    
    response = await call_next(request)
    
    # Разрешаем встраивание в iframe МойСклад
    response.headers["X-Frame-Options"] = "ALLOWALL"
    response.headers["Content-Security-Policy"] = "frame-ancestors *"
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "*"
    
    logger.info(f"⬅️ {response.status_code}")
    return response