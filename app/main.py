# main.py - ВЕРСИЯ v7.2 (с админ-уведомлениями)
# Поддержка: Отгрузки, Приёмки, Перемещения
# Автосоздание статей из Excel, Telegram уведомления, точный поиск
# Выбор валюты для комментариев
# Админ-уведомления о активациях/деактивациях

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
import jwt
import uuid
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ROOT_PATH = os.getenv("ROOT_PATH", "/expensesms")
APP_ID = os.getenv("APP_ID", "")
APP_SECRET = os.getenv("APP_SECRET", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

# Ник администратора для служебных уведомлений (например, "@kulps_dev")
ADMIN_TELEGRAM_USERNAME = os.getenv("ADMIN_TELEGRAM_USERNAME", "@kulps_dev")

app = FastAPI(title="Накладные расходы - МойСклад", root_path=ROOT_PATH)
templates = Jinja2Templates(directory="templates")

DATA_DIR = Path("/app/data")
LOGS_DIR = DATA_DIR / "logs"
ACCOUNTS_FILE = DATA_DIR / "accounts.json"
SETTINGS_FILE = DATA_DIR / "settings.json"
CONTEXT_MAP_FILE = DATA_DIR / "context_map.json"
TELEGRAM_USERS_FILE = DATA_DIR / "telegram_users.json"
USER_SETTINGS_FILE = DATA_DIR / "user_settings.json"

BASE_API_URL = "https://api.moysklad.ru/api/remap/1.2"
VENDOR_API_BASE = "https://apps-api.moysklad.ru/api/vendor/1.0"
DICTIONARY_NAME = "Статьи накладных расходов"

MSK = timezone(timedelta(hours=3))

# Символы валют для отображения
CURRENCY_SYMBOLS = {
    'руб': '₽',
    'USD': '$',
    'EUR': '€',
    'CNY': '¥',
    'KZT': '₸',
    'BYN': 'Br',
    'UAH': '₴',
    'UZS': 'сум',
    'GEL': '₾',
    'AMD': '֏',
    'TRY': '₺',
    'AED': 'د.إ'
}


def now_msk() -> datetime:
    return datetime.now(MSK)


def get_currency_symbol(currency: str) -> str:
    """Получить символ валюты"""
    return CURRENCY_SYMBOLS.get(currency, currency)


# ============== Хранилище ==============

def ensure_data_dir():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)


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


def load_telegram_users():
    return load_json(TELEGRAM_USERS_FILE, {"users": {}})


def save_telegram_users(data):
    save_json(TELEGRAM_USERS_FILE, data)


def load_user_settings():
    return load_json(USER_SETTINGS_FILE, {"users": {}})


def save_user_settings(data):
    save_json(USER_SETTINGS_FILE, data)


# ============== Настройки пользователя ==============

def get_user_telegram(account_id: str) -> str:
    settings = load_user_settings()
    return settings.get("users", {}).get(account_id, {}).get("telegram_username", "")


def save_user_telegram(account_id: str, telegram_username: str):
    settings = load_user_settings()
    if "users" not in settings:
        settings["users"] = {}
    if account_id not in settings["users"]:
        settings["users"][account_id] = {}
    settings["users"][account_id]["telegram_username"] = telegram_username
    settings["users"][account_id]["updated_at"] = now_msk().isoformat()
    save_user_settings(settings)


# ============== Аккаунты ==============

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


def get_account_by_app_id(app_id: str) -> Optional[dict]:
    for acc_id, acc in load_accounts().get("accounts", {}).items():
        if acc.get("app_id") == app_id and acc.get("status") == "active" and acc.get("access_token"):
            acc["account_id"] = acc_id
            return acc
    return None


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


# ============== Telegram ==============

def get_telegram_chat_id(username: str) -> Optional[int]:
    users = load_telegram_users()
    username_clean = username.lstrip("@").lower()
    return users.get("users", {}).get(username_clean, {}).get("chat_id")


def save_telegram_user(username: str, chat_id: int):
    users = load_telegram_users()
    username_clean = username.lstrip("@").lower()
    if "users" not in users:
        users["users"] = {}
    users["users"][username_clean] = {
        "chat_id": chat_id,
        "registered_at": now_msk().isoformat()
    }
    save_telegram_users(users)


# ============== Класс логирования ==============

class ProcessingLog:
    def __init__(self, account_id: str, account_name: str, year: int, category: str, 
                 doc_type: str = "demand", currency: str = "руб"):
        self.account_id = account_id
        self.account_name = account_name
        self.year = year
        self.category = category
        self.doc_type = doc_type
        self.currency = currency
        self.currency_symbol = get_currency_symbol(currency)
        self.started_at = now_msk()
        self.lines = []
        self.results = []
        self.errors = []
        
        doc_type_names = {'demand': 'Отгрузки', 'supply': 'Приёмки', 'move': 'Перемещения'}
        self.doc_type_name = doc_type_names.get(doc_type, 'Документы')
        
        timestamp = self.started_at.strftime("%Y%m%d_%H%M%S")
        self.log_filename = f"log_{account_id[:8]}_{doc_type}_{timestamp}.txt"
        self.log_path = LOGS_DIR / self.log_filename
        
        self._write_header()
    
    def _write_header(self):
        header = [
            "=" * 70,
            f"ОТЧЁТ ПО РАЗНЕСЕНИЮ НАКЛАДНЫХ РАСХОДОВ",
            "=" * 70,
            f"Дата/время начала: {self.started_at.strftime('%d.%m.%Y %H:%M:%S')}",
            f"Аккаунт: {self.account_name}",
            f"Тип документов: {self.doc_type_name}",
            f"Год: {self.year}",
            f"Статья расходов: {self.category}",
            f"Валюта: {self.currency} ({self.currency_symbol})",
            "=" * 70,
            "",
            "ЖУРНАЛ ОБРАБОТКИ:",
            "-" * 70,
        ]
        self.lines.extend(header)
        self._flush()
    
    def log(self, message: str):
        timestamp = now_msk().strftime("%H:%M:%S")
        line = f"[{timestamp}] {message}"
        self.lines.append(line)
        logger.info(message)
    
    def log_success(self, doc_number: str, expense: float, total: float):
        self.results.append({
            "docNumber": doc_number,
            "added": expense,
            "total": total
        })
        self.log(f"✅ {doc_number} — добавлено {expense:,.2f} {self.currency} (итого: {total:,.2f} {self.currency})")
    
    def log_error(self, doc_number: str, expense: float, error: str):
        self.errors.append({
            "docNumber": doc_number,
            "expense": expense,
            "error": error
        })
        self.log(f"❌ {doc_number} — ОШИБКА: {error}")
    
    def log_search(self, doc_number: str, found: bool, details: str = ""):
        if found:
            self.log(f"🔍 {doc_number} — найден {details}")
        else:
            self.log(f"🔍 {doc_number} — НЕ НАЙДЕН {details}")
    
    def finalize(self) -> str:
        ended_at = now_msk()
        duration = (ended_at - self.started_at).total_seconds()
        total_sum = sum(r.get("added", 0) for r in self.results)
        
        footer = [
            "",
            "-" * 70,
            "ИТОГИ:",
            "-" * 70,
            f"Время завершения: {ended_at.strftime('%d.%m.%Y %H:%M:%S')}",
            f"Длительность: {duration:.1f} сек",
            "",
            f"✅ Успешно разнесено: {len(self.results)} записей",
            f"💰 Общая сумма: {total_sum:,.2f} {self.currency}",
            f"❌ Ошибок: {len(self.errors)} записей",
            "",
        ]
        
        if self.results:
            footer.append("УСПЕШНЫЕ ЗАПИСИ:")
            footer.append("-" * 40)
            for r in self.results:
                footer.append(f"  {r['docNumber']}: +{r['added']:,.2f} {self.currency}")
        
        if self.errors:
            footer.append("")
            footer.append("ОШИБКИ:")
            footer.append("-" * 40)
            for e in self.errors:
                footer.append(f"  {e['docNumber']}: {e['error']}")
        
        footer.extend(["", "=" * 70, "КОНЕЦ ОТЧЁТА", "=" * 70])
        
        self.lines.extend(footer)
        self._flush()
        return "\n".join(self.lines)
    
    def _flush(self):
        ensure_data_dir()
        with open(self.log_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(self.lines))
    
    def get_telegram_report(self) -> str:
        ended_at = now_msk()
        duration = (ended_at - self.started_at).total_seconds()
        total_sum = sum(r.get("added", 0) for r in self.results)
        
        report = [
            f"📊 <b>Отчёт по накладным расходам</b>",
            f"",
            f"📦 Аккаунт: {self.account_name}",
            f"📄 Тип: {self.doc_type_name}",
            f"📅 Год: {self.year}",
            f"📝 Статья: {self.category}",
            f"💱 Валюта: {self.currency} ({self.currency_symbol})",
            f"⏱ Время: {duration:.1f} сек",
            f"",
            f"━━━━━━━━━━━━━━━━━━━━━━",
        ]
        
        if self.results:
            report.append(f"")
            report.append(f"✅ <b>Успешно: {len(self.results)}</b>")
            report.append(f"💰 Сумма: {total_sum:,.2f} {self.currency}")
            report.append(f"")
            for r in self.results[:15]:
                report.append(f"  • {r['docNumber']} — {r['added']:,.2f} {self.currency}")
            if len(self.results) > 15:
                report.append(f"  ... и ещё {len(self.results) - 15}")
        
        if self.errors:
            report.append(f"")
            report.append(f"❌ <b>Ошибки: {len(self.errors)}</b>")
            report.append(f"")
            for e in self.errors[:20]:
                error_short = e['error'][:50] + "..." if len(e['error']) > 50 else e['error']
                report.append(f"  • {e['docNumber']}")
                report.append(f"    └ {error_short}")
            if len(self.errors) > 20:
                report.append(f"  ... и ещё {len(self.errors) - 20}")
        
        report.append(f"")
        report.append(f"⏰ {ended_at.strftime('%d.%m.%Y %H:%M:%S')}")
        
        return "\n".join(report)


# ============== Telegram Bot ==============

async def send_telegram_message(chat_id: int, text: str, parse_mode: str = "HTML"):
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        return False
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.post(url, json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": parse_mode
            })
            return resp.status_code == 200
        except Exception as e:
            logger.error(f"❌ Telegram error: {e}")
            return False


async def send_telegram_document(chat_id: int, file_content: str, filename: str, caption: str = ""):
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        return False
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            files = {'document': (filename, file_content.encode('utf-8'), 'text/plain')}
            data = {'chat_id': chat_id, 'caption': caption}
            resp = await client.post(url, data=data, files=files)
            return resp.status_code == 200
        except Exception as e:
            logger.error(f"❌ Telegram document error: {e}")
            return False


async def notify_user_by_username(username: str, text: str):
    if not username:
        return False
    chat_id = get_telegram_chat_id(username)
    if not chat_id:
        logger.warning(f"⚠️ Telegram: @{username} не зарегистрирован")
        return False
    return await send_telegram_message(chat_id, text)


async def send_log_file_to_user(username: str, log_content: str, filename: str):
    if not username:
        return False
    chat_id = get_telegram_chat_id(username)
    if not chat_id:
        return False
    return await send_telegram_document(chat_id, log_content, filename, "📄 Полный лог обработки")


# ============== Системные Telegram-уведомления ==============

async def notify_admin(text: str):
    """
    Отправить служебное уведомление админу.
    Username берётся из ADMIN_TELEGRAM_USERNAME (переменная окружения), например "@kulps_dev".
    Другим пользователям это не приходит.
    """
    username = (ADMIN_TELEGRAM_USERNAME or "").lstrip()
    if not username:
        logger.warning("⚠️ ADMIN_TELEGRAM_USERNAME не задан")
        return False
    return await notify_user_by_username(username, text)


# ============== Context Mapping ==============

def save_context_mapping(context_key: str, account_id: str):
    if not context_key or not account_id:
        return
    acc = get_account(account_id)
    if not acc or acc.get("status") != "active":
        return
    data = load_context_map()
    data["map"][context_key] = {
        "account_id": account_id,
        "account_name": acc.get("account_name", ""),
        "created_at": now_msk().isoformat()
    }
    if len(data["map"]) > 10000:
        sorted_keys = sorted(data["map"].keys(), key=lambda k: data["map"][k].get("created_at", ""))
        for k in sorted_keys[:len(sorted_keys) - 10000]:
            del data["map"][k]
    save_context_map(data)


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
        del data["map"][context_key]
        save_context_map(data)
        return None
    return account_id


# ============== JWT ==============

def generate_jwt_token() -> str:
    now = int(time.time())
    payload = {
        "sub": "expenses.kulps",
        "iat": now,
        "exp": now + 300,
        "jti": str(uuid.uuid4())
    }
    return jwt.encode(payload, APP_SECRET, algorithm="HS256")


async def get_context_from_moysklad(context_key: str) -> Optional[dict]:
    if not context_key or not APP_SECRET:
        return None
    url = f"{VENDOR_API_BASE}/context/{context_key}"
    jwt_token = generate_jwt_token()
    headers = {
        "Accept-Encoding": "gzip",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {jwt_token}"
    }
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.post(url, headers=headers, json={})
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            logger.error(f"❌ Context error: {e}")
    return None


# ============== API МойСклад ==============

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
            except:
                result = {"_text": resp.text[:1000]}
            result["_status"] = resp.status_code
            return result
        except Exception as e:
            return {"_error": str(e), "_status": 0}


# ============== Resolve Account ==============

async def resolve_account(request: Request) -> Optional[dict]:
    context_key = request.query_params.get("contextKey", "")
    account_id_hint = request.query_params.get("accountId", "")
    app_id_from_url = request.query_params.get("appId", "")
    
    if account_id_hint:
        acc = get_account(account_id_hint)
        if acc and acc.get("status") == "active" and acc.get("access_token"):
            if context_key:
                save_context_mapping(context_key, account_id_hint)
            return acc
    
    if context_key:
        cached_account_id = get_account_id_from_context(context_key)
        if cached_account_id:
            acc = get_account(cached_account_id)
            if acc and acc.get("status") == "active":
                return acc
    
    if context_key:
        context_data = await get_context_from_moysklad(context_key)
        if context_data:
            account_id = (context_data.get("accountId") or 
                         context_data.get("account_id") or
                         context_data.get("account", {}).get("id"))
            if account_id:
                acc = get_account(account_id)
                if acc and acc.get("status") == "active" and acc.get("access_token"):
                    save_context_mapping(context_key, account_id)
                    return acc
    
    if app_id_from_url:
        acc = get_account_by_app_id(app_id_from_url)
        if acc:
            if context_key:
                save_context_mapping(context_key, acc["account_id"])
            return acc
    
    all_accounts = get_all_active_accounts()
    if len(all_accounts) == 1:
        acc = all_accounts[0]
        if context_key:
            save_context_mapping(context_key, acc["account_id"])
        return acc
    
    return None


# ============== Справочник статей ==============

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
    return None


async def get_expense_categories(token: str, dict_id: str) -> List[dict]:
    result = await ms_api("GET", f"/entity/customentity/{dict_id}", token)
    categories = []
    if result.get("_status") == 200 and "rows" in result:
        for elem in result["rows"]:
            categories.append({"id": elem.get("id"), "name": elem.get("name")})
    return categories


async def add_expense_category(token: str, dict_id: str, name: str) -> Optional[dict]:
    result = await ms_api("POST", f"/entity/customentity/{dict_id}", token, {"name": name})
    if result.get("_status") in [200, 201] and result.get("id"):
        return {"id": result["id"], "name": result.get("name", name)}
    if result.get("_status") == 412:
        return {"id": "exists", "name": name}
    return None


# ============== Поиск документов ==============

async def search_document_exact(token: str, doc_type: str, name: str, year: int, log: ProcessingLog) -> dict:
    """Точный поиск документа по номеру и году"""
    date_from = f"{year}-01-01 00:00:00"
    date_to = f"{year}-12-31 23:59:59"
    
    doc_endpoints = {
        'demand': '/entity/demand',
        'supply': '/entity/supply',
        'move': '/entity/move'
    }
    doc_names = {
        'demand': 'Отгрузка',
        'supply': 'Приёмка',
        'move': 'Перемещение'
    }
    
    endpoint_base = doc_endpoints.get(doc_type, '/entity/demand')
    doc_name_ru = doc_names.get(doc_type, 'Документ')
    
    log.log(f"🔍 Поиск {doc_name_ru}: '{name}' за {year} год...")
    
    # Точный поиск
    endpoint = f"{endpoint_base}?filter=name={name};moment>{date_from};moment<{date_to}"
    r = await ms_api("GET", endpoint, token)
    
    if r.get("_status") == 200 and r.get("rows"):
        for row in r["rows"]:
            if row.get("name") == name:
                log.log_search(name, True, f"(ID: {row.get('id')[:8]}...)")
                return {"found": True, "document": row}
        
        similar = [row.get("name") for row in r["rows"][:5]]
        log.log_search(name, False, f"| Похожие: {', '.join(similar)}")
        return {"found": False, "error": f"Точное совпадение не найдено. Похожие: {', '.join(similar)}"}
    
    # Поиск с ~
    endpoint2 = f"{endpoint_base}?filter=name~{name};moment>{date_from};moment<{date_to}"
    r2 = await ms_api("GET", endpoint2, token)
    
    if r2.get("_status") == 200 and r2.get("rows"):
        for row in r2["rows"]:
            if row.get("name") == name:
                log.log_search(name, True, f"(ID: {row.get('id')[:8]}...)")
                return {"found": True, "document": row}
        
        similar = [row.get("name") for row in r2["rows"][:5]]
        log.log_search(name, False, f"| Похожие: {', '.join(similar)}")
        return {"found": False, "error": f"Точное совпадение не найдено. Похожие: {', '.join(similar)}"}
    
    log.log_search(name, False, f"| Ничего не найдено за {year} год")
    return {"found": False, "error": f"{doc_name_ru} не найден за {year} год"}


async def update_document_overhead(token: str, doc_type: str, doc_id: str, add_sum: float, 
                                    category: str, log: ProcessingLog, currency: str = "руб") -> dict:
    """Обновить накладные расходы документа"""
    doc_endpoints = {
        'demand': '/entity/demand',
        'supply': '/entity/supply',
        'move': '/entity/move'
    }
    endpoint_base = doc_endpoints.get(doc_type, '/entity/demand')
    
    document = await ms_api("GET", f"{endpoint_base}/{doc_id}", token)
    if document.get("_status") != 200:
        return {"success": False, "error": "Документ не найден"}
    
    doc_name = document.get("name", "")
    current_overhead = 0
    overhead_data = document.get("overhead")
    if overhead_data and overhead_data.get("sum"):
        current_overhead = overhead_data.get("sum", 0)
    
    new_overhead = current_overhead + int(add_sum * 100)
    timestamp = now_msk().strftime("%d.%m.%Y %H:%M")
    
    # Используем переданную валюту в комментарии
    new_comment = f"[{timestamp}] +{add_sum:.2f} {currency} - {category}"
    
    current_desc = document.get("description") or ""
    new_desc = f"{current_desc}\n{new_comment}".strip()
    
    update_data = {
        "description": new_desc,
        "overhead": {"sum": new_overhead, "distribution": "price"}
    }
    
    log.log(f"📝 Обновление {doc_name}: +{add_sum:.2f} {currency} (было: {current_overhead/100:.2f} {currency})")
    
    result = await ms_api("PUT", f"{endpoint_base}/{doc_id}", token, update_data)
    
    if result.get("_status") == 200:
        return {"success": True, "doc_name": doc_name, "added": add_sum, "total": new_overhead / 100}
    
    return {"success": False, "error": str(result)}


# ============== Vendor API ==============

@app.put("/api/moysklad/vendor/1.0/apps/{app_id}/{account_id}")
async def activate_app(app_id: str, account_id: str, request: Request):
    body = await request.json()
    account_name = body.get("accountName", "")
    logger.info(f"🟢 АКТИВАЦИЯ: {account_name} ({account_id})")
    
    token = None
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
        logger.info(f"📚 Справочник: {dict_id}")

    # Админ-уведомление о новой активации
    try:
        from asyncio import create_task
        active_accounts = get_all_active_accounts()
        msg_lines = [
            "🟢 <b>Новая активация приложения</b>",
            "",
            f"📦 Аккаунт: {account_name or '—'}",
            f"🆔 ID: <code>{account_id}</code>",
            f"🧩 App ID: <code>{app_id}</code>",
            "",
            f"📊 Сейчас активных аккаунтов: <b>{len(active_accounts)}</b>",
            f"⏰ {now_msk().strftime('%d.%m.%Y %H:%M:%S')}",
        ]
        create_task(notify_admin("\n".join(msg_lines)))
    except Exception as e:
        logger.error(f"Не удалось отправить уведомление админу об активации: {e}")
    
    return JSONResponse({"status": "Activated"})


@app.delete("/api/moysklad/vendor/1.0/apps/{app_id}/{account_id}")
async def deactivate_app(app_id: str, account_id: str, request: Request):
    body = await request.json()
    logger.info(f"🔴 ДЕАКТИВАЦИЯ: {body.get('accountName', '')} ({account_id})")
    
    acc = get_account(account_id)
    if acc:
        acc["status"] = "inactive"
        acc["access_token"] = None
        acc["deactivated_at"] = now_msk().isoformat()
        save_account(account_id, acc)
    
    context_map = load_context_map()
    keys_to_remove = [k for k, v in context_map.get("map", {}).items() if v.get("account_id") == account_id]
    for k in keys_to_remove:
        del context_map["map"][k]
    save_context_map(context_map)

    # Админ-уведомление о деактивации
    try:
        from asyncio import create_task
        account_name = body.get("accountName", "") or (acc.get("account_name") if acc else "")
        reason = body.get("reason") or body.get("cause") or ""
        reason_text = f"\n📝 Причина: {reason}" if reason else ""
        active_accounts = get_all_active_accounts()
        msg_lines = [
            "🔴 <b>Деактивация приложения</b>",
            "",
            f"📦 Аккаунт: {account_name or '—'}",
            f"🆔 ID: <code>{account_id}</code>",
            f"🧩 App ID: <code>{app_id}</code>",
            reason_text,
            "",
            f"📊 После деактивации активных аккаунтов: <b>{len(active_accounts)}</b>",
            f"⏰ {now_msk().strftime('%d.%m.%Y %H:%M:%S')}",
        ]
        # Уберём пустые строки от reason_text
        msg = "\n".join([line for line in msg_lines if line != ""])
        create_task(notify_admin(msg))
    except Exception as e:
        logger.error(f"Не удалось отправить уведомление админу о деактивации: {e}")
    
    return JSONResponse(status_code=200, content={})


@app.get("/api/moysklad/vendor/1.0/apps/{app_id}/{account_id}/status")
async def get_status(app_id: str, account_id: str):
    acc = get_account(account_id)
    status = "Activated" if acc and acc.get("status") == "active" else "SettingsRequired"
    return JSONResponse({"status": status})


# ============== Telegram Webhook ==============

@app.post("/api/telegram/webhook")
async def telegram_webhook(request: Request):
    try:
        data = await request.json()
        message = data.get("message", {})
        if not message:
            return JSONResponse({"ok": True})
        
        chat_id = message.get("chat", {}).get("id")
        username = message.get("from", {}).get("username", "")
        text = message.get("text", "")
        
        if text == "/start":
            if username:
                save_telegram_user(username, chat_id)
                await send_telegram_message(
                    chat_id,
                    f"✅ <b>Вы зарегистрированы!</b>\n\n"
                    f"Ваш username: @{username}\n\n"
                    f"Теперь вы будете получать:\n"
                    f"• Уведомления о начале обработки\n"
                    f"• Подробный отчёт по итогам\n"
                    f"• Файл с полным логом\n\n"
                    f"Укажите <code>@{username}</code> в приложении МойСклад."
                )
                logger.info(f"📱 Telegram: зарегистрирован @{username}")
            else:
                await send_telegram_message(
                    chat_id,
                    "⚠️ У вас не установлен username в Telegram!\n"
                    "Установите username в настройках и попробуйте снова."
                )
        
        return JSONResponse({"ok": True})
    except Exception as e:
        logger.error(f"❌ Telegram webhook error: {e}")
        return JSONResponse({"ok": True})


# ============== API для фронтенда ==============

@app.get("/api/expense-categories")
async def api_get_categories(request: Request):
    acc = await resolve_account(request)
    
    if not acc:
        all_accounts = get_all_active_accounts()
        return JSONResponse({
            "categories": [],
            "error": f"Не удалось определить аккаунт ({len(all_accounts)} активных)",
            "needsReinstall": len(all_accounts) == 0,
        }, status_code=400)
    
    if not acc.get("access_token"):
        return JSONResponse({"categories": [], "error": "Нет токена"}, status_code=401)
    
    token = acc["access_token"]
    account_id = acc["account_id"]
    
    dict_id = await ensure_dictionary(token, account_id)
    if not dict_id:
        return JSONResponse({"categories": [], "error": "Не удалось создать справочник"})
    
    categories = await get_expense_categories(token, dict_id)
    saved_telegram = get_user_telegram(account_id)
    
    return JSONResponse({
        "categories": categories,
        "accountId": account_id,
        "accountName": acc.get("account_name"),
        "savedTelegram": saved_telegram
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
        return JSONResponse({"success": False, "error": "Нет справочника"})
    
    cat = await add_expense_category(token, dict_id, name)
    if cat:
        return JSONResponse({"success": True, "category": cat})
    return JSONResponse({"success": False, "error": "Ошибка создания"})


@app.post("/api/save-telegram")
async def api_save_telegram(request: Request):
    body = await request.json()
    telegram_username = body.get("telegramUsername", "").strip()
    
    acc = await resolve_account(request)
    if not acc:
        return JSONResponse({"success": False, "error": "Аккаунт не определён"}, status_code=400)
    
    save_user_telegram(acc["account_id"], telegram_username)
    return JSONResponse({"success": True})


@app.get("/api/check-telegram")
async def check_telegram(request: Request):
    username = request.query_params.get("username", "").lstrip("@")
    if not username:
        return JSONResponse({"registered": False, "error": "Username не указан"})
    chat_id = get_telegram_chat_id(username)
    return JSONResponse({"registered": chat_id is not None, "username": username})


@app.post("/api/process-expenses")
async def process_expenses(request: Request):
    body = await request.json()
    expenses = body.get("expenses", [])
    category = body.get("category", "Накладные расходы")
    year = body.get("year", now_msk().year)
    telegram_username = body.get("telegramUsername", "")
    doc_type = body.get("docType", "demand")
    currency = body.get("currency", "руб")  # Получаем валюту из запроса
    
    acc = await resolve_account(request)
    if not acc or not acc.get("access_token"):
        return JSONResponse({"success": False, "error": "Аккаунт не определён"}, status_code=400)
    
    token = acc["access_token"]
    account_id = acc["account_id"]
    account_name = acc.get("account_name", "")
    
    doc_type_names = {'demand': 'Отгрузки', 'supply': 'Приёмки', 'move': 'Перемещения'}
    doc_type_name = doc_type_names.get(doc_type, 'Документы')
    
    if telegram_username:
        save_user_telegram(account_id, telegram_username)
    
    logger.info(f"📊 Обработка {len(expenses)} ({doc_type_name}) для {account_name}, год: {year}, валюта: {currency}")
    
    # Справочник для создания статей
    dict_id = await ensure_dictionary(token, account_id)
    
    # Собираем статьи из данных
    categories_to_create = set()
    for item in expenses:
        item_category = item.get("category")
        if item_category:
            categories_to_create.add(item_category.strip())
    
    # Существующие статьи
    existing_categories = await get_expense_categories(token, dict_id) if dict_id else []
    existing_names = {c["name"].lower() for c in existing_categories}
    
    # Лог с валютой
    proc_log = ProcessingLog(account_id, account_name, year, category, doc_type, currency)
    proc_log.log(f"Начало обработки {len(expenses)} записей ({doc_type_name})")
    proc_log.log(f"Валюта: {currency} ({get_currency_symbol(currency)})")
    
    # Создаём новые статьи
    new_categories_created = []
    for cat_name in categories_to_create:
        if cat_name.lower() not in existing_names:
            proc_log.log(f"📝 Создание статьи: '{cat_name}'")
            result = await add_expense_category(token, dict_id, cat_name)
            if result:
                new_categories_created.append(cat_name)
                existing_names.add(cat_name.lower())
                proc_log.log(f"✅ Статья '{cat_name}' создана")
            else:
                proc_log.log(f"⚠️ Не удалось создать статью '{cat_name}'")
    
    if new_categories_created:
        proc_log.log(f"📚 Создано новых статей: {len(new_categories_created)}")
    
    # Уведомление о начале
    if telegram_username:
        currency_symbol = get_currency_symbol(currency)
        start_msg = f"🚀 <b>Начато разнесение накладных расходов</b>\n\n"
        start_msg += f"📦 Аккаунт: {account_name}\n"
        start_msg += f"📄 Тип: {doc_type_name}\n"
        start_msg += f"📅 Год: {year}\n"
        start_msg += f"📝 Статья: {category}\n"
        start_msg += f"💱 Валюта: {currency} ({currency_symbol})\n"
        start_msg += f"📊 Записей: {len(expenses)}\n"
        if new_categories_created:
            start_msg += f"📚 Новых статей: {len(new_categories_created)}\n"
        start_msg += f"\n⏳ Пожалуйста, подождите..."
        await notify_user_by_username(telegram_username, start_msg)
    
    # Обработка
    for idx, item in enumerate(expenses, 1):
        num = item.get("demandNumber", "").strip()
        val = float(item.get("expense", 0))
        item_category = item.get("category") or category
        
        if not num or val <= 0:
            continue
        
        proc_log.log(f"")
        proc_log.log(f"[{idx}/{len(expenses)}] {num} — {val:,.2f} {currency} ({item_category})")
        
        search_result = await search_document_exact(token, doc_type, num, year, proc_log)
        
        if not search_result["found"]:
            proc_log.log_error(num, val, search_result.get("error", "Не найден"))
            continue
        
        document = search_result["document"]
        r = await update_document_overhead(token, doc_type, document["id"], val, item_category, proc_log, currency)
        
        if r["success"]:
            proc_log.log_success(num, val, r.get("total", 0))
        else:
            proc_log.log_error(num, val, r.get("error", "Ошибка обновления"))
    
    # Финализация
    full_log = proc_log.finalize()
    
    # Telegram отчёт
    if telegram_username:
        telegram_report = proc_log.get_telegram_report()
        if new_categories_created:
            telegram_report += f"\n\n📚 <b>Созданы статьи:</b>\n"
            for nc in new_categories_created[:10]:
                telegram_report += f"  • {nc}\n"
            if len(new_categories_created) > 10:
                telegram_report += f"  ... и ещё {len(new_categories_created) - 10}"
        
        await notify_user_by_username(telegram_username, telegram_report)
        await send_log_file_to_user(telegram_username, full_log, proc_log.log_filename)
    
    return JSONResponse({
        "success": True,
        "processed": len(proc_log.results),
        "errors": len(proc_log.errors),
        "results": proc_log.results,
        "errorDetails": proc_log.errors,
        "accountName": account_name,
        "year": year,
        "docType": doc_type,
        "currency": currency,
        "logFile": proc_log.log_filename,
        "newCategories": new_categories_created
    })


@app.get("/api/debug")
async def debug(request: Request):
    all_accounts = get_all_active_accounts()
    telegram_users = load_telegram_users()
    return JSONResponse({
        "all_active_accounts": [{"id": a.get("account_id"), "name": a.get("account_name")} for a in all_accounts],
        "total_active": len(all_accounts),
        "telegram_users_count": len(telegram_users.get("users", {})),
        "telegram_bot_configured": bool(TELEGRAM_BOT_TOKEN),
        "server_time": now_msk().strftime("%Y-%m-%d %H:%M:%S"),
        "supported_currencies": list(CURRENCY_SYMBOLS.keys())
    })


@app.get("/api/accounts")
async def list_accounts():
    accounts_data = load_accounts()
    result = []
    for acc_id, acc in accounts_data.get("accounts", {}).items():
        saved_tg = get_user_telegram(acc_id)
        result.append({
            "id": acc_id,
            "name": acc.get("account_name"),
            "status": acc.get("status"),
            "has_token": bool(acc.get("access_token")),
            "telegram": saved_tg
        })
    return JSONResponse({"accounts": result})


@app.get("/api/currencies")
async def get_currencies():
    """Получить список поддерживаемых валют"""
    currencies = [
        {"code": "руб", "symbol": "₽", "name": "Российский рубль"},
        {"code": "USD", "symbol": "$", "name": "Доллар США"},
        {"code": "EUR", "symbol": "€", "name": "Евро"},
        {"code": "CNY", "symbol": "¥", "name": "Китайский юань"},
        {"code": "KZT", "symbol": "₸", "name": "Казахстанский тенге"},
        {"code": "BYN", "symbol": "Br", "name": "Белорусский рубль"},
        {"code": "UAH", "symbol": "₴", "name": "Украинская гривна"},
        {"code": "UZS", "symbol": "сум", "name": "Узбекский сум"},
        {"code": "GEL", "symbol": "₾", "name": "Грузинский лари"},
        {"code": "AMD", "symbol": "֏", "name": "Армянский драм"},
        {"code": "TRY", "symbol": "₺", "name": "Турецкая лира"},
        {"code": "AED", "symbol": "د.إ", "name": "Дирхам ОАЭ"},
    ]
    return JSONResponse({"currencies": currencies})


# ============== Админ-эндпоинт: уведомить о всех активных аккаунтах ==============

@app.post("/api/admin/notify-active-accounts")
async def admin_notify_active_accounts(request: Request):
    """
    Ручная отправка отчёта обо всех активных аккаунтах админу в Telegram.
    Можно защитить простым секретом через переменную окружения ADMIN_SECRET.
    Вызов: POST /expensesms/api/admin/notify-active-accounts?secret=XXX
    """
    secret = request.query_params.get("secret", "")
    expected = os.getenv("ADMIN_SECRET", "")
    if expected and secret != expected:
        return JSONResponse({"success": False, "error": "Forbidden"}, status_code=403)

    active_accounts = get_all_active_accounts()
    lines = [
        "📊 <b>Статус приложения</b>",
        f"Активных аккаунтов: <b>{len(active_accounts)}</b>",
        ""
    ]
    for acc in active_accounts[:30]:
        lines.append(f"• {acc.get('account_name', '—')} (<code>{acc.get('account_id')}</code>)")
    if len(active_accounts) > 30:
        lines.append(f"... и ещё {len(active_accounts) - 30}")

    await notify_admin("\n".join(lines))
    return JSONResponse({"success": True, "total": len(active_accounts)})


# ============== Страницы ==============

@app.get("/iframe", response_class=HTMLResponse)
async def iframe_page(request: Request):
    return templates.TemplateResponse("iframe.html", {"request": request})


@app.get("/widget-demand", response_class=HTMLResponse)
async def widget_demand(request: Request):
    return templates.TemplateResponse("widget_demand.html", {"request": request})


@app.get("/widget-supply", response_class=HTMLResponse)
async def widget_supply(request: Request):
    return templates.TemplateResponse("widget_supply.html", {"request": request})


@app.get("/widget-move", response_class=HTMLResponse)
async def widget_move(request: Request):
    return templates.TemplateResponse("widget_move.html", {"request": request})


@app.get("/")
async def root():
    all_accounts = get_all_active_accounts()
    return {
        "app": "Накладные расходы",
        "version": "7.2",
        "active_accounts": len(all_accounts),
        "features": [
            "demand", "supply", "move", 
            "telegram", "auto_categories", 
            "exact_match", "year_filter",
            "multi_currency", "admin_notify"
        ],
        "supported_currencies": list(CURRENCY_SYMBOLS.keys())
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.middleware("http")
async def add_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Frame-Options"] = "ALLOWALL"
    response.headers["Content-Security-Policy"] = "frame-ancestors *"
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response