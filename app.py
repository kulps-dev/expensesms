from flask import Flask, request, jsonify, render_template_string
import hmac
import hashlib
import json
import logging
import os

app = Flask(__name__)

# Логирование
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Секрет приложения из окружения
APP_SECRET = os.getenv("APP_SECRET", "").strip()

def get_signature_from_headers():
    # Поддержка нескольких возможных заголовков
    return (
        request.headers.get('X-Lognex-Signature')
        or request.headers.get('X-Lognex-Hmac-SHA256')
        or request.headers.get('X-Lognex-Content-HMAC')
        or ""
    )

def verify_signature(raw_body: bytes, signature: str) -> bool:
    """Проверка подписи запроса от МойСклад"""
    if not APP_SECRET:
        logger.warning("APP_SECRET не установлен! Пропускаем проверку в тестовом режиме.")
        return True

    try:
        expected = hmac.new(
            APP_SECRET.encode('utf-8'),
            raw_body,
            hashlib.sha256
        ).hexdigest()
        return hmac.compare_digest(signature, expected)
    except Exception as e:
        logger.exception("Ошибка при проверке подписи: %s", e)
        return False

@app.route('/expensesms')
def index():
    """Главная страница приложения (простая заглушка без templates)"""
    html = """
    <!doctype html>
    <html lang="ru">
    <head>
      <meta charset="utf-8">
      <title>ExpensesMS</title>
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <style>body{font-family:system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;padding:24px}</style>
    </head>
    <body>
      <h1>ExpensesMS</h1>
      <p>Приложение работает. Проверьте /expensesms/health для статуса.</p>
    </body>
    </html>
    """
    return render_template_string(html)

@app.route('/expensesms/api/moysklad/v2/apps/status', methods=['POST'])
def app_status():
    """
    Эндпоинт проверки статуса приложения.
    МойСклад вызывает его при установке.
    """
    raw = request.get_data(cache=True)
    signature = get_signature_from_headers()

    if not verify_signature(raw, signature):
        logger.error("Неверная подпись запроса (status)")
        return jsonify({"error": "Invalid signature"}), 403

    logger.info("Получен запрос статуса приложения")
    return jsonify({"status": "ok", "version": "1.0.0"}), 200

@app.route('/expensesms/api/moysklad/v2/apps/installed', methods=['POST'])
def app_installed():
    """
    Сохранение токена и данных аккаунта при установке.
    """
    raw = request.get_data(cache=True)
    signature = get_signature_from_headers()

    if not verify_signature(raw, signature):
        logger.error("Неверная подпись запроса (installed)")
        return jsonify({"error": "Invalid signature"}), 403

    data = request.get_json(silent=True) or {}
    logger.info("Приложение установлено. Данные: %s", json.dumps(data, ensure_ascii=False))

    # Возможные ключи токена
    access_token = data.get('access_token') or data.get('accessToken')
    account_id = data.get('accountId')
    app_uid = data.get('appUid')

    # TODO: Сохранить в БД account_id, access_token, app_uid
    if access_token:
        logger.info("Account ID: %s; Token: %s...", account_id, access_token[:8])
    else:
        logger.warning("Токен доступа отсутствует в payload")

    return jsonify({"status": "installed"}), 200

@app.route('/expensesms/api/moysklad/v2/apps/uninstalled', methods=['POST'])
def app_uninstalled():
    """
    Очистка данных при удалении приложения.
    """
    raw = request.get_data(cache=True)
    signature = get_signature_from_headers()

    if not verify_signature(raw, signature):
        logger.error("Неверная подпись запроса (uninstalled)")
        return jsonify({"error": "Invalid signature"}), 403

    data = request.get_json(silent=True) or {}
    account_id = data.get('accountId')

    # TODO: Удалить все связанные с account_id данные из БД
    logger.info("Приложение удалено. Удаляем данные для Account ID: %s", account_id)

    return jsonify({"status": "uninstalled"}), 200

@app.route('/expensesms/api/moysklad/v2/apps/iframe', methods=['POST'])
def iframe_handler():
    """
    Возвращает URL для отображения iframe в МойСклад.
    """
    raw = request.get_data(cache=True)
    signature = get_signature_from_headers()

    if not verify_signature(raw, signature):
        logger.error("Неверная подпись запроса (iframe)")
        return jsonify({"error": "Invalid signature"}), 403

    data = request.get_json(silent=True) or {}
    context_key = data.get('contextKey')
    account_id = data.get('accountId')

    logger.info("Запрос iframe: context=%s, account=%s", context_key, account_id)

    return jsonify({
        "type": "iframe",
        "url": f"https://kulps.ru/expensesms?account={account_id or ''}"
    }), 200

@app.route('/expensesms/api/orders', methods=['GET'])
def get_orders():
    """
    Пример API для получения заказов (заглушка).
    """
    # TODO: Получить access_token из БД по account_id
    # TODO: Сделать запрос к API МойСклад
    return jsonify({
        "orders": [],
        "message": "Реализуйте получение заказов"
    }), 200

@app.route('/expensesms/health')
def health():
    """Проверка работоспособности"""
    return jsonify({"status": "healthy"}), 200

# Не запускать встроенный сервер во время контейнеризации (gunicorn стартует в CMD)
if __name__ == '__main__':
    # Локальный запуск
    app.run(host='0.0.0.0', port=5000, debug=False)