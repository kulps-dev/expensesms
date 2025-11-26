from flask import Flask, request, jsonify, render_template
import hmac
import hashlib
import json
import logging

app = Flask(__name__)

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Секретный ключ приложения (получите его в личном кабинете МойСклад)
APP_SECRET = "your_app_secret_here"  # ЗАМЕНИТЕ НА РЕАЛЬНЫЙ!

def verify_signature(data, signature):
    """Проверка подписи запроса от МойСклад"""
    if not APP_SECRET or APP_SECRET == "your_app_secret_here":
        logger.warning("APP_SECRET не установлен!")
        return True  # В тестовом режиме пропускаем
    
    expected_signature = hmac.new(
        APP_SECRET.encode(),
        data.encode(),
        hashlib.sha256
    ).hexdigest()
    
    return hmac.compare_digest(signature, expected_signature)

@app.route('/expensesms')
def index():
    """Главная страница приложения"""
    return render_template('index.html')

@app.route('/expensesms/api/moysklad/v2/apps/status', methods=['POST'])
def app_status():
    """
    Эндпоинт для проверки статуса приложения.
    МойСклад вызывает его при установке приложения.
    """
    logger.info("Получен запрос статуса приложения")
    
    data = request.get_data(as_text=True)
    signature = request.headers.get('X-Lognex-Signature', '')
    
    if not verify_signature(data, signature):
        logger.error("Неверная подпись запроса")
        return jsonify({"error": "Invalid signature"}), 403
    
    return jsonify({
        "status": "ok",
        "version": "1.0.0"
    })

@app.route('/expensesms/api/moysklad/v2/apps/installed', methods=['POST'])
def app_installed():
    """
    Вызывается когда пользователь устанавливает приложение.
    Здесь сохраняется access_token для работы с API МойСклад.
    """
    logger.info("Приложение установлено")
    
    data = request.get_json()
    logger.info(f"Данные установки: {json.dumps(data, indent=2)}")
    
    # Сохраните эти данные в БД:
    # - data['accountId'] - ID аккаунта
    # - data['access_token'] - токен доступа
    # - data['appUid'] - UID приложения
    
    access_token = data.get('access_token')
    account_id = data.get('accountId')
    
    # TODO: Сохранить в базу данных
    logger.info(f"Account ID: {account_id}")
    logger.info(f"Access Token: {access_token[:10]}...")
    
    return jsonify({"status": "installed"}), 200

@app.route('/expensesms/api/moysklad/v2/apps/uninstalled', methods=['POST'])
def app_uninstalled():
    """
    Вызывается при удалении приложения.
    Здесь нужно удалить сохраненные данные пользователя.
    """
    logger.info("Приложение удалено")
    
    data = request.get_json()
    account_id = data.get('accountId')
    
    # TODO: Удалить данные из БД
    logger.info(f"Удаление данных для Account ID: {account_id}")
    
    return jsonify({"status": "uninstalled"}), 200

@app.route('/expensesms/api/moysklad/v2/apps/iframe', methods=['POST'])
def iframe_handler():
    """
    Обработчик для встраивания приложения в iframe МойСклад.
    Возвращает HTML, который будет показан в интерфейсе МойСклад.
    """
    logger.info("Запрос iframe")
    
    data = request.get_json()
    context_key = data.get('contextKey')
    account_id = data.get('accountId')
    
    logger.info(f"Context: {context_key}, Account: {account_id}")
    
    # Возвращаем URL для iframe
    return jsonify({
        "type": "iframe",
        "url": f"https://kulps.ru/expensesms?account={account_id}"
    }), 200

@app.route('/expensesms/api/orders', methods=['GET'])
def get_orders():
    """
    Пример API для получения заказов из МойСклад.
    Используйте сохраненный access_token.
    """
    # TODO: Получить access_token из БД по account_id
    # TODO: Сделать запрос к API МойСклад
    
    return jsonify({
        "orders": [],
        "message": "Реализуйте получение заказов"
    })

@app.route('/expensesms/health')
def health():
    """Проверка работоспособности"""
    return jsonify({"status": "healthy"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)