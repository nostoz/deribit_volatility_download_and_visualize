import logging 
import requests

class TelegramLogHandler(logging.Handler):
    def __init__(self, bot_token, chat_id):
        super().__init__()
        self.bot_token = bot_token
        self.chat_id = chat_id

    def emit(self, record):
        log_entry = self.format(record)
        self.send_telegram_message(log_entry)

    def send_telegram_message(self, message):
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": message
        }
        response = requests.post(url, json=payload)
        if response.status_code != 200:
            print(f"Failed to send Telegram message. Error code: {response.status_code}")