import logging
from datetime import datetime
import os
class Logger:
    """
    Singleton класс для логирования действий в системе
    """
    # ПРИМЕР ПАТТЕРНА Singleton:
    # Статическое поле _instance хранит единственный экземпляр класса
    _instance = None
    
    def __new__(cls):
        # Если экземпляр ещё не создан — создаём его
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
            cls._instance._initialize()
        # Всегда возвращаем один и тот же экземпляр
        return cls._instance
    
    def _initialize(self):
        """Инициализация логгера"""
        # Создаем папку для логов если её нет
        log_dir = 'logs'
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # Настройка логгера
        self.logger = logging.getLogger('MarketEasy')
        self.logger.setLevel(logging.INFO)
        
        # Хендлер для файла
        log_file = os.path.join(log_dir, f'marketeasy_{datetime.now().strftime("%Y%m%d")}.log')
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        
        # Формат логов
        formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
        file_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
    
    def info(self, message, user_id=None):
        """Логирование информационного сообщения"""
        user_str = f"[User:{user_id}] " if user_id else ""
        self.logger.info(f"{user_str}{message}")
        print(f"LOG: {user_str}{message}")  # Дублируем в консоль для отладки
    
    def error(self, message, user_id=None):
        """Логирование ошибки"""
        user_str = f"[User:{user_id}] " if user_id else ""
        self.logger.error(f"{user_str}{message}")
        print(f"ERROR: {user_str}{message}")
    
    def warning(self, message, user_id=None):
        """Логирование предупреждения"""
        user_str = f"[User:{user_id}] " if user_id else ""
        self.logger.warning(f"{user_str}{message}")
        print(f"WARN: {user_str}{message}")
    
    def log_action(self, action, details, user_id=None):
        """Логирование действия пользователя"""
        user_str = f"[User:{user_id}] " if user_id else ""
        self.logger.info(f"{user_str}ACTION: {action} | DETAILS: {details}")
        print(f"ACTION: {user_str}{action} | {details}")
logger = Logger()
