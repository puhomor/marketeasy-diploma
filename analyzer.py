from abc import ABC, abstractmethod
from logger import logger

class ReportAnalyzer(ABC):
    """
    Шаблонный метод для анализа отчётов маркетплейсов
    
    ПРИМЕР ПАТТЕРНА Template Method:
    Метод analyze() определяет НЕИЗМЕНЯЕМЫЙ скелет алгоритма.
    Конкретные шаги (validate, load_data, parse_data, calculate_metrics)
    переопределяются в классах-наследниках.
    """
    
    def __init__(self):
        # ПРИМЕР СОЧЕТАНИЯ ПАТТЕРНОВ:
        # Template Method использует Singleton для логирования
        self.logger = logger
    
    def analyze(self, file_content=None, user_id=None, demo_mode=False):
        """
        Шаблонный метод — определяет скелет алгоритма анализа
        
        ЭТОТ МЕТОД НЕ ПЕРЕОПРЕДЕЛЯЕТСЯ В НАСЛЕДНИКАХ!
        Все наследники используют этот алгоритм, меняя только конкретные шаги.
        """
        self.logger.info("Начало анализа отчёта", user_id)
        
        # Шаг 1: Валидация (разная для разных маркетплейсов)
        if not self.validate(demo_mode):
            self.logger.error("Валидация не пройдена", user_id)
            return None
        
        # Шаг 2: Загрузка данных (разная для разных маркетплейсов)
        data = self.load_data(file_content, demo_mode)
        if data is None:
            self.logger.error("Ошибка загрузки данных", user_id)
            return None
        
        # Шаг 3: Парсинг (разный для разных маркетплейсов)
        parsed = self.parse_data(data, demo_mode)
        
        # Шаг 4: Расчёт метрик (разный для разных маркетплейсов)
        metrics = self.calculate_metrics(parsed)
        
        # Шаг 5: Сохранение (общий для всех маркетплейсов)
        result = self.save_result(metrics, user_id)
        
        self.logger.info(f"Анализ завершён", user_id)
        return result
    
    # Абстрактные методы — ДОЛЖНЫ быть реализованы в наследниках
    @abstractmethod
    def validate(self, demo_mode):
        """Проверка валидности файла (специфична для маркетплейса)"""
        pass
    
    @abstractmethod
    def load_data(self, file_content, demo_mode):
        """Загрузка данных из файла (специфична для маркетплейса)"""
        pass
    
    @abstractmethod
    def parse_data(self, data, demo_mode):
        """Парсинг специфичных полей (разное для WB и Ozon)"""
        pass
    
    @abstractmethod
    def calculate_metrics(self, parsed_data):
        """Расчёт метрик (разные формулы для WB и Ozon)"""
        pass
    
    # Общий метод — можно использовать как есть или переопределить
    def save_result(self, metrics, user_id):
        """Сохранение результата (общее для всех маркетплейсов)"""
        self.logger.info(f"Результат: выручка={metrics.get('revenue', 0)} руб., прибыль={metrics.get('net_profit', 0)} руб.", user_id)
        return {"status": "saved", "metrics": metrics}

class WildberriesAnalyzer(ReportAnalyzer):
    """
    Анализатор для Wildberries
    
    ПРИМЕР ПАТТЕРНА Template Method:
    Переопределяет только те методы, которые отличаются от базового класса.
    """
    
    def validate(self, demo_mode):
        self.logger.info("✓ Валидация пройдена")
        return True
    
    def load_data(self, file_content, demo_mode):
        if demo_mode:
            self.logger.info("✓ Демо-режим: данные сгенерированы")
            return {"demo": True, "revenue": 50000, "logistics": 5000, "storage": 2000}
        
        import pandas as pd
        from io import BytesIO
        self.logger.info("Загрузка реального Excel файла")
        # ПРИМЕР СПЕЦИФИКИ WB: используется engine='openpyxl'
        return pd.read_excel(BytesIO(file_content), engine='openpyxl')
    
    def parse_data(self, data, demo_mode):
        self.logger.info("Парсинг данных")
        if demo_mode:
            return data
        # ПРИМЕР СПЕЦИФИКИ WB: используется колонка 'Вайлдберриз реализовал Товар (Пр)'
        return {'revenue': data['Вайлдберриз реализовал Товар (Пр)'].sum()}
    
    def calculate_metrics(self, parsed):
        self.logger.info("Расчёт метрик Wildberries")
        if parsed.get('demo'):
            revenue = parsed['revenue']
            logistics = parsed['logistics']
            storage = parsed['storage']
        else:
            revenue = parsed['revenue']
            # ПРИМЕР СПЕЦИФИКИ WB: комиссия 5%, хранение 2%
            logistics = revenue * 0.05
            storage = revenue * 0.02
        
        net_profit = revenue - logistics - storage
        return {
            'revenue': revenue,
            'net_profit': net_profit,
            'logistics': logistics,
            'storage': storage
        }

class OzonAnalyzer(ReportAnalyzer):
    """
    Анализатор для Ozon
    
    ПРИМЕР ПАТТЕРНА Template Method:
    Переопределяет только те методы, которые отличаются от базового класса.
    При добавлении нового маркетплейса (например, Яндекс) достаточно создать
    новый класс-наследник и переопределить 4 метода.
    """
    
    def validate(self, demo_mode):
        self.logger.info("✓ Валидация пройдена")
        return True
    
    def load_data(self, file_content, demo_mode):
        if demo_mode:
            self.logger.info("✓ Демо-режим: данные сгенерированы")
            return {"demo": True, "revenue": 30000, "logistics": 4000, "storage": 1500}
        
        import pandas as pd
        from io import BytesIO
        return pd.read_excel(BytesIO(file_content), engine='openpyxl')
    
    def parse_data(self, data, demo_mode):
        self.logger.info("Парсинг данных Ozon")
        if demo_mode:
            return data
        # ПРИМЕР СПЕЦИФИКИ OZON: используется колонка 'Стоимость'
        return {'revenue': data['Стоимость'].sum()}
    
    def calculate_metrics(self, parsed):
        self.logger.info("Расчёт метрик Ozon")
        if parsed.get('demo'):
            revenue = parsed['revenue']
            logistics = parsed['logistics']
            storage = parsed['storage']
        else:
            revenue = parsed['revenue']
            # ПРИМЕР СПЕЦИФИКИ OZON: комиссия 7%, хранение 3%
            logistics = revenue * 0.07
            storage = revenue * 0.03
        
        net_profit = revenue - logistics - storage
        return {
            'revenue': revenue,
            'net_profit': net_profit,
            'logistics': logistics,
            'storage': storage
        }