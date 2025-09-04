"""
Конфигурация приложения COA Consumer
"""

import os
from typing import Optional
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()


class Config:
    """Класс конфигурации приложения"""

    # RabbitMQ настройки
    AMQP_URL: str = os.getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/")
    AMQP_COA_QUEUE: str = os.getenv("AMQP_COA_QUEUE", "coa_requests")

    # RADIUS настройки
    RADIUS_SECRET: str = os.getenv("RADIUS_SECRET", "123456")
    RADIUS_TIMEOUT: int = int(os.getenv("RADIUS_TIMEOUT", "30"))

    # Логирование
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    # Настройки приложения
    MAX_RETRY_ATTEMPTS: int = int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))
    RETRY_DELAY: float = float(os.getenv("RETRY_DELAY", "1.0"))

    @classmethod
    def validate(cls) -> bool:
        """Проверка корректности конфигурации"""
        required_vars = ["AMQP_URL"]

        for var in required_vars:
            if not getattr(cls, var):
                return False

        return True

    @classmethod
    def get_radius_config(cls) -> dict:
        """Получение конфигурации RADIUS"""
        return {
            "secret": cls.RADIUS_SECRET,
            "timeout": cls.RADIUS_TIMEOUT,
        }

    @classmethod
    def get_amqp_config(cls) -> dict:
        """Получение конфигурации RabbitMQ"""
        return {"url": cls.AMQP_URL, "queue": cls.AMQP_COA_QUEUE}


# Создаем экземпляр конфигурации
config = Config()
