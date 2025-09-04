"""
RabbitMQ Consumer для обработки COA (Change of Authorization) сообщений.
"""

import asyncio
import json
import logging
import sys
from typing import Dict, Any

import aio_pika
from dotenv import load_dotenv
from pyrad.client import Client
from pyrad import dictionary
from pyrad import packet

from config import config

# Настройка логирования
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S,%f"[:-3],
)
logger = logging.getLogger(__name__)


class RADIUSClient:
    """Клиент для работы с RADIUS CoA пакетами"""

    def __init__(self, server: str, secret: str, timeout: int = 30):
        self.server = server
        self.secret = secret.encode() if isinstance(secret, str) else secret
        self.timeout = timeout
        self.client = None
        self._init_client()

    def _init_client(self):
        """Инициализация RADIUS клиента"""
        try:
            self.client = Client(
                server=self.server,
                secret=self.secret,
                dict=dictionary.Dictionary("dictionary.pyrad"),
            )
            self.client.timeout = self.timeout
            logger.info("RADIUS клиент инициализирован для сервера %s", self.server)
        except Exception as e:
            logger.error("Ошибка инициализации RADIUS клиента: %s", e)
            raise

    def send_coa_request(self, attributes: Dict[str, str]) -> bool:
        """Отправка CoA-Request пакета"""
        try:
            # Преобразуем атрибуты в формат pyrad (заменяем дефисы на подчеркивания)
            pyrad_attributes = {k.replace("-", "_"): v for k, v in attributes.items()}

            logger.info("Отправка CoA-Request с атрибутами: %s", pyrad_attributes)

            # Создаем CoA пакет
            request = self.client.CreateCoAPacket(**pyrad_attributes)

            # Отправляем пакет
            result = self.client.SendPacket(request)

            logger.info("CoA-Request отправлен, результат: %s", result.code)
            return True

        except Exception as e:
            logger.error("Ошибка отправки CoA-Request: %s", e)
            return False

    def send_disconnect_request(self, attributes: Dict[str, str]) -> bool:
        """Отправка Disconnect-Request пакета"""
        try:
            # Преобразуем атрибуты в формат pyrad
            pyrad_attributes = {k.replace("-", "_"): v for k, v in attributes.items()}

            logger.info(
                "Отправка Disconnect-Request с атрибутами: %s", pyrad_attributes
            )

            # Создаем Disconnect пакет
            request = self.client.CreateCoAPacket(
                code=packet.DisconnectRequest, **pyrad_attributes
            )

            # Отправляем пакет
            result = self.client.SendPacket(request)

            logger.info("Disconnect-Request отправлен, результат: %s", result.code)
            return True

        except Exception as e:
            logger.error("Ошибка отправки Disconnect-Request: %s", e)
            return False


class COAConsumer:
    """Консьюмер для обработки COA сообщений из RabbitMQ"""

    def __init__(self, amqp_url: str, queue_name: str = "coa_requests"):
        self.amqp_url = amqp_url
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.queue = None
        # Разрешенные логины для обработки
        self.allowed_logins = {"test8", "police", "test-ag"}

    def is_login_allowed(self, session_data: Dict[str, Any]) -> bool:
        """Проверяет, разрешен ли логин для обработки"""
        username = session_data.get("login", "")
        return username in self.allowed_logins

    async def connect(self):
        """Подключение к RabbitMQ"""
        try:
            self.connection = await aio_pika.connect_robust(self.amqp_url)
            self.channel = await self.connection.channel()
            await self.channel.set_qos(prefetch_count=1)

            # Объявляем очередь
            self.queue = await self.channel.declare_queue(self.queue_name, durable=True)

            logger.info("Подключен к RabbitMQ, очередь: %s", self.queue_name)

        except Exception as e:
            logger.error("Ошибка подключения к RabbitMQ: %s", e)
            raise

    def get_radius_client(self, nas_ip: str) -> RADIUSClient:
        """Получение RADIUS клиента для конкретного NAS"""
        try:
            radius_secret = config.RADIUS_SECRET
            return RADIUSClient(nas_ip, radius_secret)
        except Exception as e:
            logger.error("Ошибка создания RADIUS клиента для %s: %s", nas_ip, e)
            raise

    async def disconnect(self):
        """Отключение от RabbitMQ"""
        if self.connection:
            await self.connection.close()
            logger.info("Отключен от RabbitMQ")

    async def process_message(self, message: aio_pika.IncomingMessage):
        """Обработка входящего сообщения"""
        async with message.process():
            try:
                # Декодируем сообщение
                body = message.body.decode("utf-8")
                data = json.loads(body)

                logger.info(
                    "Получено COA сообщение: %s",
                    json.dumps(data, indent=4, ensure_ascii=False),
                )

                # Обрабатываем сообщение в зависимости от типа
                request_type = data.get("type")
                session_data = data.get("session_data", {})
                attributes = data.get("attributes", {})

                # Проверяем, разрешен ли логин для обработки
                if not self.is_login_allowed(session_data):
                    username = session_data.get("login", "unknown")
                    logger.info(
                        "Сообщение для логина %s отправлено в мертвую очередь", username
                    )
                    return

                if request_type == "kill":
                    await self.handle_kill_request(session_data)
                elif request_type == "update":
                    await self.handle_update_request(session_data, attributes)
                else:
                    logger.warning("Неизвестный тип COA запроса: %s", request_type)

                logger.info(
                    "COA сообщение %s обработано успешно", data.get("request_id")
                )

            except json.JSONDecodeError as e:
                logger.error("Ошибка декодирования JSON: %s", e)
            except Exception as e:
                logger.error("Ошибка обработки сообщения: %s", e, exc_info=True)

    async def handle_kill_request(self, session_data: Dict[str, Any]):
        """Обработка запроса на завершение сессии"""
        session_id = session_data.get("Acct-Session-Id", "unknown")
        nas_ip = session_data.get("NAS-IP-Address")

        if not nas_ip:
            logger.error("NAS-IP-Address не найден в данных сессии %s", session_id)
            return

        logger.info(
            "Отправка CoA Disconnect-Request для сессии %s на NAS %s",
            session_id,
            nas_ip,
        )

        # Формируем атрибуты для завершения сессии
        attributes = self._build_kill_attributes(session_data)

        try:
            # Создаем RADIUS клиент для конкретного NAS
            radius_client = self.get_radius_client(nas_ip)
            success = radius_client.send_disconnect_request(attributes)
            if success:
                logger.info(
                    "Disconnect-Request успешно отправлен для сессии %s", session_id
                )
            else:
                logger.error(
                    "Ошибка отправки Disconnect-Request для сессии %s", session_id
                )
        except Exception as e:
            logger.error(
                "Ошибка при отправке Disconnect-Request для сессии %s: %s",
                session_id,
                e,
            )

    def _build_kill_attributes(self, session_data: Dict[str, Any]) -> Dict[str, str]:
        """Формирование атрибутов для завершения сессии"""
        session_id = session_data.get("Acct-Session-Id", "")

        # Минимальные атрибуты для Disconnect-Request
        attributes = {
            "Acct-Session-Id": session_id,
        }

        return attributes

    async def handle_update_request(
        self, session_data: Dict[str, Any], attributes: Dict[str, Any]
    ):
        """Обработка запроса на обновление атрибутов сессии"""
        session_id = session_data.get("Acct-Session-Id", "unknown")
        nas_ip = session_data.get("NAS-IP-Address")

        if not nas_ip:
            logger.error("NAS-IP-Address не найден в данных сессии %s", session_id)
            return

        logger.info("Отправка CoA-Request для сессии %s на NAS %s", session_id, nas_ip)
        logger.info("Атрибуты для обновления: %s", attributes)

        # Формируем атрибуты для CoA-Request
        coa_attributes = {
            "Acct-Session-Id": session_id,
        }

        # Добавляем пользовательские атрибуты
        coa_attributes.update(attributes)

        try:
            # Создаем RADIUS клиент для конкретного NAS
            radius_client = self.get_radius_client(nas_ip)
            success = radius_client.send_coa_request(coa_attributes)
            if success:
                logger.info("CoA-Request успешно отправлен для сессии %s", session_id)
            else:
                logger.error("Ошибка отправки CoA-Request для сессии %s", session_id)
        except Exception as e:
            logger.error(
                "Ошибка при отправке CoA-Request для сессии %s: %s", session_id, e
            )

    async def start_consuming(self):
        """Начало потребления сообщений"""
        try:
            # Начинаем потребление
            await self.queue.consume(self.process_message)
            logger.info("Начато потребление COA сообщений")

            # Держим соединение активным
            await asyncio.Future()  # Бесконечное ожидание

        except Exception as e:
            logger.error("Ошибка при потреблении сообщений: %s", e)
            raise


async def main():
    """Главная функция"""
    # Загружаем переменные окружения
    load_dotenv()

    # Проверяем корректность конфигурации
    if not config.validate():
        logger.error("Некорректная конфигурация приложения")
        sys.exit(1)

    # Получаем настройки
    amqp_url = config.AMQP_URL
    queue_name = config.AMQP_COA_QUEUE

    if not amqp_url:
        logger.error("AMQP_URL не задан в переменных окружения")
        sys.exit(1)

    consumer = COAConsumer(amqp_url, queue_name)

    try:
        # Подключаемся к RabbitMQ
        await consumer.connect()

        # Начинаем потребление
        await consumer.start_consuming()

    except KeyboardInterrupt:
        logger.info("Получен сигнал завершения")
    except Exception as e:
        logger.error("Критическая ошибка: %s", e, exc_info=True)
    finally:
        # Отключаемся
        await consumer.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Программа завершена пользователем")
    except Exception as e:
        logger.error("Неожиданная ошибка: %s", e, exc_info=True)
        sys.exit(1)
