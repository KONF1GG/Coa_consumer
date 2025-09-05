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
        self.secret = secret
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

    def send_coa_request(self, attributes: Dict[str, str]) -> tuple[bool, str]:
        """Отправка CoA-Request пакета

        Returns:
            tuple[bool, str]: (success, reason) - успех операции и причина (при неуспехе)
        """
        try:
            # Преобразуем атрибуты в формат pyrad (заменяем дефисы на подчеркивания)
            pyrad_attributes = {k.replace("-", "_"): v for k, v in attributes.items()}

            logger.info("Отправка CoA-Request с атрибутами: %s", pyrad_attributes)

            # Создаем CoA пакет
            request = self.client.CreateCoAPacket(**pyrad_attributes)

            # Отправляем пакет
            result = self.client.SendPacket(request)

            logger.info("CoA-Request отправлен, результат: %s", result.code)

            # Проверяем код ответа
            if result.code == packet.CoAACK:
                return True, "CoA-Request успешно выполнен"
            elif result.code == packet.CoANAK:
                return False, f"CoA-Request отклонен (CoA-NAK), код: {result.code}"
            else:
                return False, f"Неожиданный код ответа: {result.code}"

        except Exception as e:
            logger.error("Ошибка отправки CoA-Request: %s", e)
            return False, f"Исключение при отправке CoA-Request: {str(e)}"

    def send_disconnect_request(self, attributes: Dict[str, str]) -> tuple[bool, str]:
        """Отправка Disconnect-Request пакета

        Returns:
            tuple[bool, str]: (success, reason) - успех операции и причина (при неуспехе)
        """
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

            # Проверяем код ответа
            if result.code == packet.DisconnectACK:
                return True, "Disconnect-Request успешно выполнен"
            elif result.code == packet.DisconnectNAK:
                return (
                    False,
                    f"Disconnect-Request отклонен (Disconnect-NAK), код: {result.code}",
                )
            else:
                return False, f"Неожиданный код ответа: {result.code}"

        except Exception as e:
            logger.error("Ошибка отправки Disconnect-Request: %s", e)
            return False, f"Исключение при отправке Disconnect-Request: {str(e)}"


class COAConsumer:
    """Консьюмер для обработки COA сообщений из RabbitMQ"""

    def __init__(
        self,
        amqp_url: str,
        exchange_name: str = "coa_exchange",
        dlq_exchange_name: str = "coa_dlq_exchange",
        queue_name: str = "coa_requests",
        dlq_name: str = "dlq_coa_requests",
    ):
        self.amqp_url = amqp_url
        self.exchange_name = exchange_name
        self.dlq_exchange_name = dlq_exchange_name
        self.queue_name = queue_name
        self.dlq_name = dlq_name
        self.connection = None
        self.channel = None
        self.exchange = None
        self.dlq_exchange = None
        self.queue = None
        self.dlq_queue = None
        # Разрешенные NAS IP адреса для обработки
        self.allowed_nas_ips = {"10.10.1.12"}

    def is_nas_allowed(self, session_data: Dict[str, Any]) -> bool:
        """Проверяет, разрешен ли NAS IP для обработки"""
        nas_ip = session_data.get("NAS-IP-Address", "")
        return nas_ip in self.allowed_nas_ips

    async def send_to_dlq(self, original_message: Dict[str, Any], error_reason: str):
        """Отправляет сообщение в мертвую очередь с описанием ошибки"""
        try:
            # Добавляем информацию об ошибке к оригинальному сообщению
            dlq_message = original_message.copy()
            dlq_message["dlq_info"] = {
                "error_reason": error_reason,
                "timestamp": asyncio.get_event_loop().time(),
                "original_queue": self.queue_name,
            }

            # Отправляем в мертвую очередь через DLQ exchange
            await self.dlq_exchange.publish(
                aio_pika.Message(
                    json.dumps(dlq_message, ensure_ascii=False).encode("utf-8"),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key="dlq.coa",
            )

            logger.info("Сообщение отправлено в мертвую очередь: %s", error_reason)

        except Exception as e:
            logger.error("Ошибка отправки в мертвую очередь: %s", e)

    async def connect(self):
        """Подключение к RabbitMQ"""
        try:
            self.connection = await aio_pika.connect_robust(self.amqp_url)
            self.channel = await self.connection.channel()
            await self.channel.set_qos(prefetch_count=1)

            # Создаем COA exchange (topic для гибкой маршрутизации)
            self.exchange = await self.channel.declare_exchange(
                self.exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
            )

            # Создаем DLQ exchange (direct для простой маршрутизации)
            self.dlq_exchange = await self.channel.declare_exchange(
                self.dlq_exchange_name, aio_pika.ExchangeType.DIRECT, durable=True
            )

            # Создаем мертвую очередь и привязываем к DLQ exchange
            self.dlq_queue = await self.channel.declare_queue(
                self.dlq_name, durable=True
            )
            await self.dlq_queue.bind(
                self.dlq_exchange,
                routing_key="dlq.coa",  # Простой routing key для DLQ
            )

            # Создаем основную очередь с настройками для мертвой очереди
            self.queue = await self.channel.declare_queue(
                self.queue_name,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": self.dlq_exchange_name,  # Используем DLQ exchange
                    "x-dead-letter-routing-key": "dlq.coa",  # Маршрутизируем в DLQ
                },
            )

            # Привязываем основную очередь к exchange с routing key
            await self.queue.bind(
                self.exchange,
                routing_key="coa.request.*",  # Принимаем все COA запросы
            )

            logger.info(
                "Подключен к RabbitMQ, exchange: %s, DLQ exchange: %s, очередь: %s, DLQ: %s",
                self.exchange_name,
                self.dlq_exchange_name,
                self.queue_name,
                self.dlq_name,
            )

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

                # Проверяем, разрешен ли NAS IP для обработки
                if not self.is_nas_allowed(session_data):
                    nas_ip = session_data.get("NAS-IP-Address", "unknown")
                    error_reason = f"NAS IP '{nas_ip}' не разрешен для обработки"
                    logger.info(
                        "Сообщение для NAS %s отправлено в мертвую очередь", nas_ip
                    )
                    await self.send_to_dlq(data, error_reason)
                    return

                if request_type == "kill":
                    await self.handle_kill_request(session_data, data)
                elif request_type == "update":
                    await self.handle_update_request(session_data, attributes, data)
                else:
                    error_reason = f"Неизвестный тип COA запроса: {request_type}"
                    logger.warning(error_reason)
                    await self.send_to_dlq(data, error_reason)
                    return

                logger.info(
                    "COA сообщение %s обработано успешно", data.get("request_id")
                )

            except json.JSONDecodeError as e:
                error_reason = f"Ошибка декодирования JSON: {str(e)}"
                logger.error(error_reason)
                # Для JSON ошибок пытаемся отправить в DLQ оригинальное сообщение
                try:
                    raw_body = message.body.decode("utf-8")
                    dlq_data = {"raw_message": raw_body, "error": "json_decode_error"}
                    await self.send_to_dlq(dlq_data, error_reason)
                except Exception as dlq_error:
                    logger.error(
                        "Не удалось отправить в DLQ после JSON ошибки: %s", dlq_error
                    )
            except Exception as e:
                error_reason = f"Ошибка обработки сообщения: {str(e)}"
                logger.error(error_reason, exc_info=True)
                # Для других ошибок пытаемся отправить в DLQ
                try:
                    raw_body = message.body.decode("utf-8")
                    dlq_data = {"raw_message": raw_body, "error": "processing_error"}
                    await self.send_to_dlq(dlq_data, error_reason)
                except Exception as dlq_error:
                    logger.error(
                        "Не удалось отправить в DLQ после ошибки обработки: %s",
                        dlq_error,
                    )

    async def handle_kill_request(
        self, session_data: Dict[str, Any], original_message: Dict[str, Any]
    ):
        """Обработка запроса на завершение сессии"""
        session_id = session_data.get("Acct-Session-Id", "unknown")
        nas_ip = session_data.get("NAS-IP-Address")

        if not nas_ip:
            error_reason = f"NAS-IP-Address не найден в данных сессии {session_id}"
            logger.error(error_reason)
            await self.send_to_dlq(original_message, error_reason)
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
            success, reason = radius_client.send_disconnect_request(attributes)
            if success:
                logger.info(
                    "Disconnect-Request успешно отправлен для сессии %s", session_id
                )
            else:
                error_reason = (
                    f"Disconnect-Request неуспешен для сессии {session_id}: {reason}"
                )
                logger.error(error_reason)
                await self.send_to_dlq(original_message, error_reason)
        except Exception as e:
            error_reason = f"Ошибка при отправке Disconnect-Request для сессии {session_id}: {str(e)}"
            logger.error(error_reason)
            await self.send_to_dlq(original_message, error_reason)

    def _build_kill_attributes(self, session_data: Dict[str, Any]) -> Dict[str, str]:
        """Формирование атрибутов для завершения сессии"""
        session_id = session_data.get("Acct-Session-Id", "")

        # Минимальные атрибуты для Disconnect-Request
        attributes = {
            "Acct-Session-Id": session_id,
        }

        return attributes

    async def handle_update_request(
        self,
        session_data: Dict[str, Any],
        attributes: Dict[str, Any],
        original_message: Dict[str, Any],
    ):
        """Обработка запроса на обновление атрибутов сессии"""
        session_id = session_data.get("Acct-Session-Id", "unknown")
        nas_ip = session_data.get("NAS-IP-Address")

        if not nas_ip:
            error_reason = f"NAS-IP-Address не найден в данных сессии {session_id}"
            logger.error(error_reason)
            await self.send_to_dlq(original_message, error_reason)
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
            success, reason = radius_client.send_coa_request(coa_attributes)
            if success:
                logger.info("CoA-Request успешно отправлен для сессии %s", session_id)
            else:
                error_reason = (
                    f"CoA-Request неуспешен для сессии {session_id}: {reason}"
                )
                logger.error(error_reason)
                await self.send_to_dlq(original_message, error_reason)
        except Exception as e:
            error_reason = (
                f"Ошибка при отправке CoA-Request для сессии {session_id}: {str(e)}"
            )
            logger.error(error_reason)
            await self.send_to_dlq(original_message, error_reason)

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
    exchange_name = config.AMQP_COA_EXCHANGE
    dlq_exchange_name = config.AMQP_DLQ_EXCHANGE
    queue_name = config.AMQP_COA_QUEUE
    dlq_name = config.AMQP_DLQ_QUEUE

    if not amqp_url:
        logger.error("AMQP_URL не задан в переменных окружения")
        sys.exit(1)

    consumer = COAConsumer(
        amqp_url, exchange_name, dlq_exchange_name, queue_name, dlq_name
    )

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
