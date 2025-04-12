"""
Модуль для проверки валидности YouTube API ключей.
Проверяет каждый ключ из файла api.txt на работоспособность
и сохраняет валидные ключи в файл Good_API.txt.
Расширенная версия с поддержкой многопоточности, мониторингом квот и аналитикой.
Оптимизированная версия с улучшенным управлением ресурсами и обработкой ошибок.
"""

import os
import logging
import time
import json
import traceback
import datetime
import threading
import platform
import signal
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Попытка импорта psutil для мониторинга системных ресурсов
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

# Константы для YouTube API
YT_API_QUOTA_LIMIT = 10000  # Стандартный дневной лимит YouTube API
YT_API_MIN_UNITS_TEST = 1   # Минимальное количество единиц для тестового запроса

class RateLimiter:
    """Ограничитель скорости для API запросов с поддержкой токенов и временных интервалов."""
    
    def __init__(self, max_requests=10, time_window=1.0):
        """
        Инициализация лимитера.
        
        :param max_requests: Максимальное количество запросов в окне
        :param time_window: Размер временного окна в секундах
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.tokens = max_requests
        self.last_refill = time.time()
        self.lock = threading.RLock()
    
    def acquire(self, block=True, timeout=None):
        """
        Получить токен для выполнения запроса.
        
        :param block: Блокировать до получения токена
        :param timeout: Таймаут ожидания в секундах
        :return: True если токен получен, иначе False
        """
        start_time = time.time()
        
        while True:
            with self.lock:
                # Обновляем токены на основе прошедшего времени
                current_time = time.time()
                elapsed = current_time - self.last_refill
                
                # Пополняем токены пропорционально прошедшему времени
                if elapsed > 0:
                    new_tokens = elapsed * (self.max_requests / self.time_window)
                    self.tokens = min(self.max_requests, self.tokens + new_tokens)
                    self.last_refill = current_time
                
                # Если есть доступные токены, используем один
                if self.tokens >= 1:
                    self.tokens -= 1
                    return True
            
            # Если не блокирующий режим, сразу возвращаем False
            if not block:
                return False
            
            # Проверка таймаута
            if timeout is not None:
                if time.time() - start_time > timeout:
                    return False
            
            # Ожидаем короткое время перед следующей проверкой
            time.sleep(0.1)

class CachedDiscovery:
    """Кэш для документов Google API Discovery."""
    
    def __init__(self, max_size=10):
        """
        Инициализация кэша.
        
        :param max_size: Максимальный размер кэша
        """
        self.cache = {}
        self.max_size = max_size
        self.lock = threading.RLock()
    
    def get(self, service_name, version, api_key):
        """
        Получить сервис из кэша или создать новый.
        
        :param service_name: Имя сервиса
        :param version: Версия сервиса
        :param api_key: Ключ API
        :return: Объект сервиса
        """
        # Создаем "ключ" для кэша на основе хэша от параметров
        cache_key = self._make_cache_key(service_name, version, api_key)
        
        with self.lock:
            # Проверка наличия в кэше
            if cache_key in self.cache:
                return self.cache[cache_key]
            
            # Создание нового сервиса
            service = build(service_name, version, developerKey=api_key, cache_discovery=False)
            
            # Сохранение в кэш
            if len(self.cache) >= self.max_size:
                # Удаляем случайный элемент при переполнении
                # В реальном сценарии можно использовать LRU или другую стратегию
                if self.cache:
                    self.cache.pop(next(iter(self.cache)))
            
            self.cache[cache_key] = service
            return service
    
    def _make_cache_key(self, service_name, version, api_key):
        """Создать ключ для кэша на основе входных параметров."""
        # Используем только начало и конец ключа для хэша
        masked_key = f"{api_key[:4]}...{api_key[-4:]}"
        key_str = f"{service_name}_{version}_{masked_key}"
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def clear(self):
        """Очистить кэш."""
        with self.lock:
            self.cache.clear()

class SystemMonitor:
    """Мониторинг системных ресурсов."""
    
    def __init__(self):
        """Инициализация монитора."""
        self.psutil_available = PSUTIL_AVAILABLE
        self.start_time = time.time()
        self.stats = {
            "cpu_percent": [],
            "memory_percent": [],
            "io_counters": [],
            "timestamps": []
        }
        self.monitoring = False
        self.monitor_thread = None
        self.lock = threading.RLock()
    
    def start_monitoring(self, interval=1.0):
        """
        Начать мониторинг системных ресурсов в отдельном потоке.
        
        :param interval: Интервал сбора статистики в секундах
        """
        if not self.psutil_available:
            logging.warning("psutil не доступен. Мониторинг системных ресурсов отключен.")
            return
        
        with self.lock:
            if self.monitoring:
                return
            
            self.monitoring = True
            self.monitor_thread = threading.Thread(
                target=self._monitor_loop,
                args=(interval,),
                daemon=True
            )
            self.monitor_thread.start()
    
    def stop_monitoring(self):
        """Остановить мониторинг системных ресурсов."""
        with self.lock:
            self.monitoring = False
            if self.monitor_thread:
                self.monitor_thread.join(timeout=2.0)
                self.monitor_thread = None
    
    def _monitor_loop(self, interval):
        """
        Цикл сбора данных о системных ресурсах.
        
        :param interval: Интервал сбора данных в секундах
        """
        while self.monitoring:
            try:
                self.collect_stats()
                time.sleep(interval)
            except Exception as e:
                logging.error(f"Ошибка при сборе статистики: {str(e)}")
                # Продолжаем работу несмотря на ошибки
    
    def collect_stats(self):
        """Собрать текущую статистику системных ресурсов."""
        if not self.psutil_available:
            return
        
        try:
            with self.lock:
                # Ограничим сохраняемую историю
                max_history = 60  # Хранить данные за последнюю минуту (при интервале 1 сек)
                
                # Обрезаем историю при необходимости
                if len(self.stats["timestamps"]) >= max_history:
                    for key in self.stats:
                        if isinstance(self.stats[key], list) and len(self.stats[key]) >= max_history:
                            self.stats[key] = self.stats[key][-max_history:]
                
                # Добавляем текущие значения
                self.stats["timestamps"].append(time.time())
                self.stats["cpu_percent"].append(psutil.cpu_percent(interval=0.1))
                self.stats["memory_percent"].append(psutil.virtual_memory().percent)
                
                # Сохраняем дисковую активность (может быть недоступно в некоторых ОС)
                try:
                    io = psutil.disk_io_counters()
                    self.stats["io_counters"].append({
                        "read_bytes": io.read_bytes,
                        "write_bytes": io.write_bytes
                    })
                except:
                    self.stats["io_counters"].append(None)
        except Exception as e:
            logging.debug(f"Ошибка при сборе системной статистики: {e}")
    
    def get_stats_summary(self):
        """
        Получить сводку статистики.
        
        :return: Словарь со сводной статистикой
        """
        if not self.psutil_available or not self.stats["cpu_percent"]:
            return {
                "available": False,
                "runtime_seconds": time.time() - self.start_time
            }
        
        with self.lock:
            try:
                cpu_stats = self.stats["cpu_percent"]
                memory_stats = self.stats["memory_percent"]
                
                summary = {
                    "available": True,
                    "cpu_current": cpu_stats[-1] if cpu_stats else None,
                    "cpu_avg": sum(cpu_stats) / len(cpu_stats) if cpu_stats else None,
                    "cpu_max": max(cpu_stats) if cpu_stats else None,
                    "memory_current": memory_stats[-1] if memory_stats else None,
                    "memory_avg": sum(memory_stats) / len(memory_stats) if memory_stats else None,
                    "memory_max": max(memory_stats) if memory_stats else None,
                    "runtime_seconds": time.time() - self.start_time
                }
                
                # Добавляем информацию о загруженности подсистемы ввода-вывода
                io_counters = self.stats["io_counters"]
                if io_counters and len(io_counters) > 1 and io_counters[-1] and io_counters[0]:
                    summary["io_bytes_read"] = io_counters[-1]["read_bytes"] - io_counters[0]["read_bytes"]
                    summary["io_bytes_write"] = io_counters[-1]["write_bytes"] - io_counters[0]["write_bytes"]
                
                return summary
            except Exception as e:
                logging.debug(f"Ошибка при создании сводки статистики: {e}")
                return {
                    "available": False,
                    "error": str(e),
                    "runtime_seconds": time.time() - self.start_time
                }

class YouTubeAPIValidator:
    """Валидатор ключей YouTube API с расширенными возможностями."""
    
    def __init__(self, api_file='api.txt', output_file='Good_API.txt', max_workers=None, delay_between=1.0):
        """
        Инициализация валидатора.
        
        :param api_file: Путь к файлу с ключами API
        :param output_file: Путь к файлу для сохранения валидных ключей
        :param max_workers: Максимальное количество рабочих потоков (None=автоопределение)
        :param delay_between: Задержка между запросами в секундах
        """
        self.api_file = api_file
        self.output_file = output_file
        self.api_keys = []
        self.valid_keys = []
        self.invalid_keys = []
        self.quota_exceeded_keys = []
        
        # Определение оптимального количества потоков
        self.max_workers = self._get_optimal_thread_count() if max_workers is None else max_workers
        
        self.delay_between = delay_between
        self.quota_database_file = "api_keys_quota.json"
        self.quota_database = {}
        self.analytics_file = "api_usage_analytics.json"
        self.analytics_data = {
            "daily_usage": {},
            "hourly_usage": {},
            "quota_history": [],
            "system_stats": {}
        }
        
        # Для отслеживания прогресса
        self.progress_callback = None
        self.total_keys = 0
        self.processed_keys = 0
        self.cancel_validation = False
        
        # Компоненты для оптимизации работы
        self.rate_limiter = RateLimiter(max_requests=5, time_window=1.0)  # 5 запросов в секунду
        self.discovery_cache = CachedDiscovery(max_size=10)
        self.system_monitor = SystemMonitor()
        
        # События для синхронизации потоков
        self.validation_complete_event = threading.Event()
        self.pause_event = threading.Event()
        self.pause_event.set()  # Изначально не приостановлено
        
        # Настройка логирования
        self.setup_logging()
        
        # Загрузка данных о квотах и аналитики
        self.load_quota_database()
        self.load_analytics_data()
        
        # Настройка обработчика сигналов для корректного выхода
        self._setup_signal_handlers()
    
    def _get_optimal_thread_count(self):
        """
        Определение оптимального количества потоков на основе характеристик системы.
        
        :return: Оптимальное количество потоков
        """
        try:
            # Если доступен psutil, используем его для более точного определения
            if PSUTIL_AVAILABLE:
                cpu_count = psutil.cpu_count(logical=True)
                if cpu_count is None:
                    cpu_count = os.cpu_count() or 4
                
                # Проверяем текущую загрузку системы
                cpu_percent = psutil.cpu_percent(interval=0.5)
                memory_percent = psutil.virtual_memory().percent
                
                # Если система уже сильно загружена, уменьшаем количество потоков
                if cpu_percent > 80 or memory_percent > 80:
                    return max(1, min(2, cpu_count // 2))
                
                # Используем количество логических ядер, но оставляем запас
                return max(1, cpu_count - 1)
            else:
                # Запасной вариант, если psutil недоступен
                cpu_count = os.cpu_count() or 4
                return max(1, cpu_count - 1)
        except Exception as e:
            logging.debug(f"Ошибка при определении оптимального количества потоков: {e}")
            # Возвращаем разумное значение по умолчанию
            return 4
    
    def _setup_signal_handlers(self):
        """Настройка обработчиков сигналов для корректного завершения."""
        # Обработка сигналов завершения в поддерживаемых системах
        if platform.system() != "Windows":  # В Windows другой механизм сигналов
            try:
                signal.signal(signal.SIGINT, self._signal_handler)
                signal.signal(signal.SIGTERM, self._signal_handler)
            except (AttributeError, ValueError):
                # Не во всех средах можно настроить обработчики сигналов
                pass
    
    def _signal_handler(self, sig, frame):
        """
        Обработчик сигналов завершения.
        
        :param sig: Номер сигнала
        :param frame: Текущий стек вызовов
        """
        logging.info(f"Получен сигнал {sig}, начинаем корректное завершение...")
        self.cancel_validation = True
        
        # Ожидаем завершения валидации
        if not self.validation_complete_event.is_set():
            logging.info("Ожидание завершения текущих операций...")
            # Ждем максимум 5 секунд
            if not self.validation_complete_event.wait(timeout=5.0):
                logging.warning("Превышено время ожидания, принудительное завершение.")
        
        # Останавливаем мониторинг системы
        self.system_monitor.stop_monitoring()
        
        # Сохраняем результаты
        self.save_valid_keys()
        self.save_quota_database()
        self.save_analytics_data()
        
        logging.info("Корректное завершение выполнено.")
    
    def setup_logging(self):
        """Настройка логирования с поддержкой ротации файлов."""
        # Создаем директорию для логов, если она не существует
        if not os.path.exists('logs'):
            os.makedirs('logs')
            
        # Создаем имя файла лога с текущей датой
        log_file = f"logs/api_validator_{datetime.datetime.now().strftime('%Y-%m-%d')}.log"
        
        # Настраиваем логирование
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
    
    def load_api_keys(self):
        """
        Загрузка API ключей из файла.
        
        :return: True если загрузка успешна, иначе False
        """
        logging.info(f"Загрузка API ключей из файла {self.api_file}...")
        
        try:
            if not os.path.exists(self.api_file):
                logging.error(f"Файл {self.api_file} не найден!")
                self._create_empty_api_file()
                return False
                
            with open(self.api_file, 'r', encoding='utf-8') as f:
                # Загрузка и очистка ключей
                self.api_keys = []
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        # Базовая проверка формата ключа
                        if len(line) >= 30:  # API ключи обычно длинные
                            self.api_keys.append(line)
                        else:
                            logging.warning(f"Пропуск ключа с подозрительной длиной: {line[:5]}...")
            
            if not self.api_keys:
                logging.error(f"В файле {self.api_file} не найдено API ключей!")
                return False
                
            logging.info(f"Загружено {len(self.api_keys)} API ключей")
            self.total_keys = len(self.api_keys)
            return True
            
        except Exception as e:
            logging.error(f"Ошибка при загрузке API ключей: {str(e)}")
            logging.debug(traceback.format_exc())
            return False
    
    def _create_empty_api_file(self):
        """Создание пустого файла API с шаблоном."""
        try:
            with open(self.api_file, 'w', encoding='utf-8') as f:
                f.write("# Введите ваши YouTube API ключи здесь, по одному на строку\n")
            logging.info(f"Создан пустой файл {self.api_file}")
        except Exception as e:
            logging.error(f"Ошибка при создании файла API: {str(e)}")
    
    def validate_api_key(self, api_key):
        """
        Проверка валидности отдельного API ключа с анализом квоты.
        
        :param api_key: Ключ API для проверки
        :return: (is_valid, error_type, quota_info) - результат проверки
        """
        # Проверка сигнала отмены
        if self.cancel_validation:
            return False, "canceled", {"status": "canceled"}
        
        # Проверка приостановки
        self.pause_event.wait()
        
        # Задержка для соблюдения ограничений API
        self.rate_limiter.acquire()
        
        try:
            # Получаем сервис из кэша
            service = self.discovery_cache.get('youtube', 'v3', api_key)
            
            # Выполняем минимальный запрос (стоимость: 1 единица квоты)
            request = service.channels().list(
                part="id,statistics",
                maxResults=1,
                fields="items/id,items/statistics/subscriberCount",
                id="UC_x5XG1OV2P6uZZ5FSM9Ttw"  # Google Developers channel
            )
            
            start_time = time.time()
            response = request.execute()
            response_time = time.time() - start_time
            
            # Собираем информацию о квоте и производительности
            quota_info = {
                "last_checked": datetime.datetime.now().isoformat(),
                "status": "valid",
                "response_time": response_time,
                "units_used": YT_API_MIN_UNITS_TEST,
                "estimated_daily_limit": YT_API_QUOTA_LIMIT,
                "estimated_remaining": YT_API_QUOTA_LIMIT - self.get_used_quota(api_key) - YT_API_MIN_UNITS_TEST
            }
            
            # Если удалось получить subscriber_count, добавляем его в ответ для показа
            try:
                subscriber_count = response['items'][0]['statistics']['subscriberCount']
                quota_info["test_channel_subscribers"] = int(subscriber_count)
            except (KeyError, IndexError, ValueError):
                quota_info["test_channel_subscribers"] = None
            
            # Сохраняем информацию о квоте
            self.update_quota_info(api_key, quota_info)
            
            # Обновляем аналитику
            self.update_analytics(api_key, "valid_request", YT_API_MIN_UNITS_TEST)
            
            # Если запрос успешен, ключ валидный
            logging.info(f"API ключ {api_key[:5]}...{api_key[-5:]} валиден (отклик: {response_time:.2f}s)")
            return True, "valid", quota_info
            
        except HttpError as e:
            error_code = getattr(e, 'status_code', 0)
            error_reason = str(e)
            
            # Проверка на превышение квоты
            if "quota" in error_reason.lower():
                quota_info = {
                    "last_checked": datetime.datetime.now().isoformat(),
                    "status": "quota_exceeded",
                    "error_code": error_code,
                    "error_reason": error_reason,
                    "estimated_remaining": 0
                }
                
                # Обновляем информацию о квоте
                self.update_quota_info(api_key, quota_info)
                self.update_analytics(api_key, "quota_exceeded", 0)
                
                logging.warning(f"API ключ {api_key[:5]}...{api_key[-5:]} превысил квоту: {error_reason}")
                return False, "quota_exceeded", quota_info
            
            # Проверка на ошибки авторизации (невалидный ключ)
            elif error_code in [400, 403]:
                # Более детальная классификация ошибок 403
                if "accessNotConfigured" in error_reason:
                    error_subtype = "access_not_configured"
                    error_message = "API YouTube не включен для этого проекта"
                elif "invalid API key" in error_reason.lower():
                    error_subtype = "invalid_key"
                    error_message = "Недействительный ключ API"
                elif "API key not valid" in error_reason or "API key expired" in error_reason:
                    error_subtype = "key_expired"
                    error_message = "Ключ API истек или недействителен"
                elif "keyInvalid" in error_reason:
                    error_subtype = "key_invalid"
                    error_message = "Недействительный формат ключа API"
                elif "permission" in error_reason.lower() or "forbidden" in error_reason.lower():
                    error_subtype = "permission_denied"
                    error_message = "Отказано в доступе (недостаточно прав)"
                else:
                    error_subtype = "authorization_error"
                    error_message = f"Ошибка авторизации: {error_reason}"
                
                quota_info = {
                    "last_checked": datetime.datetime.now().isoformat(),
                    "status": "invalid",
                    "error_code": error_code,
                    "error_subtype": error_subtype,
                    "error_message": error_message,
                    "error_reason": error_reason
                }
                
                # Обновляем информацию о квоте
                self.update_quota_info(api_key, quota_info)
                self.update_analytics(api_key, f"invalid_key_{error_subtype}", 0)
                
                logging.error(f"API ключ {api_key[:5]}...{api_key[-5:]} невалиден: {error_message}")
                return False, "invalid", quota_info
            
            # Проверка на ошибки превышения частоты запросов
            elif error_code == 429:
                quota_info = {
                    "last_checked": datetime.datetime.now().isoformat(),
                    "status": "rate_limited",
                    "error_code": error_code,
                    "error_reason": error_reason
                }
                
                # Обновляем информацию о квоте
                self.update_quota_info(api_key, quota_info)
                self.update_analytics(api_key, "rate_limited", 0)
                
                logging.warning(f"API ключ {api_key[:5]}...{api_key[-5:]} превысил ограничение частоты запросов")
                
                # Добавляем дополнительную задержку перед следующим запросом
                time.sleep(2.0)
                
                # Пробуем еще раз с тем же ключом, но с другой задержкой
                return self.validate_api_key(api_key)
            
            # Ошибки сервера
            elif error_code >= 500:
                quota_info = {
                    "last_checked": datetime.datetime.now().isoformat(),
                    "status": "server_error",
                    "error_code": error_code,
                    "error_reason": error_reason
                }
                
                # Обновляем информацию о квоте
                self.update_quota_info(api_key, quota_info)
                self.update_analytics(api_key, "server_error", 0)
                
                logging.warning(f"Ошибка сервера {error_code} при проверке ключа {api_key[:5]}...{api_key[-5:]}")
                
                # Добавляем задержку перед повторной попыткой
                time.sleep(2.0)
                
                # Для серверных ошибок пробуем еще раз
                return self.validate_api_key(api_key)
            
            # Другие ошибки
            else:
                quota_info = {
                    "last_checked": datetime.datetime.now().isoformat(),
                    "status": "error",
                    "error_code": error_code,
                    "error_reason": error_reason
                }
                
                # Обновляем информацию о квоте
                self.update_quota_info(api_key, quota_info)
                self.update_analytics(api_key, "other_error", 0)
                
                logging.error(f"Ошибка при проверке API ключа {api_key[:5]}...{api_key[-5:]}: {error_reason}")
                return False, "error", quota_info
                
        except Exception as e:
            quota_info = {
                "last_checked": datetime.datetime.now().isoformat(),
                "status": "error",
                "error_message": str(e)
            }
            
            # Обновляем информацию о квоте
            self.update_quota_info(api_key, quota_info)
            self.update_analytics(api_key, "unexpected_error", 0)
            
            logging.error(f"Неожиданная ошибка при проверке API ключа {api_key[:5]}...{api_key[-5:]}: {str(e)}")
            logging.debug(traceback.format_exc())
            return False, "error", quota_info
    
    def save_valid_keys(self):
        """
        Сохранение валидных API ключей в выходной файл.
        
        :return: True если сохранение успешно, иначе False
        """
        try:
            # Создаем новый файл или перезаписываем существующий
            with open(self.output_file, 'w', encoding='utf-8') as f:
                f.write("# Валидные YouTube API ключи\n")
                f.write(f"# Проверены: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                
                if self.valid_keys:
                    # Сортируем ключи по оставшейся квоте (по убыванию)
                    sorted_keys = sorted(self.valid_keys, key=lambda k: self.get_estimated_remaining_quota(k), reverse=True)
                    
                    for key in sorted_keys:
                        # Добавляем комментарий с информацией о квоте
                        remaining = self.get_estimated_remaining_quota(key)
                        f.write(f"{key}  # Осталось ~{remaining} единиц квоты\n")
                else:
                    f.write("# Валидные ключи не найдены\n")
                    
            logging.info(f"Сохранено {len(self.valid_keys)} валидных API ключей в файл {self.output_file}")
            
            # Дополнительно сохраняем ключи в отдельные файлы по категориям
            self._save_categorized_keys()
            
            return True
            
        except Exception as e:
            logging.error(f"Ошибка при сохранении валидных API ключей: {str(e)}")
            logging.debug(traceback.format_exc())
            return False
    
    def _save_categorized_keys(self):
        """Сохранение ключей по категориям в отдельные файлы."""
        try:
            # Сохраняем ключи с исчерпанной квотой
            if self.quota_exceeded_keys:
                with open('Quota_Exceeded_API.txt', 'w', encoding='utf-8') as f:
                    f.write("# API ключи с исчерпанной дневной квотой\n")
                    f.write(f"# Проверены: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    
                    for key in self.quota_exceeded_keys:
                        # Добавляем комментарий с информацией о времени последней проверки
                        last_checked = self.quota_database.get(key, {}).get("last_checked", "неизвестно")
                        if isinstance(last_checked, str) and last_checked != "неизвестно":
                            try:
                                last_checked_dt = datetime.datetime.fromisoformat(last_checked)
                                last_checked = last_checked_dt.strftime('%Y-%m-%d %H:%M:%S')
                            except:
                                pass
                        f.write(f"{key}  # Последняя проверка: {last_checked}\n")
                        
                logging.info(f"Сохранено {len(self.quota_exceeded_keys)} API ключей с исчерпанной квотой")
            
            # Сохраняем недействительные ключи
            if self.invalid_keys:
                with open('Invalid_API.txt', 'w', encoding='utf-8') as f:
                    f.write("# Недействительные API ключи\n")
                    f.write(f"# Проверены: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    
                    for key in self.invalid_keys:
                        # Добавляем комментарий с информацией об ошибке
                        error_info = self.quota_database.get(key, {}).get("error_message", "недействительный ключ")
                        f.write(f"{key}  # Ошибка: {error_info}\n")
                        
                logging.info(f"Сохранено {len(self.invalid_keys)} недействительных API ключей")
            
        except Exception as e:
            logging.error(f"Ошибка при сохранении категоризированных ключей: {str(e)}")
    
    def validate_keys_parallel(self):
        """
        Проверка API ключей параллельно с использованием ThreadPoolExecutor.
        
        :return: True если найдены валидные ключи, иначе False
        """
        self.processed_keys = 0
        self.valid_keys = []
        self.invalid_keys = []
        self.quota_exceeded_keys = []
        
        # Сброс флага отмены и установка события незавершенной валидации
        self.cancel_validation = False
        self.validation_complete_event.clear()
        
        # Запуск мониторинга системы
        self.system_monitor.start_monitoring()
        
        # Определение оптимального количества потоков на основе количества ключей
        workers = min(self.max_workers, max(1, len(self.api_keys)))
        
        logging.info(f"Запуск параллельной проверки {len(self.api_keys)} API ключей с {workers} потоками...")
        
        start_time = time.time()
        
        # Используем ThreadPoolExecutor для параллельной проверки
        with ThreadPoolExecutor(max_workers=workers) as executor:
            try:
                # Создаем словарь future -> api_key для отслеживания
                future_to_key = {}
                
                # Запускаем задачи с ограничением скорости
                for i, api_key in enumerate(self.api_keys):
                    if self.cancel_validation:
                        logging.info("Проверка отменена пользователем.")
                        break
                    
                    # Добавляем небольшую задержку каждые несколько ключей для избежания rate limiting
                    if i > 0 and i % workers == 0:
                        time.sleep(self.delay_between)
                    
                    future = executor.submit(self.validate_api_key, api_key)
                    future_to_key[future] = api_key
                
                # Обрабатываем результаты по мере их поступления
                for future in as_completed(future_to_key):
                    if self.cancel_validation:
                        # Отменяем все оставшиеся задачи
                        for f in future_to_key:
                            if not f.done():
                                try:
                                    f.cancel()
                                except:
                                    pass
                        break
                    
                    api_key = future_to_key[future]
                    try:
                        is_valid, error_type, quota_info = future.result()
                        
                        # Сортируем ключи по результатам
                        if is_valid:
                            self.valid_keys.append(api_key)
                        elif error_type == "quota_exceeded":
                            self.quota_exceeded_keys.append(api_key)
                        else:
                            self.invalid_keys.append(api_key)
                        
                        # Обновляем прогресс
                        self.processed_keys += 1
                        if self.progress_callback:
                            progress_percent = (self.processed_keys / self.total_keys) * 100
                            self.progress_callback(progress_percent, api_key, is_valid, error_type, quota_info)
                            
                    except Exception as e:
                        logging.error(f"Ошибка при обработке результата для ключа {api_key[:5]}...{api_key[-5:]}: {str(e)}")
                        logging.debug(traceback.format_exc())
                        
                        # Обновляем прогресс даже в случае ошибки
                        self.processed_keys += 1
                        if self.progress_callback:
                            progress_percent = (self.processed_keys / self.total_keys) * 100
                            self.progress_callback(progress_percent, api_key, False, "error", {"error": str(e)})
            
            except Exception as e:
                logging.error(f"Ошибка в параллельной проверке: {str(e)}")
                logging.debug(traceback.format_exc())
            
            finally:
                # Гарантированно отмечаем завершение валидации
                self.validation_complete_event.set()
                
                # Останавливаем мониторинг системы
                self.system_monitor.stop_monitoring()
                
                # Записываем статистику выполнения
                self.analytics_data["last_validation_stats"] = {
                    "duration_seconds": time.time() - start_time,
                    "keys_total": len(self.api_keys),
                    "keys_processed": self.processed_keys,
                    "keys_valid": len(self.valid_keys),
                    "keys_invalid": len(self.invalid_keys),
                    "keys_quota_exceeded": len(self.quota_exceeded_keys),
                    "system_stats": self.system_monitor.get_stats_summary()
                }
        
        return len(self.valid_keys) > 0
    
    def validate_all_keys(self, use_parallel=True):
        """
        Проверка всех API ключей.
        
        :param use_parallel: Использовать ли параллельную проверку
        :return: True если найдены валидные ключи, иначе False
        """
        if not self.load_api_keys():
            return False
        
        # Сброс состояния
        self.valid_keys = []
        self.invalid_keys = []
        self.quota_exceeded_keys = []
        
        logging.info("Начинаем проверку API ключей...")
        
        start_time = time.time()
        
        try:
            # Выбираем режим проверки: параллельный или последовательный
            if use_parallel and len(self.api_keys) > 1:
                result = self.validate_keys_parallel()
            else:
                # Пометка начала последовательной валидации
                self.validation_complete_event.clear()
                
                # Запуск мониторинга системы
                self.system_monitor.start_monitoring()
                
                try:
                    # Последовательная проверка
                    for i, api_key in enumerate(self.api_keys):
                        if self.cancel_validation:
                            logging.info("Проверка отменена пользователем.")
                            break
                        
                        logging.info(f"Проверка ключа {i+1}/{len(self.api_keys)}: {api_key[:5]}...{api_key[-5:]}")
                        is_valid, error_type, quota_info = self.validate_api_key(api_key)
                        
                        if is_valid:
                            self.valid_keys.append(api_key)
                        elif error_type == "quota_exceeded":
                            self.quota_exceeded_keys.append(api_key)
                        else:
                            self.invalid_keys.append(api_key)
                        
                        # Обновляем прогресс
                        self.processed_keys = i + 1
                        if self.progress_callback:
                            progress_percent = (self.processed_keys / self.total_keys) * 100
                            self.progress_callback(progress_percent, api_key, is_valid, error_type, quota_info)
                            
                        # Задержка между запросами для предотвращения превышения лимита скорости
                        if i < len(self.api_keys) - 1 and not self.cancel_validation:
                            time.sleep(self.delay_between)
                    
                    result = len(self.valid_keys) > 0
                
                except Exception as e:
                    logging.error(f"Ошибка при последовательной проверке ключей: {str(e)}")
                    logging.debug(traceback.format_exc())
                    result = False
                
                finally:
                    # Отмечаем завершение валидации
                    self.validation_complete_event.set()
                    
                    # Останавливаем мониторинг системы
                    self.system_monitor.stop_monitoring()
                    
                    # Записываем статистику выполнения
                    self.analytics_data["last_validation_stats"] = {
                        "duration_seconds": time.time() - start_time,
                        "keys_total": len(self.api_keys),
                        "keys_processed": self.processed_keys,
                        "keys_valid": len(self.valid_keys),
                        "keys_invalid": len(self.invalid_keys),
                        "keys_quota_exceeded": len(self.quota_exceeded_keys),
                        "system_stats": self.system_monitor.get_stats_summary()
                    }
            
            # Сохранение валидных ключей, если они есть
            if self.valid_keys:
                self.save_valid_keys()
            
            # Сохраняем статистику о квотах и использовании
            self.save_quota_database()
            self.save_analytics_data()
            
            # Вывод статистики
            self._print_validation_statistics()
            
            # Сохраняем статистику в JSON для программного использования
            self._save_validation_statistics()
            
            return result
            
        except Exception as e:
            logging.error(f"Неожиданная ошибка при проверке ключей: {str(e)}")
            logging.debug(traceback.format_exc())
            
            # Пытаемся сохранить результаты даже в случае ошибки
            if self.valid_keys:
                self.save_valid_keys()
            
            # Отмечаем завершение валидации в любом случае
            self.validation_complete_event.set()
            
            return len(self.valid_keys) > 0
    
    def _print_validation_statistics(self):
        """Вывод статистики проверки API ключей."""
        # Вывод общей статистики
        logging.info("=== Результаты проверки API ключей ===")
        logging.info(f"Всего проверено: {self.processed_keys} из {len(self.api_keys)} ключей")
        logging.info(f"Валидные ключи: {len(self.valid_keys)}")
        logging.info(f"Невалидные ключи: {len(self.invalid_keys)}")
        logging.info(f"Ключи с превышенной квотой: {len(self.quota_exceeded_keys)}")
        
        # Если есть валидные ключи, показать информацию о доступной квоте
        if self.valid_keys:
            total_quota = sum(self.get_estimated_remaining_quota(key) for key in self.valid_keys)
            logging.info(f"Общая доступная квота: ~{total_quota} единиц")
            
            # Показать топ-3 ключа с наибольшей оставшейся квотой
            sorted_keys = sorted(self.valid_keys, key=lambda k: self.get_estimated_remaining_quota(k), reverse=True)
            logging.info("Топ ключи по доступной квоте:")
            for i, key in enumerate(sorted_keys[:3]):
                quota = self.get_estimated_remaining_quota(key)
                masked_key = key[:5] + "*" * (len(key) - 10) + key[-5:]
                logging.info(f"  {i+1}. {masked_key}: ~{quota} единиц")
        
        # Информация о времени выполнения
        duration = self.analytics_data.get("last_validation_stats", {}).get("duration_seconds", 0)
        if duration > 0:
            logging.info(f"Время выполнения: {duration:.2f} секунд")
    
    def _save_validation_statistics(self):
        """Сохранение статистики проверки в отдельный JSON файл."""
        try:
            stats = {
                "timestamp": datetime.datetime.now().isoformat(),
                "total": len(self.api_keys),
                "processed": self.processed_keys,
                "valid": len(self.valid_keys),
                "invalid": len(self.invalid_keys),
                "quota_exceeded": len(self.quota_exceeded_keys),
                "duration_seconds": self.analytics_data.get("last_validation_stats", {}).get("duration_seconds", 0),
                "system_stats": self.system_monitor.get_stats_summary(),
                "quota_info": {key: self.quota_database.get(key, {}) for key in self.valid_keys}
            }
            
            with open("api_validation_stats.json", 'w', encoding='utf-8') as f:
                json.dump(stats, f, indent=4)
                
            logging.info("Статистика валидации сохранена в api_validation_stats.json")
            
        except Exception as e:
            logging.error(f"Ошибка при сохранении статистики валидации: {str(e)}")
    
    def pause_validation(self):
        """Приостановить процесс валидации."""
        if not self.pause_event.is_set():
            # Уже приостановлено
            return
            
        logging.info("Приостановка процесса валидации...")
        self.pause_event.clear()
    
    def resume_validation(self):
        """Возобновить процесс валидации."""
        if self.pause_event.is_set():
            # Уже запущено
            return
            
        logging.info("Возобновление процесса валидации...")
        self.pause_event.set()
    
    def get_quota_info(self, api_key):
        """
        Получить информацию о квоте для конкретного ключа.
        
        :param api_key: Ключ API
        :return: Информация о квоте или None, если информация отсутствует
        """
        if api_key in self.quota_database:
            return self.quota_database[api_key]
        return None
    
    def update_quota_info(self, api_key, quota_info):
        """
        Обновить информацию о квоте для ключа API.
        
        :param api_key: Ключ API
        :param quota_info: Новая информация о квоте
        """
        if api_key not in self.quota_database:
            self.quota_database[api_key] = {
                "history": [],
                "usage_count": 0,
                "first_used": datetime.datetime.now().isoformat()
            }
        
        # Обновляем текущую информацию о ключе
        self.quota_database[api_key].update(quota_info)
        
        # Добавляем запись в историю
        history_entry = {
            "timestamp": datetime.datetime.now().isoformat(),
            "status": quota_info.get("status", "unknown"),
            "units_used": quota_info.get("units_used", 0)
        }
        
        # Сохраняем дополнительную информацию, если это ошибка
        if "error_code" in quota_info:
            history_entry["error_code"] = quota_info["error_code"]
        if "error_reason" in quota_info:
            # Сохраняем только первые 100 символов причины ошибки, чтобы не раздувать файл
            history_entry["error_reason"] = quota_info["error_reason"][:100]
        
        # Увеличиваем счетчик использования
        self.quota_database[api_key]["usage_count"] = self.quota_database[api_key].get("usage_count", 0) + 1
        
        # Ограничиваем размер истории
        max_history_entries = 50  # Хранить не более 50 последних записей
        history = self.quota_database[api_key].get("history", [])
        history.append(history_entry)
        
        if len(history) > max_history_entries:
            history = history[-max_history_entries:]
        
        self.quota_database[api_key]["history"] = history
    
    def load_quota_database(self):
        """Загрузить базу данных квот из файла."""
        if os.path.exists(self.quota_database_file):
            try:
                with open(self.quota_database_file, 'r', encoding='utf-8') as f:
                    self.quota_database = json.load(f)
                logging.info(f"Загружена база данных квот с информацией о {len(self.quota_database)} ключах.")
            except Exception as e:
                logging.error(f"Ошибка загрузки базы данных квот: {str(e)}")
                logging.debug(traceback.format_exc())
                self.quota_database = {}
        else:
            logging.info("База данных квот не найдена. Будет создана новая.")
            self.quota_database = {}
    
    def save_quota_database(self):
        """Сохранить базу данных квот в файл."""
        try:
            with open(self.quota_database_file, 'w', encoding='utf-8') as f:
                json.dump(self.quota_database, f, indent=2)
            logging.info(f"База данных квот сохранена в {self.quota_database_file}")
        except Exception as e:
            logging.error(f"Ошибка сохранения базы данных квот: {str(e)}")
            logging.debug(traceback.format_exc())
    
    def get_used_quota(self, api_key):
        """
        Рассчитать примерное использование квоты для ключа на сегодня.
        
        :param api_key: Ключ API
        :return: Примерное количество использованных единиц квоты
        """
        if api_key not in self.quota_database:
            return 0
        
        # Получаем записи истории
        history = self.quota_database[api_key].get("history", [])
        
        # Фильтруем только записи за сегодня
        today = datetime.datetime.now().date()
        today_usage = 0
        
        for entry in history:
            try:
                entry_time = datetime.datetime.fromisoformat(entry.get("timestamp", "")).date()
                if entry_time == today:
                    today_usage += entry.get("units_used", 0)
            except (ValueError, TypeError):
                continue
        
        return today_usage
    
    def get_estimated_remaining_quota(self, api_key):
        """
        Получить примерную оставшуюся квоту для ключа.
        
        :param api_key: Ключ API
        :return: Примерное количество оставшихся единиц квоты
        """
        if api_key not in self.quota_database:
            return YT_API_QUOTA_LIMIT  # Предполагаем полную квоту для неизвестных ключей
        
        # Проверяем наличие информации о лимите и оставшейся квоте
        quota_info = self.quota_database[api_key]
        
        # Если есть прямое указание оставшейся квоты, используем его
        if "estimated_remaining" in quota_info:
            return quota_info["estimated_remaining"]
        
        # Иначе вычисляем на основе использованной квоты
        used_quota = self.get_used_quota(api_key)
        daily_limit = quota_info.get("estimated_daily_limit", YT_API_QUOTA_LIMIT)
        
        return max(0, daily_limit - used_quota)
    
    def get_daily_quota_usage(self):
        """
        Получить суммарное использование квоты по всем ключам по дням.
        
        :return: Словарь с датами и использованным количеством единиц квоты
        """
        daily_usage = {}
        
        for api_key, key_info in self.quota_database.items():
            for entry in key_info.get("history", []):
                try:
                    # Преобразуем timestamp в дату
                    entry_time = datetime.datetime.fromisoformat(entry.get("timestamp", ""))
                    date_str = entry_time.date().isoformat()
                    
                    # Добавляем использование квоты
                    if date_str not in daily_usage:
                        daily_usage[date_str] = 0
                    
                    daily_usage[date_str] += entry.get("units_used", 0)
                except (ValueError, TypeError):
                    continue
        
        return daily_usage
    
    def update_analytics(self, api_key, event_type, units_used):
        """
        Обновить аналитику использования API.
        
        :param api_key: Ключ API
        :param event_type: Тип события
        :param units_used: Количество использованных единиц квоты
        """
        # Получаем текущую дату и час
        now = datetime.datetime.now()
        date_str = now.date().isoformat()
        hour_str = f"{date_str}T{now.hour:02d}"
        
        # Обновляем данные по дням
        if date_str not in self.analytics_data["daily_usage"]:
            self.analytics_data["daily_usage"][date_str] = {
                "total_units": 0,
                "valid_requests": 0,
                "quota_exceeded": 0,
                "invalid_keys": 0,
                "errors": 0
            }
        
        # Обновляем данные по часам
        if hour_str not in self.analytics_data["hourly_usage"]:
            self.analytics_data["hourly_usage"][hour_str] = {
                "total_units": 0,
                "valid_requests": 0,
                "quota_exceeded": 0,
                "invalid_keys": 0,
                "errors": 0
            }
        
        # Увеличиваем счетчики
        self.analytics_data["daily_usage"][date_str]["total_units"] += units_used
        self.analytics_data["hourly_usage"][hour_str]["total_units"] += units_used
        
        # Увеличиваем счетчик по типу события
        if event_type == "valid_request":
            self.analytics_data["daily_usage"][date_str]["valid_requests"] += 1
            self.analytics_data["hourly_usage"][hour_str]["valid_requests"] += 1
        elif event_type == "quota_exceeded":
            self.analytics_data["daily_usage"][date_str]["quota_exceeded"] += 1
            self.analytics_data["hourly_usage"][hour_str]["quota_exceeded"] += 1
        elif event_type.startswith("invalid_key"):
            self.analytics_data["daily_usage"][date_str]["invalid_keys"] += 1
            self.analytics_data["hourly_usage"][hour_str]["invalid_keys"] += 1
        else:  # Прочие ошибки
            self.analytics_data["daily_usage"][date_str]["errors"] += 1
            self.analytics_data["hourly_usage"][hour_str]["errors"] += 1
        
        # Добавляем запись в историю квот
        quota_entry = {
            "timestamp": now.isoformat(),
            "key_prefix": api_key[:5] + "..." + api_key[-5:] if len(api_key) > 10 else api_key,
            "event_type": event_type,
            "units_used": units_used
        }
        
        # Ограничиваем размер истории квот
        self.analytics_data["quota_history"].append(quota_entry)
        if len(self.analytics_data["quota_history"]) > 1000:  # Хранить не более 1000 записей
            self.analytics_data["quota_history"] = self.analytics_data["quota_history"][-1000:]
    
    def load_analytics_data(self):
        """Загрузить данные аналитики из файла."""
        if os.path.exists(self.analytics_file):
            try:
                with open(self.analytics_file, 'r', encoding='utf-8') as f:
                    self.analytics_data = json.load(f)
                logging.info(f"Загружены данные аналитики API.")
            except Exception as e:
                logging.error(f"Ошибка загрузки данных аналитики: {str(e)}")
                logging.debug(traceback.format_exc())
                
                # Инициализируем данные аналитики, если их не удалось загрузить
                self.analytics_data = {
                    "daily_usage": {},
                    "hourly_usage": {},
                    "quota_history": [],
                    "system_stats": {}
                }
        else:
            logging.info("Файл аналитики не найден. Будет создан новый.")
            self.analytics_data = {
                "daily_usage": {},
                "hourly_usage": {},
                "quota_history": [],
                "system_stats": {}
            }
    
    def save_analytics_data(self):
        """Сохранить данные аналитики в файл."""
        try:
            # Очищаем устаревшие данные перед сохранением
            self._cleanup_old_analytics_data()
            
            # Добавляем данные о системе
            self.analytics_data["system_stats"] = {
                "last_updated": datetime.datetime.now().isoformat(),
                "platform": platform.platform(),
                "python_version": platform.python_version(),
                **self.system_monitor.get_stats_summary()
            }
            
            with open(self.analytics_file, 'w', encoding='utf-8') as f:
                json.dump(self.analytics_data, f, indent=2)
            logging.info(f"Данные аналитики сохранены в {self.analytics_file}")
        except Exception as e:
            logging.error(f"Ошибка сохранения данных аналитики: {str(e)}")
            logging.debug(traceback.format_exc())
    
    def _cleanup_old_analytics_data(self):
        """Очистить устаревшие данные аналитики (старше 30 дней)."""
        # Рассчитываем дату, раньше которой данные считаются устаревшими
        cutoff_date = (datetime.datetime.now() - datetime.timedelta(days=30)).date().isoformat()
        
        # Очищаем данные по дням
        daily_usage = {}
        for date_str, data in self.analytics_data["daily_usage"].items():
            if date_str >= cutoff_date:
                daily_usage[date_str] = data
        self.analytics_data["daily_usage"] = daily_usage
        
        # Очищаем данные по часам (оставляем только последние 7 дней)
        cutoff_date_hourly = (datetime.datetime.now() - datetime.timedelta(days=7)).date().isoformat()
        hourly_usage = {}
        for hour_str, data in self.analytics_data["hourly_usage"].items():
            date_part = hour_str.split("T")[0]  # Извлекаем дату из формата "YYYY-MM-DDThh"
            if date_part >= cutoff_date_hourly:
                hourly_usage[hour_str] = data
        self.analytics_data["hourly_usage"] = hourly_usage
        
        # Очищаем историю квот (оставляем только последние 1000 записей)
        if len(self.analytics_data["quota_history"]) > 1000:
            self.analytics_data["quota_history"] = self.analytics_data["quota_history"][-1000:]
    
    def analyze_usage_trends(self):
        """
        Анализировать тренды использования API и делать прогнозы.
        
        :return: Словарь с результатами анализа
        """
        # Получаем данные использования за последние 7 дней
        now = datetime.datetime.now()
        
        # Создаем список последних 7 дней
        last_7_days = []
        for i in range(7):
            date_str = (now - datetime.timedelta(days=i)).date().isoformat()
            last_7_days.append(date_str)
        
        # Собираем данные об использовании квоты за эти дни
        daily_units = []
        daily_requests = []
        for date_str in reversed(last_7_days):  # От самого раннего к последнему
            if date_str in self.analytics_data["daily_usage"]:
                data = self.analytics_data["daily_usage"][date_str]
                daily_units.append(data.get("total_units", 0))
                daily_requests.append(data.get("valid_requests", 0) + data.get("quota_exceeded", 0) + 
                                      data.get("invalid_keys", 0) + data.get("errors", 0))
            else:
                daily_units.append(0)
                daily_requests.append(0)
        
        # Рассчитываем средние значения
        avg_daily_units = sum(daily_units) / len(daily_units) if daily_units else 0
        avg_daily_requests = sum(daily_requests) / len(daily_requests) if daily_requests else 0
        
        # Рассчитываем тренд (простая линейная регрессия)
        trend = 0
        if len(daily_units) > 1:
            # Для упрощения используем простой расчет наклона
            first_half = sum(daily_units[:len(daily_units)//2]) / (len(daily_units)//2) if len(daily_units)//2 > 0 else 0
            second_half = sum(daily_units[len(daily_units)//2:]) / (len(daily_units) - len(daily_units)//2) if (len(daily_units) - len(daily_units)//2) > 0 else 0
            trend = second_half - first_half
        
        # Прогноз на следующий день
        forecast = avg_daily_units + trend
        
        # Прогноз количества дней до достижения дневного лимита
        days_to_limit = None
        daily_limit = YT_API_QUOTA_LIMIT  # Стандартный дневной лимит YouTube API
        if avg_daily_units > 0:
            days_to_limit = daily_limit / avg_daily_units
        
        # Расчет среднего времени обработки ключа
        avg_processing_time = None
        last_validation = self.analytics_data.get("last_validation_stats", {})
        if last_validation:
            duration = last_validation.get("duration_seconds", 0)
            processed = last_validation.get("keys_processed", 0)
            if duration > 0 and processed > 0:
                avg_processing_time = duration / processed
        
        # Формируем результат анализа
        return {
            "analysis_time": now.isoformat(),
            "period": {
                "start": last_7_days[0],
                "end": last_7_days[-1]
            },
            "averages": {
                "daily_units": avg_daily_units,
                "daily_requests": avg_daily_requests,
                "key_processing_time": avg_processing_time
            },
            "trend_direction": "increasing" if trend > 0 else "decreasing" if trend < 0 else "stable",
            "trend_value": trend,
            "forecast": {
                "next_day_units": forecast,
                "days_to_quota_limit": days_to_limit
            },
            "system_stats": self.system_monitor.get_stats_summary(),
            "recommendations": self._generate_recommendations(avg_daily_units, trend, last_validation)
        }
    
    def _generate_recommendations(self, avg_usage, trend, last_validation):
        """
        Сгенерировать рекомендации на основе данных использования.
        
        :param avg_usage: Среднее использование квоты в день
        :param trend: Тренд использования
        :param last_validation: Информация о последней валидации
        :return: Список рекомендаций
        """
        recommendations = []
        
        # Анализ использования квоты
        daily_limit = YT_API_QUOTA_LIMIT
        
        if avg_usage > 0:
            # Процент использования от дневного лимита
            usage_percent = (avg_usage / daily_limit) * 100
            
            if usage_percent > 80:
                recommendations.append("Высокое использование квоты (>80%). Рекомендуется добавить дополнительные API ключи.")
            elif usage_percent > 50:
                recommendations.append(f"Среднее использование квоты ({usage_percent:.1f}%). Рекомендуется мониторить использование.")
            
            # Анализ тренда
            if trend > 0 and trend > avg_usage * 0.1:  # Рост более 10% в день
                recommendations.append("Значительный рост использования квоты. Рекомендуется оптимизировать запросы или добавить ключи.")
        
        # Анализ валидации
        valid_keys = last_validation.get("keys_valid", 0)
        total_keys = last_validation.get("keys_total", 0)
        
        if total_keys > 0:
            valid_ratio = valid_keys / total_keys
            
            if valid_ratio < 0.5:
                recommendations.append(f"Низкий процент валидных ключей ({valid_ratio*100:.1f}%). Рекомендуется пересмотреть источник ключей.")
            
            if valid_keys == 0:
                recommendations.append("Не найдено валидных ключей. Срочно требуется добавить действующие ключи API.")
        
        # Добавляем общие рекомендации, если список пуст
        if not recommendations:
            recommendations.append("Использование API в пределах нормы. Специальных рекомендаций нет.")
        
        return recommendations
    
    def get_optimal_key(self):
        """
        Получить оптимальный ключ API с учетом оставшейся квоты и производительности.
        
        :return: Оптимальный ключ API или None, если нет подходящих ключей
        """
        valid_keys = []
        
        # Собираем все ключи с статусом "valid"
        for api_key, key_info in self.quota_database.items():
            if key_info.get("status") == "valid":
                # Рассчитываем примерную оставшуюся квоту
                estimated_remaining = self.get_estimated_remaining_quota(api_key)
                
                # Пропускаем ключи с малой оставшейся квотой
                if estimated_remaining < 100:
                    continue
                
                # Получаем среднее время отклика из истории
                response_times = []
                for entry in key_info.get("history", []):
                    if "response_time" in entry:
                        response_times.append(entry["response_time"])
                avg_response_time = sum(response_times) / len(response_times) if response_times else 1.0
                
                valid_keys.append({
                    "key": api_key,
                    "estimated_remaining": estimated_remaining,
                    "avg_response_time": avg_response_time,
                    "usage_count": key_info.get("usage_count", 0)
                })
        
        if not valid_keys:
            # Проверяем ключи с превышенной квотой на случай, если прошло достаточно времени
            now = datetime.datetime.now()
            today = now.date()
            
            for api_key, key_info in self.quota_database.items():
                if key_info.get("status") == "quota_exceeded":
                    # Проверяем, когда была последняя проверка
                    last_checked = key_info.get("last_checked")
                    if last_checked:
                        try:
                            last_date = datetime.datetime.fromisoformat(last_checked).date()
                            # Если проверка была не сегодня, возможно квота сброшена
                            if last_date < today:
                                valid_keys.append({
                                    "key": api_key,
                                    "estimated_remaining": YT_API_QUOTA_LIMIT,  # Предполагаем полный сброс
                                    "avg_response_time": 1.0,  # Нет данных о производительности
                                    "usage_count": key_info.get("usage_count", 0)
                                })
                        except (ValueError, TypeError):
                            pass
        
        if not valid_keys:
            return None
        
        # Сортируем ключи сначала по квоте, затем по времени отклика, затем по количеству использований
        sorted_keys = sorted(valid_keys, key=lambda k: (-k["estimated_remaining"], k["avg_response_time"], k["usage_count"]))
        
        # Возвращаем лучший ключ
        return sorted_keys[0]["key"]
    
    def stop_validation(self):
        """Остановить процесс валидации."""
        self.cancel_validation = True
        logging.info("Запрошена остановка валидации API ключей.")
        
    def export_to_csv(self, filename="api_keys_export.csv"):
        """
        Экспортировать информацию о ключах API в CSV файл.
        
        :param filename: Имя файла для экспорта
        :return: True если экспорт успешен, иначе False
        """
        try:
            # Подготовка данных для экспорта
            export_data = []
            
            # Сначала валидные ключи
            for key in self.valid_keys:
                info = self.quota_database.get(key, {})
                
                export_data.append({
                    "key": key,
                    "status": "valid",
                    "remaining_quota": self.get_estimated_remaining_quota(key),
                    "last_checked": info.get("last_checked", ""),
                    "response_time": info.get("response_time", ""),
                    "usage_count": info.get("usage_count", 0)
                })
            
            # Затем ключи с превышенной квотой
            for key in self.quota_exceeded_keys:
                info = self.quota_database.get(key, {})
                
                export_data.append({
                    "key": key,
                    "status": "quota_exceeded",
                    "remaining_quota": 0,
                    "last_checked": info.get("last_checked", ""),
                    "error_code": info.get("error_code", ""),
                    "usage_count": info.get("usage_count", 0)
                })
            
            # Затем недействительные ключи
            for key in self.invalid_keys:
                info = self.quota_database.get(key, {})
                
                export_data.append({
                    "key": key,
                    "status": "invalid",
                    "error_subtype": info.get("error_subtype", ""),
                    "error_message": info.get("error_message", ""),
                    "last_checked": info.get("last_checked", ""),
                    "usage_count": info.get("usage_count", 0)
                })
            
            # Запись в CSV
            if not export_data:
                logging.warning("Нет данных для экспорта в CSV.")
                return False
            
            with open(filename, 'w', encoding='utf-8', newline='') as csvfile:
                # Определение заголовков по всем возможным ключам
                all_keys = set()
                for item in export_data:
                    all_keys.update(item.keys())
                
                fieldnames = sorted(list(all_keys))
                
                # Импортируем csv только здесь, чтобы не загружать модуль, если он не используется
                import csv
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                writer.writerows(export_data)
            
            logging.info(f"Данные о {len(export_data)} ключах экспортированы в {filename}")
            return True
            
        except Exception as e:
            logging.error(f"Ошибка при экспорте в CSV: {str(e)}")
            logging.debug(traceback.format_exc())
            return False
    
    def import_from_csv(self, filename, append=True):
        """
        Импортировать ключи API из CSV файла.
        
        :param filename: Имя файла для импорта
        :param append: Добавлять к существующим ключам (True) или заменить их (False)
        :return: (success, count) - успешность операции и количество импортированных ключей
        """
        try:
            # Проверка существования файла
            if not os.path.exists(filename):
                logging.error(f"Файл {filename} не существует.")
                return False, 0
            
            # Импортируем csv только здесь, чтобы не загружать модуль, если он не используется
            import csv
            
            imported_keys = []
            
            with open(filename, 'r', encoding='utf-8', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                
                for row in reader:
                    if "key" in row and row["key"]:
                        imported_keys.append(row["key"])
            
            if not imported_keys:
                logging.warning(f"В файле {filename} не найдено ключей API.")
                return False, 0
            
            # Обновляем список ключей
            if append:
                # Добавляем только новые ключи
                existing_keys = set(self.api_keys)
                new_keys = [key for key in imported_keys if key not in existing_keys]
                self.api_keys.extend(new_keys)
                self.total_keys = len(self.api_keys)
                
                logging.info(f"Добавлено {len(new_keys)} новых ключей из {len(imported_keys)} импортированных.")
                return True, len(new_keys)
            else:
                # Заменяем все ключи
                self.api_keys = imported_keys
                self.total_keys = len(self.api_keys)
                
                logging.info(f"Загружено {len(imported_keys)} ключей. Предыдущие ключи заменены.")
                return True, len(imported_keys)
            
        except Exception as e:
            logging.error(f"Ошибка при импорте из CSV: {str(e)}")
            logging.debug(traceback.format_exc())
            return False, 0

def validate_api_keys(api_file='api.txt', output_file='Good_API.txt', 
                     max_workers=None, delay_between=1.0, progress_callback=None):
    """
    Функция для проверки API ключей, которую можно вызывать из других модулей.
    
    :param api_file: Путь к файлу с ключами API
    :param output_file: Путь к файлу для сохранения валидных ключей
    :param max_workers: Максимальное количество рабочих потоков (None=автоопределение)
    :param delay_between: Задержка между запросами в секундах
    :param progress_callback: Функция для отслеживания прогресса
    :return: True если найдены валидные ключи, иначе False
    """
    validator = YouTubeAPIValidator(api_file, output_file, max_workers, delay_between)
    
    # Установка callback для отслеживания прогресса
    validator.progress_callback = progress_callback
    
    # Запуск проверки в зависимости от количества ключей
    use_parallel = len(validator.api_keys) > 1 if validator.load_api_keys() else False
    return validator.validate_all_keys(use_parallel)

def analyze_api_usage_trends(analytics_file="api_usage_analytics.json"):
    """
    Анализировать тренды использования API и делать прогнозы.
    
    :param analytics_file: Путь к файлу аналитики
    :return: Результаты анализа
    """
    validator = YouTubeAPIValidator()
    validator.analytics_file = analytics_file
    validator.load_analytics_data()
    return validator.analyze_usage_trends()

def get_optimal_api_key(quota_database_file="api_keys_quota.json"):
    """
    Получить оптимальный ключ API с учетом оставшейся квоты и производительности.
    
    :param quota_database_file: Путь к файлу базы данных квот
    :return: Оптимальный ключ API или None, если нет подходящих ключей
    """
    validator = YouTubeAPIValidator()
    validator.quota_database_file = quota_database_file
    validator.load_quota_database()
    return validator.get_optimal_key()

def check_api_quota(api_key):
    """
    Быстрая проверка доступной квоты для конкретного ключа API.
    
    :param api_key: Ключ API для проверки
    :return: (is_valid, remaining_quota, error_message) - статус ключа, оставшаяся квота и сообщение об ошибке
    """
    validator = YouTubeAPIValidator()
    validator.load_quota_database()
    
    # Проверка ключа
    is_valid, error_type, quota_info = validator.validate_api_key(api_key)
    
    if is_valid:
        remaining_quota = validator.get_estimated_remaining_quota(api_key)
        return True, remaining_quota, None
    else:
        error_message = quota_info.get("error_message", error_type)
        return False, 0, error_message

if __name__ == "__main__":
    # Если скрипт запущен напрямую, выполняем проверку
    import argparse
    
    parser = argparse.ArgumentParser(description="Валидатор ключей YouTube API")
    parser.add_argument("--input", "-i", default="api.txt", help="Входной файл с API ключами")
    parser.add_argument("--output", "-o", default="Good_API.txt", help="Выходной файл для валидных ключей")
    parser.add_argument("--threads", "-t", type=int, default=None, help="Количество потоков (по умолчанию: авто)")
    parser.add_argument("--delay", "-d", type=float, default=1.0, help="Задержка между запросами (секунды)")
    parser.add_argument("--sequential", "-s", action="store_true", help="Использовать последовательную проверку")
    parser.add_argument("--export", "-e", help="Экспортировать результаты в CSV файл")
    parser.add_argument("--analyze", "-a", action="store_true", help="Показать аналитику использования API")
    parser.add_argument("--check", "-c", help="Проверить конкретный ключ API")
    
    args = parser.parse_args()
    
    # Если указан конкретный ключ для проверки
    if args.check:
        valid, quota, error = check_api_quota(args.check)
        if valid:
            print(f"API ключ действителен. Оставшаяся квота: ~{quota} единиц.")
        else:
            print(f"API ключ недействителен: {error}")
        sys.exit(0)
    
    # Если запрошен анализ использования
    if args.analyze:
        trends = analyze_api_usage_trends()
        
        print("=== Анализ трендов использования API ===")
        print(f"Период анализа: с {trends['period']['start']} по {trends['period']['end']}")
        print(f"Среднее дневное использование: {trends['averages']['daily_units']:.2f} единиц")
        print(f"Тренд: {trends['trend_direction']} ({trends['trend_value']:.2f})")
        print(f"Прогноз на следующий день: {trends['forecast']['next_day_units']:.2f} единиц")
        
        if trends['forecast']['days_to_quota_limit'] is not None:
            print(f"Дней до исчерпания квоты: {trends['forecast']['days_to_quota_limit']:.1f}")
        else:
            print("Недостаточно данных для прогноза исчерпания квоты")
            
        print("\nРекомендации:")
        for rec in trends['recommendations']:
            print(f"- {rec}")
            
        sys.exit(0)
    
    # Основной режим - валидация ключей
    validator = YouTubeAPIValidator(
        api_file=args.input, 
        output_file=args.output,
        max_workers=args.threads,
        delay_between=args.delay
    )
    
    # Настройка отображения прогресса в консоли
    def console_progress(percent, api_key, is_valid, error_type, quota_info):
        status = "✓" if is_valid else "✗"
        masked_key = api_key[:5] + "..." + api_key[-5:] if len(api_key) > 10 else api_key
        
        # Определение статуса
        if is_valid:
            status_text = "валиден"
        elif error_type == "quota_exceeded":
            status_text = "квота исчерпана"
        else:
            # Более подробная информация об ошибке
            error_message = quota_info.get("error_message", "недействителен")
            status_text = f"недействителен: {error_message}"
        
        print(f"\r[{percent:.1f}%] Ключ {masked_key}: {status_text} {status}", end="")
        
        # Добавляем перевод строки после каждого пятого ключа для лучшей читаемости
        if validator.processed_keys % 5 == 0:
            print()
    
    validator.progress_callback = console_progress
    
    print(f"Запуск проверки ключей API из файла {args.input}...")
    result = validator.validate_all_keys(use_parallel=not args.sequential)
    
    # Добавляем перевод строки после завершения
    print("\n")
    
    # Экспорт результатов в CSV при необходимости
    if args.export:
        validator.export_to_csv(args.export)
    
    if result:
        print(f"Проверка завершена. Найдено {len(validator.valid_keys)} валидных ключей.")
        sys.exit(0)
    else:
        print("Проверка завершена. Не найдено валидных ключей.")
        sys.exit(1)