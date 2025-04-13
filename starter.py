"""
Запускающий файл для приложения парсера YouTube каналов.
Настроен на русский язык интерфейса по умолчанию.
Содержит улучшенные настройки кодировки для предотвращения проблем с Unicode.
"""

import os
import sys
import io
import codecs
import locale
import time
import traceback
import logging
from datetime import datetime
import tkinter as tk
import tkinter.messagebox as msgbox

# ===== НАСТРОЙКА КОДИРОВОК ДЛЯ ПРЕДОТВРАЩЕНИЯ UNICODE ОШИБОК =====

# Создаем утилиты для общих операций
class FileUtils:
    """Утилиты для файловых операций с правильной обработкой кодировок."""
    
    @staticmethod
    def safe_open(file_path, mode='r', encoding='utf-8', errors='backslashreplace'):
        """Безопасное открытие файла с обработкой ошибок кодировки."""
        try:
            return open(file_path, mode, encoding=encoding, errors=errors)
        except UnicodeDecodeError as e:
            logging.warning(f"Ошибка декодирования при открытии {file_path}: {e}")
            # Попытка определить кодировку файла
            try:
                import chardet
                with open(file_path, 'rb') as binary_file:
                    result = chardet.detect(binary_file.read())
                    detected_encoding = result['encoding'] or 'utf-8'
                logging.info(f"Обнаружена кодировка {detected_encoding} для файла {file_path}")
                return open(file_path, mode, encoding=detected_encoding, errors='backslashreplace')
            except ImportError:
                logging.warning("Модуль chardet не установлен. Используем системную кодировку.")
                system_encoding = locale.getpreferredencoding()
                return open(file_path, mode, encoding=system_encoding, errors='backslashreplace')
            except Exception as e:
                logging.error(f"Не удалось определить кодировку: {e}")
                # Крайний случай - попытка использовать системную кодировку
                system_encoding = locale.getpreferredencoding()
                return open(file_path, mode, encoding=system_encoding, errors='backslashreplace')
    
    @staticmethod
    def ensure_directory(dir_path):
        """Создать директорию, если она не существует."""
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
            logging.info(f"Создана директория: {dir_path}")
    
    @staticmethod
    def create_file_with_content(file_path, content, encoding='utf-8'):
        """Создать файл с указанным содержимым."""
        try:
            with open(file_path, 'w', encoding=encoding) as f:
                f.write(content)
            logging.info(f"Создан файл: {file_path}")
            return True
        except Exception as e:
            logging.error(f"Ошибка при создании файла {file_path}: {e}")
            # Попытка создать файл с другой кодировкой
            try:
                with open(file_path, 'w', encoding=locale.getpreferredencoding()) as f:
                    f.write(content)
                logging.warning(f"Файл {file_path} создан с системной кодировкой")
                return True
            except Exception as e2:
                logging.error(f"Критическая ошибка при создании файла {file_path}: {e2}")
                return False

# Определение используемой системы и настройка кодировок
def setup_encoding():
    """Настройка кодировки для консоли и потоков ввода-вывода."""
    # Получаем текущую кодировку системы
    system_encoding = locale.getpreferredencoding()
    logging.info(f"Системная кодировка: {system_encoding}")
    
    # Настройка для Windows
    if sys.platform == 'win32':
        try:
            # Устанавливаем UTF-8 для консоли на Windows
            if sys.stdout.encoding != 'utf-8':
                sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='backslashreplace')
                sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='backslashreplace')
                logging.info("Кодировка консоли изменена на UTF-8")
            
            # Попытка установить консоль в режим UTF-8
            try:
                import ctypes
                kernel32 = ctypes.windll.kernel32
                kernel32.SetConsoleOutputCP(65001)  # 65001 - код для UTF-8
                kernel32.SetConsoleCP(65001)
                logging.info("Кодовая страница консоли установлена на UTF-8 (65001)")
            except Exception as e:
                logging.warning(f"Не удалось установить кодовую страницу консоли: {e}")
        except Exception as e:
            logging.warning(f"Ошибка при настройке кодировки для Windows: {e}")
            # Запасной вариант с заменой недопустимых символов
            sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'backslashreplace')
            sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'backslashreplace')
    else:
        # Для Linux, macOS и других Unix-подобных систем
        try:
            if sys.stdout.encoding != 'utf-8':
                sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='backslashreplace')
                sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='backslashreplace')
                logging.info("Кодировка консоли изменена на UTF-8")
        except Exception as e:
            logging.warning(f"Ошибка при настройке кодировки для Unix: {e}")

# Настройка базового логгирования с правильной кодировкой
def setup_logging():
    """Настройка логирования с корректной обработкой Unicode."""
    # Создаем директорию для логов если отсутствует
    FileUtils.ensure_directory('logs')
    
    # Формируем имя файла лога с текущей датой и временем
    log_filename = f"logs/scraper_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
    
    # Настраиваем логирование
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)  # Используем настроенный stdout
        ]
    )
    
    # Добавляем обработчик для отладочного журнала
    debug_handler = logging.FileHandler("debug.txt", encoding='utf-8')
    debug_handler.setLevel(logging.DEBUG)
    debug_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    debug_handler.setFormatter(debug_formatter)
    logging.getLogger().addHandler(debug_handler)
    
    return log_filename

def create_required_files():
    """Создание необходимых файлов, если они отсутствуют, с правильной кодировкой."""
    required_files = {
        'keywords.txt': "music\ngaming\ntutorial\ntech\nvlog\n",
        'proxy.txt': "# Format: ip:port:login:password\n",
        'settings.txt': ("min_subscribers=1000\nmax_subscribers=1000000\n"
                         "min_total_views=10000\ncreation_year_limit=2015\n"
                         "delay_min=0.5\ndelay_max=2\nparse_mode=email\n"
                         "max_workers=5\nbatch_size=50\nuse_caching=true\n"
                         "shorts_filter_mode=3\n"),
        'blacklist.txt': "IN\nBR\nPK\n",
        'api.txt': "# Enter your YouTube API keys here, one per line\n"
    }
    
    # Создание директорий для вывода данных (если нет)
    FileUtils.ensure_directory('logs')
    
    # Создание необходимых файлов с дефолтным содержимым
    for filename, default_content in required_files.items():
        if not os.path.exists(filename):
            FileUtils.create_file_with_content(filename, default_content)

def check_files_encoding():
    """Проверяет кодировку существующих файлов и исправляет при необходимости."""
    files_to_check = ['keywords.txt', 'proxy.txt', 'settings.txt', 'blacklist.txt', 
                     'api.txt', 'channels.txt', 'emails.txt', 'social_media.txt']
    
    for filename in files_to_check:
        if os.path.exists(filename):
            try:
                # Проверяем открытие файла в UTF-8
                with open(filename, 'r', encoding='utf-8') as f:
                    content = f.read()
                    # Если успешно прочитали файл, значит с кодировкой всё в порядке
                    continue
            except UnicodeDecodeError:
                logging.warning(f"Файл {filename} имеет некорректную кодировку. Попытка исправить...")
                
                try:
                    # Определяем текущую кодировку файла
                    import chardet
                    with open(filename, 'rb') as binary_file:
                        result = chardet.detect(binary_file.read(1024))
                        detected_encoding = result['encoding']
                    
                    # Если кодировка определена и отличается от UTF-8
                    if detected_encoding and detected_encoding.lower() != 'utf-8':
                        # Читаем содержимое в обнаруженной кодировке
                        with open(filename, 'r', encoding=detected_encoding) as f:
                            content = f.read()
                        
                        # Записываем в UTF-8
                        with open(filename + '.utf8', 'w', encoding='utf-8') as f:
                            f.write(content)
                        
                        # Заменяем старый файл новым
                        os.remove(filename)
                        os.rename(filename + '.utf8', filename)
                        logging.info(f"Файл {filename} конвертирован из {detected_encoding} в UTF-8")
                    else:
                        logging.warning(f"Не удалось определить кодировку файла {filename}")
                except ImportError:
                    logging.warning("Модуль chardet не установлен. Невозможно определить кодировку.")
                except Exception as e:
                    logging.error(f"Ошибка при конвертации кодировки файла {filename}: {e}")

def main():
    """Основная функция запуска приложения с обработкой ошибок."""
    # Устанавливаем кодировку консоли и потоков
    setup_encoding()
    
    # Настраиваем логирование
    log_file = setup_logging()
    logging.info(f"Запуск парсера YouTube каналов. Логи сохраняются в {log_file}")
    
    # Проверяем, что у нас есть файл debug.txt
    if not os.path.exists("debug.txt"):
        FileUtils.create_file_with_content(
            "debug.txt", 
            f"=== ЖУРНАЛ ОТЛАДКИ СОЗДАН {datetime.now()} ===\n\n"
        )
    
    # Создаем необходимые файлы конфигурации
    create_required_files()
    
    # Проверяем кодировку существующих файлов
    check_files_encoding()
    
    try:
        # Печатаем информацию о системе и кодировках
        logging.info(f"Платформа: {sys.platform}")
        logging.info(f"Версия Python: {sys.version}")
        logging.info(f"Кодировка локали: {locale.getpreferredencoding()}")
        logging.info(f"Кодировка stdin: {sys.stdin.encoding}")
        logging.info(f"Кодировка stdout: {sys.stdout.encoding}")
        logging.info(f"Кодировка по умолчанию: {sys.getdefaultencoding()}")
        logging.info(f"Кодировка файловой системы: {sys.getfilesystemencoding()}")
        
        # Импортируем класс YouTubeScraperGUI из youtube_scraper_gui
        from youtube_scraper_gui import YouTubeScraperGUI
        
        # Создаем и настраиваем корневое окно
        root = tk.Tk()
        root.title("Парсер YouTube Каналов")
        
        # Устанавливаем размер окна
        root.geometry("800x600")
        
        # Добавляем иконку (если доступна)
        try:
            if os.path.exists('logo.ico'):
                root.iconbitmap('logo.ico')
        except Exception:
            logging.warning("Не удалось установить иконку приложения")
        
        # Создаем приложение
        app = YouTubeScraperGUI(root)
        
        # Устанавливаем русский язык по умолчанию
        app.switch_language("ru")
        
        # Если есть файл Good_API.txt, используем его вместо api.txt
        if os.path.exists('Good_API.txt'):
            app.file_paths["api"] = 'Good_API.txt'
            app.check_files_status()  # Обновляем статус после изменения пути к файлу
        
        # Запускаем приложение
        logging.info("Запуск графического интерфейса")
        root.mainloop()
        
    except Exception as e:
        # Логгируем любые необработанные исключения
        error_message = f"Ошибка запуска приложения: {str(e)}"
        logging.error(error_message)
        
        with open("debug.txt", "a", encoding="utf-8") as f:
            f.write(f"\n=== ОШИБКА ЗАПУСКА {datetime.now()} ===\n")
            f.write(f"{error_message}\n")
            f.write(traceback.format_exc() + "\n\n")
        
        # Показываем ошибку в консоли
        print("Подробности смотрите в файле debug.txt")
        
        # Пробуем показать ошибку в графическом интерфейсе если возможно
        try:
            msgbox.showerror("Ошибка запуска приложения", 
                             f"Произошла ошибка при запуске приложения:\n{str(e)}\n\nПодробности смотрите в файле debug.txt.")
        except Exception:
            pass
        
        sys.exit(1)

if __name__ == "__main__":
    main()
