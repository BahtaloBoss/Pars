import os
import sys
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import threading
import logging
import traceback
from datetime import datetime
from api_validator import validate_api_keys
import json

# Import the scraper class from the original script
# Assuming the original script is saved as youtube_scraper.py
try:
    from youtube_scraper import YouTubeChannelScraper
except ImportError:
    # If running standalone, define a wrapper for the class
    class YouTubeChannelScraper:
        def __init__(self):
            self.api_keys = []
            self.keywords = []
            self.api_usage_count = {}
            self.parsed_emails = set()
            self.parsed_channels = set()
            self.parsed_social_media = set()
            self.processed_keywords = set()
        
        def initialize(self):
            return True
        
        def run(self):
            pass
        
        def stop(self):
            pass
            
        def remove_email_duplicates(self):
            pass

# Set up logging to file and debug.txt
def setup_logging():
    # Create logs directory if it doesn't exist
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # Set up regular logging
    log_file = f"logs/scraper_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Set up debug logging
    debug_handler = logging.FileHandler("debug.txt")
    debug_handler.setLevel(logging.DEBUG)
    debug_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    debug_handler.setFormatter(debug_formatter)
    
    # Get the root logger and add the debug handler
    root_logger = logging.getLogger()
    root_logger.addHandler(debug_handler)
    
    return root_logger

# Custom logger that also updates the GUI
class GUILogger:
    def __init__(self, text_widget):
        self.text_widget = text_widget
        self.logger = logging.getLogger()
    
    def log(self, level, message):
        # Log to regular logger
        if level == "INFO":
            self.logger.info(message)
        elif level == "ERROR":
            self.logger.error(message)
        elif level == "WARNING":
            self.logger.warning(message)
        elif level == "DEBUG":
            self.logger.debug(message)
        
        # Update GUI
        timestamp = datetime.now().strftime('%H:%M:%S')
        formatted_message = f"[{timestamp}] {level}: {message}\n"
        
        # Use different colors based on log level
        tag = level.lower()
        
        self.text_widget.configure(state='normal')
        self.text_widget.insert(tk.END, formatted_message, tag)
        self.text_widget.see(tk.END)
        self.text_widget.configure(state='disabled')

class YouTubeScraperGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("YouTube Channel Scraper")
        self.root.geometry("800x600")  # Smaller window size
        self.root.configure(bg="white")
        
        # Initialize scraper
        self.scraper = YouTubeChannelScraper()
        
        # Initialize logger
        self.logger = setup_logging()
        
        # Define colors
        self.colors = {
            "bg": "white",
            "success": "#4CAF50",  # Green
            "error": "#F44336",    # Red
            "button_bg": "#2196F3", # Blue for better visibility
            "button_active": "#1976D2", # Darker Blue
            "accent": "#FFC107",   # Amber for highlights
            "text": "#212121"      # Dark gray for text
        }
        
        # Track if scraper is running
        self.is_running = False
        
        # Language setting (en = English, ru = Russian)
        self.language = "en"
        self.translations = self.load_translations()
        
        # File paths
        self.file_paths = {
            "keywords": "keywords.txt",
            "proxy": "proxy.txt",
            "settings": "settings.txt",
            "blacklist": "blacklist.txt",
            "api": "api.txt",
            "channels": "channels.txt",
            "emails": "emails.txt",
            "emails_detailed": "emails_detailed.txt",
            "social_media": "social_media.txt"
        }
        
        # Create the GUI elements
        self.create_menu()
        self.create_main_frame()
        
        # Load existing files and check status
        self.check_files_status()
        
        # Display initial message
        self.gui_logger.log("INFO", self.get_translation("gui_started"))
        
        # Set up window close handler
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
    
    def load_translations(self):
        """Load translations for multilingual support"""
        translations = {
            "en": {
                # Menu items
                "file_menu": "File",
                "start_scraping": "Start Scraping",
                "stop_scraping": "Stop Scraping",
                "exit": "Exit",
                "settings_menu": "Settings",
                "configure_settings": "Configure Settings",
                "configure_file_paths": "Configure File Paths",
                "view_menu": "View",
                "view_keywords": "View Keywords",
                "view_proxies": "View Proxies",
                "view_blacklist": "View Blacklist",
                "view_api_keys": "View API Keys",
                "view_debug_log": "View Debug Log",
                "help_menu": "Help",
                "about": "About",
                "language_menu": "Language",
                "email_format_menu": "Email Format",
                "email_format_full": "Full (with channel info)",
                "email_format_email_only": "Email Only",
                "validate_api_keys": "Validate API Keys",
                "validate_api_btn": "Validate API",
                "validation_started": "Starting API key validation...",
                "validation_not_available": "Cannot validate API keys while scraper is running.",
                "validation_complete_title": "Validation Complete",
                "validation_error_title": "Validation Error",
                "validation_success": "Successfully validated {} API keys. Using validated keys for future operations.",
                "validation_no_valid_keys": "No valid API keys found. Please check your API keys and try again.",
                "validation_error": "Error during API validation: {}",
                "validating": "Validating...",
                "validated": "Validated",
                "validation_failed": "Failed",
                "remove_duplicates": "Remove Email Duplicates",
                
                # Buttons and labels
                "configure_settings_btn": "Settings",
                "configure_file_paths_btn": "File Paths",
                "view_results_btn": "View Results",
                
                # Status items
                "status_title": "Status",
                "api_keys_status": "API Keys",
                "keywords_status": "Keywords",
                "settings_status": "Settings",
                "running_status": "Running Status",
                "api_quota_status": "API Quota",
                
                # Statistics items
                "statistics_title": "Statistics",
                "channels_found": "Channels Found",
                "emails_found": "Emails Found",
                "social_media_found": "Social Media Found",
                "keywords_processed": "Keywords Processed",
                
                # Log titles
                "log_title": "Log",
                
                # Status values
                "not_loaded": "Not loaded",
                "loaded": "Loaded",
                "running": "Running",
                "stopped": "Stopped",
                "stopping": "Stopping...",
                "saved": "Saved",
                "error": "Error",
                
                # Messages
                "gui_started": "YouTube Channel Scraper GUI started. Ready to configure and run.",
                "already_running": "The scraper is already running.",
                "init_failed": "Failed to initialize the scraper. Please check the settings and API keys.",
                "scraping_started": "Started scraping process.",
                "scraping_complete": "Scraping completed successfully.",
                "scraping_stopping": "Stopping scraper. Please wait...",
                "duplicates_removed": "Email duplicates removed successfully.",
                "closing_confirmation": "Scraper is running. Do you want to stop it and exit?",
                
                # Settings window
                "settings_window_title": "Configure Settings",
                "min_subscribers_label": "Minimum Subscribers",
                "min_subscribers_desc": "Minimum number of subscribers a channel must have",
                "max_subscribers_label": "Maximum Subscribers",
                "max_subscribers_desc": "Maximum number of subscribers a channel can have",
                "min_total_views_label": "Minimum Total Views",
                "min_total_views_desc": "Minimum number of total views a channel must have",
                "creation_year_limit_label": "Creation Year Limit",
                "creation_year_limit_desc": "Only consider channels created after this year",
                "delay_min_label": "Minimum Delay (seconds)",
                "delay_min_desc": "Minimum delay between requests",
                "delay_max_label": "Maximum Delay (seconds)",
                "delay_max_desc": "Maximum delay between requests",
                "parse_mode_label": "Parse Mode",
                "parse_mode_desc": "What to parse: 'email', 'social', or 'both'",
                "max_workers_label": "Max Worker Threads",
                "max_workers_desc": "Maximum number of concurrent threads",
                "batch_size_label": "Batch Size",
                "batch_size_desc": "Number of items to process in one batch",
                "use_caching_label": "Use Caching",
                "use_caching_desc": "Whether to use caching (true/false)",
                "shorts_filter_mode_label": "Shorts Filter Mode",
                "shorts_filter_mode_desc": "Filtering mode for channels based on Shorts content",
                "shorts_mode_1_desc": "Skip channels with only Shorts or more Shorts than regular videos",
                "shorts_mode_2_desc": "Only include channels with no Shorts videos at all",
                "shorts_mode_3_desc": "Include all channels meeting other criteria",
                "save_settings_btn": "Save Settings",
                
                # Advanced Email Finder settings
                "use_advanced_email_finder_label": "Advanced Email Finder",
                "use_advanced_email_finder_desc": "Use advanced email finding techniques (true/false)",
                "email_finder_max_depth_label": "Max Site Depth",
                "email_finder_max_depth_desc": "Maximum depth of scanning linked websites (0-3)",
                "email_finder_dns_check_label": "DNS Check",
                "email_finder_dns_check_desc": "Validate email domains via DNS (true/false)",
                "email_finder_ai_heuristics_label": "AI Heuristics",
                "email_finder_ai_heuristics_desc": "Use AI-based heuristics for email finding (true/false)",
                
                # File paths window
                "file_paths_window_title": "Configure File Paths",
                "keywords_file_label": "Keywords File",
                "keywords_file_desc": "List of keywords to search for",
                "proxy_file_label": "Proxy File",
                "proxy_file_desc": "List of proxies to use",
                "settings_file_label": "Settings File",
                "settings_file_desc": "Scraper settings",
                "blacklist_file_label": "Blacklist File",
                "blacklist_file_desc": "Countries to exclude",
                "api_file_label": "API Keys File",
                "api_file_desc": "YouTube API keys",
                "channels_file_label": "Channels Output",
                "channels_file_desc": "File to save channel information",
                "emails_file_label": "Emails Output",
                "emails_file_desc": "File to save discovered emails",
                "social_media_file_label": "Social Media Output",
                "social_media_file_desc": "File to save social media links",
                "save_file_paths_btn": "Save File Paths",
                "browse_btn": "Browse",
                
                # Results window
                "results_window_title": "Scraper Results",
                "channels_tab": "Channels",
                "emails_tab": "Emails",
                "social_media_tab": "Social Media",
                "export_btn": "Export {0} to CSV",
                "export_successful": "Export Successful",
                "export_success_msg": "Successfully exported to {0}.",
                "export_error": "Export Error",
                "export_error_msg": "Error exporting to CSV: {0}",
                
                # About window
                "about_window_title": "About YouTube Channel Scraper",
                "about_app_name": "YT Scraper",
                "about_app_version": "Version 1.0",
                "about_app_description": "A graphical interface for the YouTube channel scraper script.\nCollects channel data, emails, and social media links.",
                "close_btn": "Close",
                
                # Messages
                "file_saved": "File {0} saved successfully.",
                "error_loading_file": "Error loading file {0}: {1}",
                "file_not_found": "File {0} not found.",
                "error_saving_file": "Error saving file {0}: {1}",
                "settings_saved_successfully": "Settings saved successfully.",
                "file_paths_saved_successfully": "File paths saved successfully.",
                "file_paths_loaded_successfully": "File paths loaded successfully.",
                "error_loading_file_paths": "Error loading file paths: {0}",
                
                # File editor
                "edit": "Edit",
                "save": "Save"
            },
            "ru": {
                # Menu items
                "file_menu": "Файл",
                "start_scraping": "Запустить парсинг",
                "stop_scraping": "Остановить парсинг",
                "exit": "Выход",
                "settings_menu": "Настройки",
                "configure_settings": "Настроить параметры",
                "configure_file_paths": "Настроить пути файлов",
                "view_menu": "Просмотр",
                "view_keywords": "Просмотр ключевых слов",
                "view_proxies": "Просмотр прокси",
                "view_blacklist": "Просмотр черного списка",
                "view_api_keys": "Просмотр API ключей",
                "view_debug_log": "Просмотр лог файла отладки",
                "help_menu": "Помощь",
                "about": "О программе",
                "language_menu": "Язык",
                "email_format_menu": "Формат email",
                "email_format_full": "Полный (с информацией о канале)",
                "email_format_email_only": "Только email",
                "validate_api_keys": "Проверить API ключи",
                "validate_api_btn": "Проверить API",
                "validation_started": "Начинается проверка API ключей...",
                "validation_not_available": "Невозможно проверить API ключи пока парсер запущен.",
                "validation_complete_title": "Проверка завершена",
                "validation_error_title": "Ошибка проверки",
                "validation_success": "Успешно проверено {} API ключей. Используются проверенные ключи для будущих операций.",
                "validation_no_valid_keys": "Не найдено действительных API ключей. Пожалуйста, проверьте ваши API ключи и попробуйте снова.",
                "validation_error": "Ошибка при проверке API: {}",
                "validating": "Проверка...",
                "validated": "Проверены",
                "validation_failed": "Ошибка",
                "remove_duplicates": "Удалить дубликаты email",
                
                # Buttons and labels
                "configure_settings_btn": "Настройки",
                "configure_file_paths_btn": "Пути файлов",
                "view_results_btn": "Результаты",
                
                # Status items
                "status_title": "Статус",
                "api_keys_status": "API ключи",
                "keywords_status": "Ключевые слова",
                "settings_status": "Настройки",
                "running_status": "Статус работы",
                "api_quota_status": "Квота API",
                
                # Statistics items
                "statistics_title": "Статистика",
                "channels_found": "Найдено каналов",
                "emails_found": "Найдено email",
                "social_media_found": "Найдено соц. сетей",
                "keywords_processed": "Обработано ключевых слов",
                
                # Log titles
                "log_title": "Журнал",
                
                # Status values
                "not_loaded": "Не загружено",
                "loaded": "Загружено",
                "running": "Работает",
                "stopped": "Остановлено",
                "stopping": "Останавливается...",
                "saved": "Сохранено",
                "error": "Ошибка",
                
                # Messages
                "gui_started": "Интерфейс парсера YouTube каналов запущен. Готов к настройке и запуску.",
                "already_running": "Парсер уже запущен.",
                "init_failed": "Не удалось инициализировать парсер. Проверьте настройки и API ключи.",
                "scraping_started": "Процесс парсинга запущен.",
                "scraping_complete": "Парсинг успешно завершен.",
                "scraping_stopping": "Останавливаем парсер. Пожалуйста, подождите...",
                "duplicates_removed": "Дубликаты email успешно удалены.",
                "closing_confirmation": "Парсер запущен. Хотите остановить его и выйти?",
                
                # Settings window
                "settings_window_title": "Настройка параметров",
                "min_subscribers_label": "Минимум подписчиков",
                "min_subscribers_desc": "Минимальное количество подписчиков канала",
                "max_subscribers_label": "Максимум подписчиков",
                "max_subscribers_desc": "Максимальное количество подписчиков канала",
                "min_total_views_label": "Минимум просмотров",
                "min_total_views_desc": "Минимальное общее количество просмотров канала",
                "creation_year_limit_label": "Год создания канала",
                "creation_year_limit_desc": "Учитывать только каналы, созданные после этого года",
                "delay_min_label": "Минимальная задержка (сек)",
                "delay_min_desc": "Минимальная задержка между запросами",
                "delay_max_label": "Максимальная задержка (сек)",
                "delay_max_desc": "Максимальная задержка между запросами",
                "parse_mode_label": "Режим парсинга",
                "parse_mode_desc": "Что парсить: 'email', 'social', или 'both' (оба)",
                "max_workers_label": "Макс. потоков",
                "max_workers_desc": "Максимальное количество одновременных потоков",
                "batch_size_label": "Размер пакета",
                "batch_size_desc": "Количество элементов для обработки в одном пакете",
                "use_caching_label": "Использовать кэш",
                "use_caching_desc": "Использовать кэширование (true/false)",
                "shorts_filter_mode_label": "Режим фильтрации Shorts",
                "shorts_filter_mode_desc": "Режим фильтрации каналов на основе Shorts контента",
                "shorts_mode_1_desc": "Пропускать каналы только с Shorts или с преобладанием Shorts",
                "shorts_mode_2_desc": "Включать только каналы совсем без Shorts видео",
                "shorts_mode_3_desc": "Включать все каналы, соответствующие другим критериям",
                "save_settings_btn": "Сохранить настройки",
                
                # Advanced Email Finder settings
                "use_advanced_email_finder_label": "Расширенный поиск Email",
                "use_advanced_email_finder_desc": "Использовать расширенные техники поиска email (true/false)",
                "email_finder_max_depth_label": "Глубина сканирования",
                "email_finder_max_depth_desc": "Максимальная глубина сканирования связанных сайтов (0-3)",
                "email_finder_dns_check_label": "DNS проверка",
                "email_finder_dns_check_desc": "Проверять доменные имена через DNS (true/false)",
                "email_finder_ai_heuristics_label": "AI эвристика",
                "email_finder_ai_heuristics_desc": "Использовать AI эвристики для поиска email (true/false)",
                
                # File paths window
                "file_paths_window_title": "Настройка путей файлов",
                "keywords_file_label": "Файл ключевых слов",
                "keywords_file_desc": "Список ключевых слов для поиска",
                "proxy_file_label": "Файл прокси",
                "proxy_file_desc": "Список прокси для использования",
                "settings_file_label": "Файл настроек",
                "settings_file_desc": "Настройки парсера",
                "blacklist_file_label": "Файл черного списка",
                "blacklist_file_desc": "Страны для исключения",
                "api_file_label": "Файл API ключей",
                "api_file_desc": "API ключи YouTube",
                "channels_file_label": "Файл каналов",
                "channels_file_desc": "Файл для сохранения информации о каналах",
                "emails_file_label": "Файл email",
                "emails_file_desc": "Файл для сохранения найденных email адресов",
                "social_media_file_label": "Файл соц. сетей",
                "social_media_file_desc": "Файл для сохранения ссылок на социальные сети",
                "save_file_paths_btn": "Сохранить пути файлов",
                "browse_btn": "Обзор",
                
                # Results window
                "results_window_title": "Результаты парсера",
                "channels_tab": "Каналы",
                "emails_tab": "Email",
                "social_media_tab": "Соц. сети",
                "export_btn": "Экспорт {0} в CSV",
                "export_successful": "Экспорт выполнен",
                "export_success_msg": "Успешно экспортировано в {0}.",
                "export_error": "Ошибка экспорта",
                "export_error_msg": "Ошибка экспорта в CSV: {0}",
                
                # About window
                "about_window_title": "О программе YouTube Channel Scraper",
                "about_app_name": "Парсер YouTube",
                "about_app_version": "Версия 1.0",
                "about_app_description": "Графический интерфейс для скрипта парсинга YouTube каналов.\nСобирает данные каналов, email адреса и ссылки на социальные сети.",
                "close_btn": "Закрыть",
                
                # Messages
                "file_saved": "Файл {0} успешно сохранен.",
                "error_loading_file": "Ошибка загрузки файла {0}: {1}",
                "file_not_found": "Файл {0} не найден.",
                "error_saving_file": "Ошибка сохранения файла {0}: {1}",
                "settings_saved_successfully": "Настройки успешно сохранены.",
                "file_paths_saved_successfully": "Пути файлов успешно сохранены.",
                "file_paths_loaded_successfully": "Пути файлов успешно загружены.",
                "error_loading_file_paths": "Ошибка загрузки путей файлов: {0}",
                
                # File editor
                "edit": "Редактировать",
                "save": "Сохранить"
            }
        }
        return translations
        
    
    def get_translation(self, key):
        """Get translated text based on current language"""
        if key in self.translations[self.language]:
            return self.translations[self.language][key]
        # Fallback to English if translation is missing
        if key in self.translations["en"]:
            return self.translations["en"][key]
        # Return the key itself if no translation found
        return key
    
    def switch_language(self, lang):
        """Switch the UI language"""
        if lang in self.translations:
            self.language = lang
            # Update UI text elements
            self.update_ui_language()
            self.gui_logger.log("INFO", f"Language switched to {lang}")
    
    def update_ui_language(self):
        """Update all UI text elements based on selected language"""
        # Update menu items
        self.recreate_menu()
        
        # Update status frame
        self.status_frame.configure(text=self.get_translation("status_title"))
        
        # Update statistics frame
        self.stats_frame.configure(text=self.get_translation("statistics_title"))
        
        # Update log frame
        self.log_frame.configure(text=self.get_translation("log_title"))
        
        # Update buttons
        self.start_button.configure(text=self.get_translation("start_scraping"))
        self.stop_button.configure(text=self.get_translation("stop_scraping"))
        
        # Update other buttons
        for widget in self.middle_frame.winfo_children():
            if isinstance(widget, ttk.Button):
                if widget.cget("text") == "Configure Settings" or widget.cget("text") == "Настроить параметры":
                    widget.configure(text=self.get_translation("configure_settings_btn"))
                elif widget.cget("text") == "Configure File Paths" or widget.cget("text") == "Настроить пути файлов":
                    widget.configure(text=self.get_translation("configure_file_paths_btn"))
                elif widget.cget("text") == "View Results" or widget.cget("text") == "Результаты":
                    widget.configure(text=self.get_translation("view_results_btn"))
    
    def create_menu(self):
        """Create the top menu bar"""
        self.menu_bar = tk.Menu(self.root)
        
        # File menu
        file_menu = tk.Menu(self.menu_bar, tearoff=0)
        file_menu.add_command(label=self.get_translation("start_scraping"), command=self.start_scraping)
        file_menu.add_command(label=self.get_translation("stop_scraping"), command=self.stop_scraping)
        file_menu.add_separator()
        file_menu.add_command(label=self.get_translation("remove_duplicates"), command=self.remove_email_duplicates)
        file_menu.add_separator()
        file_menu.add_command(label=self.get_translation("exit"), command=self.on_exit)
        self.menu_bar.add_cascade(label=self.get_translation("file_menu"), menu=file_menu)
        
        # Settings menu
        settings_menu = tk.Menu(self.menu_bar, tearoff=0)
        settings_menu.add_command(
            label=self.get_translation("configure_settings"), 
            command=self.open_settings
        )
        settings_menu.add_command(
            label=self.get_translation("configure_file_paths"), 
            command=self.open_file_paths
        )
        
        settings_menu.add_separator()
        settings_menu.add_command(
            label=self.get_translation("validate_api_keys"),
            command=self.validate_api_keys
        )
        self.menu_bar.add_cascade(label=self.get_translation("settings_menu"), menu=settings_menu)
        
        # View menu
        view_menu = tk.Menu(self.menu_bar, tearoff=0)
        view_menu.add_command(
            label=self.get_translation("view_keywords"), 
            command=lambda: self.open_file_editor("keywords")
        )
        view_menu.add_command(
            label=self.get_translation("view_proxies"), 
            command=lambda: self.open_file_editor("proxy")
        )
        view_menu.add_command(
            label=self.get_translation("view_blacklist"), 
            command=lambda: self.open_file_editor("blacklist")
        )
        view_menu.add_command(
            label=self.get_translation("view_api_keys"), 
            command=lambda: self.open_file_editor("api")
        )
        view_menu.add_separator()
        view_menu.add_command(
            label=self.get_translation("view_debug_log"), 
            command=lambda: self.open_file_viewer("debug.txt")
        )
        self.menu_bar.add_cascade(label=self.get_translation("view_menu"), menu=view_menu)
        
        # Language menu
        language_menu = tk.Menu(self.menu_bar, tearoff=0)
        language_menu.add_radiobutton(
            label="English", 
            variable=tk.StringVar(value=self.language),
            value="en",
            command=lambda: self.switch_language("en")
        )
        language_menu.add_radiobutton(
            label="Русский", 
            variable=tk.StringVar(value=self.language),
            value="ru",
            command=lambda: self.switch_language("ru")
        )
        self.menu_bar.add_cascade(label=self.get_translation("language_menu"), menu=language_menu)
        
        # Help menu
        help_menu = tk.Menu(self.menu_bar, tearoff=0)
        help_menu.add_command(label=self.get_translation("about"), command=self.show_about)
        self.menu_bar.add_cascade(label=self.get_translation("help_menu"), menu=help_menu)
        
        # Apply the menu bar to the root window
        self.root.config(menu=self.menu_bar)
    
    def validate_api_keys(self):
        """Validate YouTube API keys using the api_validator module."""
        if self.is_running:
            messagebox.showinfo(
                "Validation Not Available", 
                self.get_translation("validation_not_available")
            )
            return
        
        # Show status message
        self.gui_logger.log("INFO", self.get_translation("validation_started"))
        self.update_status("API Keys", self.get_translation("validating"), "orange")
        
        # Run the validation in a separate thread to avoid blocking the GUI
        self.validation_thread = threading.Thread(target=self._run_validation)
        self.validation_thread.daemon = True
        self.validation_thread.start()

    def _run_validation(self):
        """Run the API key validation in a background thread."""
        try:
            # Get the current API file path from our settings
            api_file = self.file_paths["api"]
            output_file = 'Good_API.txt'
            
            # Run the validation
            success = validate_api_keys(api_file, output_file)
            
            # Update UI on the main thread
            self.root.after(0, lambda: self._validation_completed(success, output_file))
            
        except Exception as e:
            error_msg = f"Error during API validation: {str(e)}"
            
            # Log the error
            logging.error(error_msg)
            with open("debug.txt", "a") as f:
                f.write(f"=== API VALIDATION ERROR AT {datetime.now()} ===\n")
                f.write(error_msg + "\n")
                f.write(traceback.format_exc() + "\n\n")
            
            # Update UI on the main thread
            self.root.after(0, lambda: self._validation_failed(str(e)))

    def _validation_completed(self, success, output_file):
        """Handle completion of API validation."""
        if success:
            # Read the validated keys
            try:
                with open(output_file, 'r', encoding='utf-8') as f:
                    valid_keys = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
                    
                # Update the file path to use validated keys
                self.file_paths["api"] = output_file
                
                # Update the scraper's API keys
                self.scraper.api_keys = valid_keys
                
                # Update status indicators
                self.update_status("API Keys", self.get_translation("validated"), "green")
                self.gui_logger.log("INFO", self.get_translation("validation_success").format(len(valid_keys)))
                
                # Show success message
                messagebox.showinfo(
                    self.get_translation("validation_complete_title"),
                    self.get_translation("validation_success").format(len(valid_keys))
                )
                
                # Refresh API usage display
                self.update_api_usage_text()
                
            except Exception as e:
                self._validation_failed(str(e))
        else:
            # Validation was not successful
            self.update_status("API Keys", self.get_translation("validation_failed"), "red")
            self.gui_logger.log("ERROR", self.get_translation("validation_no_valid_keys"))
            
            messagebox.showerror(
                self.get_translation("validation_error_title"),
                self.get_translation("validation_no_valid_keys")
            )

    def _validation_failed(self, error_message):
        """Handle API validation failure."""
        self.update_status("API Keys", self.get_translation("validation_failed"), "red")
        self.gui_logger.log("ERROR", self.get_translation("validation_error").format(error_message))
        
        messagebox.showerror(
            self.get_translation("validation_error_title"),
            self.get_translation("validation_error").format(error_message)
        )
    
    def recreate_menu(self):
        """Recreate the menu with updated language"""
        self.root.config(menu="")  # Remove existing menu
        self.create_menu()  # Create new menu
    
    def create_main_frame(self):
        """Create the main frame with all controls"""
        # Create a style for ttk widgets
        self.style = ttk.Style()
        self.style.configure("TButton", font=("Arial", 10), background=self.colors["button_bg"])
        self.style.map("TButton", 
                       background=[('active', self.colors["button_active"])],
                       foreground=[('active', 'black')])
        
        self.style.configure("Green.TButton", background=self.colors["success"], foreground="black")
        self.style.map("Green.TButton",
                       background=[('active', '#388E3C')],  # Darker green
                       foreground=[('active', 'black')])
                       
        self.style.configure("Red.TButton", background=self.colors["error"], foreground="black")
        self.style.map("Red.TButton",
                      background=[('active', '#D32F2F')],  # Darker red
                      foreground=[('active', 'black')])
        
        self.style.configure("Blue.TButton", background=self.colors["button_bg"], foreground="black")
        self.style.map("Blue.TButton",
                      background=[('active', self.colors["button_active"])],
                      foreground=[('active', 'black')])
        
        # Create main container frame
        self.main_frame = ttk.Frame(self.root, padding=5)
        self.main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Top frame for buttons
        self.top_frame = ttk.Frame(self.main_frame, padding=2)
        self.top_frame.pack(fill=tk.X)
        
        # Start button
        self.start_button = ttk.Button(
            self.top_frame, 
            text=self.get_translation("start_scraping"), 
            command=self.start_scraping,
            style="Green.TButton"
        )
        self.start_button.pack(side=tk.LEFT, padx=2)
        
        # Stop button
        self.stop_button = ttk.Button(
            self.top_frame, 
            text=self.get_translation("stop_scraping"), 
            command=self.stop_scraping,
            style="Red.TButton"
        )
        self.stop_button.pack(side=tk.LEFT, padx=2)
        self.stop_button.config(state=tk.DISABLED)
        
        # Middle frame with settings buttons and status
        self.middle_frame = ttk.Frame(self.main_frame, padding=2)
        self.middle_frame.pack(fill=tk.X, pady=2)
        
        # Settings button
        settings_button = ttk.Button(
            self.middle_frame,
            text=self.get_translation("configure_settings_btn"),
            command=self.open_settings,
            style="Blue.TButton"
        )
        settings_button.pack(side=tk.LEFT, padx=2)
        
        # File paths button
        file_paths_button = ttk.Button(
            self.middle_frame,
            text=self.get_translation("configure_file_paths_btn"),
            command=self.open_file_paths,
            style="Blue.TButton"
        )
        file_paths_button.pack(side=tk.LEFT, padx=2)
        
        # View Results button
        view_results_button = ttk.Button(
            self.middle_frame,
            text=self.get_translation("view_results_btn"),
            command=self.view_results,
            style="Blue.TButton"
        )
        view_results_button.pack(side=tk.LEFT, padx=2)
        
        # API Validator button
        validate_api_button = ttk.Button(
            self.middle_frame,
            text=self.get_translation("validate_api_btn"),
            command=self.validate_api_keys,
            style="Blue.TButton"
        )
        validate_api_button.pack(side=tk.LEFT, padx=2)
        
        # Two-column layout for status, stats and log
        self.info_frame = ttk.Frame(self.main_frame)
        self.info_frame.pack(fill=tk.BOTH, expand=True, pady=2)
        
        # Left column for status and stats
        self.left_column = ttk.Frame(self.info_frame)
        self.left_column.pack(side=tk.LEFT, fill=tk.Y, padx=2)
        
        # Status frame
        self.status_frame = ttk.LabelFrame(
            self.left_column, 
            text=self.get_translation("status_title"), 
            padding=3
        )
        self.status_frame.pack(fill=tk.X, pady=2)
        
        # Status indicators
        self.status_indicators = {}
        status_items = [
            ("API Keys", self.get_translation("not_loaded")),
            ("Keywords", self.get_translation("not_loaded")),
            ("Settings", self.get_translation("not_loaded")),
            ("Running Status", self.get_translation("stopped")),
            ("API Quota", "OK")
        ]
        
        # Use grid layout for more compact display
        for i, (label_key, value) in enumerate(status_items):
            label_text = self.get_translation(label_key) if label_key in self.translations[self.language] else label_key
            
            label_widget = ttk.Label(
                self.status_frame, 
                text=f"{label_text}:", 
                font=("Arial", 9, "bold")
            )
            label_widget.grid(row=i, column=0, sticky=tk.W, padx=2, pady=1)
            
            value_widget = ttk.Label(self.status_frame, text=value, font=("Arial", 9))
            value_widget.grid(row=i, column=1, sticky=tk.W, padx=2, pady=1)
            
            self.status_indicators[label_key] = value_widget
        
        # Statistics frame
        self.stats_frame = ttk.LabelFrame(
            self.left_column, 
            text=self.get_translation("statistics_title"), 
            padding=3
        )
        self.stats_frame.pack(fill=tk.X, pady=2)
        
        # Statistics indicators
        self.stats_indicators = {}
        stats_items = [
            ("Channels Found", "0"),
            ("Emails Found", "0"),
            ("Social Media Found", "0"),
            ("Keywords Processed", "0")
        ]
        
        # Use grid layout for more compact display
        for i, (label_key, value) in enumerate(stats_items):
            label_text = self.get_translation(label_key) if label_key in self.translations[self.language] else label_key
            
            label_widget = ttk.Label(
                self.stats_frame, 
                text=f"{label_text}:", 
                font=("Arial", 9, "bold")
            )
            label_widget.grid(row=i, column=0, sticky=tk.W, padx=2, pady=1)
            
            value_widget = ttk.Label(self.stats_frame, text=value, font=("Arial", 9))
            value_widget.grid(row=i, column=1, sticky=tk.W, padx=2, pady=1)
            
            self.stats_indicators[label_key] = value_widget
        
        # API usage frame
        self.api_frame = ttk.LabelFrame(
            self.left_column, 
            text="API Usage", 
            padding=3
        )
        self.api_frame.pack(fill=tk.BOTH, expand=True, pady=2)
        
        # API usage text widget
        self.api_text = scrolledtext.ScrolledText(self.api_frame, width=30, height=8)
        self.api_text.pack(fill=tk.BOTH, expand=True)
        self.api_text.configure(state='disabled', font=("Consolas", 8))
        
        # Right column for log
        self.right_column = ttk.Frame(self.info_frame)
        self.right_column.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=2)
        
        # Log frame
        self.log_frame = ttk.LabelFrame(
            self.right_column, 
            text=self.get_translation("log_title"), 
            padding=3
        )
        self.log_frame.pack(fill=tk.BOTH, expand=True, pady=2)
        
        # Create scrolled text widget for logging
        self.log_text = scrolledtext.ScrolledText(self.log_frame, width=50, height=20)
        self.log_text.pack(fill=tk.BOTH, expand=True)
        self.log_text.configure(state='disabled', font=("Consolas", 9))
        
        # Configure text tags for different log levels
        self.log_text.tag_configure("info", foreground="black")
        self.log_text.tag_configure("debug", foreground="blue")
        self.log_text.tag_configure("warning", foreground="orange")
        self.log_text.tag_configure("error", foreground="red")
        
        # Create GUI logger
        self.gui_logger = GUILogger(self.log_text)
    
    def check_files_status(self):
        """Check the status of required files and update indicators"""
        # Check API keys
        try:
            with open(self.file_paths["api"], 'r', encoding='utf-8') as f:
                api_keys = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
                if api_keys:
                    self.update_status("API Keys", self.get_translation("loaded"), "green")
                    # Also update scraper's api_keys list for GUI display
                    self.scraper.api_keys = api_keys
                else:
                    self.update_status("API Keys", self.get_translation("not_loaded"), "red")
        except Exception:
            self.update_status("API Keys", self.get_translation("not_loaded"), "red")
        
        # Check keywords
        try:
            with open(self.file_paths["keywords"], 'r', encoding='utf-8') as f:
                keywords = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
                if keywords:
                    self.update_status("Keywords", self.get_translation("loaded"), "green")
                    # Also update scraper's keywords list for GUI display
                    self.scraper.keywords = keywords
                else:
                    self.update_status("Keywords", self.get_translation("not_loaded"), "red")
        except Exception:
            self.update_status("Keywords", self.get_translation("not_loaded"), "red")
        
        # Check settings
        try:
            with open(self.file_paths["settings"], 'r', encoding='utf-8') as f:
                has_settings = False
                for line in f:
                    if line.strip() and not line.strip().startswith('#'):
                        has_settings = True
                        break
                
                if has_settings:
                    self.update_status("Settings", self.get_translation("loaded"), "green")
                else:
                    self.update_status("Settings", self.get_translation("not_loaded"), "red")
        except Exception:
            self.update_status("Settings", self.get_translation("not_loaded"), "red")
        
        # Update API usage text
        self.update_api_usage_text()
    
    def update_api_usage_text(self):
        """Update the API usage text widget with current API key information"""
        self.api_text.configure(state='normal')
        self.api_text.delete(1.0, tk.END)
        
        if hasattr(self.scraper, 'api_keys') and self.scraper.api_keys:
            self.api_text.insert(tk.END, f"API Keys: {len(self.scraper.api_keys)}\n\n")
            
            for i, key in enumerate(self.scraper.api_keys):
                # Mask most of the key for security
                masked_key = key[:4] + '*' * (len(key) - 8) + key[-4:] if len(key) > 8 else '*' * len(key)
                
                # Show usage count if available
                usage_count = self.scraper.api_usage_count.get(key, 0)
                
                self.api_text.insert(tk.END, f"{i+1}. {masked_key}\n")
                self.api_text.insert(tk.END, f"   Uses: {usage_count}\n")
        else:
            self.api_text.insert(tk.END, "No API keys loaded.\nPlease add API keys in the settings.")
        
        self.api_text.configure(state='disabled')
    
    def start_scraping(self):
        """Start the scraping process in a separate thread"""
        if self.is_running:
            messagebox.showinfo(
                "Already Running", 
                self.get_translation("already_running")
            )
            return
        
        # Check if the scraper initialized properly
        try:
            if not self.scraper.initialize():
                self.gui_logger.log(
                    "ERROR", 
                    self.get_translation("init_failed")
                )
                return
            
            # Update status
            self.is_running = True
            self.update_status("Running Status", self.get_translation("running"), "green")
            
            # Disable start button, enable stop button
            self.start_button.config(state=tk.DISABLED)
            self.stop_button.config(state=tk.NORMAL)
            
            # Start the scraper in a separate thread
            self.scraper_thread = threading.Thread(target=self.run_scraper)
            self.scraper_thread.daemon = True
            self.scraper_thread.start()
            
            # Start the update timer
            self.root.after(1000, self.update_statistics)
            
            self.gui_logger.log("INFO", self.get_translation("scraping_started"))
            
        except Exception as e:
            error_msg = f"Error starting scraper: {str(e)}"
            self.gui_logger.log("ERROR", error_msg)
            
            # Also log detailed traceback to debug.txt
            with open("debug.txt", "a") as f:
                f.write(f"=== ERROR AT {datetime.now()} ===\n")
                f.write(error_msg + "\n")
                f.write(traceback.format_exc() + "\n\n")
    
    def run_scraper(self):
        """Run the scraper with error handling"""
        try:
            self.scraper.run()
            self.gui_logger.log("INFO", self.get_translation("scraping_complete"))
        except Exception as e:
            error_msg = f"Error during scraping: {str(e)}"
            self.gui_logger.log("ERROR", error_msg)
            
            # Log detailed error to debug.txt
            with open("debug.txt", "a") as f:
                f.write(f"=== RUNTIME ERROR AT {datetime.now()} ===\n")
                f.write(error_msg + "\n")
                f.write(traceback.format_exc() + "\n\n")
        finally:
            # Update UI when done
            self.root.after(0, self.scraping_finished)
    
    def scraping_finished(self):
        """Update UI when scraping is finished"""
        self.is_running = False
        self.update_status("Running Status", self.get_translation("stopped"), "black")
        self.start_button.config(state=tk.NORMAL)
        self.stop_button.config(state=tk.DISABLED)
        
        # Final update of statistics and API usage
        self.update_statistics()
    
    def stop_scraping(self):
        """Stop the scraping process"""
        if not self.is_running:
            return
        
        self.gui_logger.log("INFO", self.get_translation("scraping_stopping"))
        
        # Call the scraper's stop method if available
        if hasattr(self.scraper, 'stop'):
            self.scraper.stop()
        else:
            # Fallback: Set a flag to stop the scraper
            self.is_running = False
            
            # This will allow the scraper to finish current task and save progress
            if hasattr(self.scraper, 'save_progress'):
                self.scraper.save_progress()
        
        self.update_status("Running Status", self.get_translation("stopping"), "orange")
        self.stop_button.config(state=tk.DISABLED)
    
    def update_statistics(self):
        """Update statistics display if scraper is running"""
        if not self.is_running:
            return
        
        try:
            # Update channel count
            if hasattr(self.scraper, 'parsed_channels'):
                self.update_stat("Channels Found", str(len(self.scraper.parsed_channels)))
            
            # Update email count
            if hasattr(self.scraper, 'parsed_emails'):
                self.update_stat("Emails Found", str(len(self.scraper.parsed_emails)))
            
            # Update social media count
            if hasattr(self.scraper, 'parsed_social_media'):
                self.update_stat("Social Media Found", str(len(self.scraper.parsed_social_media)))
            
            # Update processed keywords
            if hasattr(self.scraper, 'processed_keywords'):
                self.update_stat("Keywords Processed", str(len(self.scraper.processed_keywords)))
            
            # Update API usage display
            self.update_api_usage_text()
            
            # Check API quota status
            if hasattr(self.scraper, 'api_keys') and self.scraper.api_keys:
                initial_count = len(self.scraper.api_keys)
                current_count = len(self.scraper.api_keys)
                quota_used = False
                
                for key, count in self.scraper.api_usage_count.items():
                    if count > 0:
                        quota_used = True
                
                if current_count < initial_count:
                    self.update_status("API Quota", "Warning: Some keys exceeded quota", "orange")
                elif quota_used:
                    self.update_status("API Quota", "OK - In use", "green")
                else:
                    self.update_status("API Quota", "OK", "green")
            else:
                self.update_status("API Quota", "No keys", "red")
            
        except Exception as e:
            self.gui_logger.log("ERROR", f"Error updating statistics: {str(e)}")
            
            # Log to debug.txt
            with open("debug.txt", "a") as f:
                f.write(f"=== STATISTICS ERROR AT {datetime.now()} ===\n")
                f.write(f"Error updating statistics: {str(e)}\n")
                f.write(traceback.format_exc() + "\n\n")
        
        # Schedule next update
        if self.is_running:
            self.root.after(1000, self.update_statistics)
    
    def update_status(self, key, value, color="black"):
        """Update a status indicator"""
        if key in self.status_indicators:
            self.status_indicators[key].config(text=value, foreground=color)
    
    def update_stat(self, key, value):
        """Update a statistics indicator"""
        if key in self.stats_indicators:
            self.stats_indicators[key].config(text=value)
    
    def remove_email_duplicates(self):
        """Remove duplicate emails from the emails.txt file"""
        if hasattr(self.scraper, 'remove_email_duplicates'):
            try:
                self.scraper.remove_email_duplicates()
                self.gui_logger.log("INFO", self.get_translation("duplicates_removed"))
                
                # Update the email count
                if hasattr(self.scraper, 'parsed_emails'):
                    self.update_stat("Emails Found", str(len(self.scraper.parsed_emails)))
            except Exception as e:
                error_msg = f"Error removing email duplicates: {str(e)}"
                self.gui_logger.log("ERROR", error_msg)
                
                # Log to debug.txt
                with open("debug.txt", "a") as f:
                    f.write(f"=== EMAIL DEDUP ERROR AT {datetime.now()} ===\n")
                    f.write(error_msg + "\n")
                    f.write(traceback.format_exc() + "\n\n")
    
    def open_settings(self):
        """Open the settings editor window"""
        settings_window = tk.Toplevel(self.root)
        settings_window.title(self.get_translation("settings_window_title"))
        settings_window.geometry("600x500")
        settings_window.configure(bg=self.colors["bg"])
    
        # Создаем основной фрейм
        main_frame = ttk.Frame(settings_window, padding=10)
        main_frame.pack(fill=tk.BOTH, expand=True)
    
        # Создаем канву с полосой прокрутки для многих настроек
        canvas = tk.Canvas(main_frame)
        scrollbar = ttk.Scrollbar(main_frame, orient="vertical", command=canvas.yview)
    
        # Создаем прокручиваемый фрейм внутри канвы
        scrollable_frame = ttk.Frame(canvas)
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
    
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
    
        # Размещаем канву и полосу прокрутки в основном фрейме
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
    
        # Загружаем текущие настройки
        current_settings = self.load_settings_dict()
    
        # Создаем записи для каждой настройки
        settings_entries = {}
        row = 0
    
        # Настройки с описаниями
        settings_info = {
            "min_subscribers": (self.get_translation("min_subscribers_label"), self.get_translation("min_subscribers_desc")),
            "max_subscribers": (self.get_translation("max_subscribers_label"), self.get_translation("max_subscribers_desc")),
            "min_total_views": (self.get_translation("min_total_views_label"), self.get_translation("min_total_views_desc")),
            "creation_year_limit": (self.get_translation("creation_year_limit_label"), self.get_translation("creation_year_limit_desc")),
            "delay_min": (self.get_translation("delay_min_label"), self.get_translation("delay_min_desc")),
            "delay_max": (self.get_translation("delay_max_label"), self.get_translation("delay_max_desc")),
            "parse_mode": (self.get_translation("parse_mode_label"), self.get_translation("parse_mode_desc")),
            "max_workers": (self.get_translation("max_workers_label"), self.get_translation("max_workers_desc")),
            "batch_size": (self.get_translation("batch_size_label"), self.get_translation("batch_size_desc")),
            "use_caching": (self.get_translation("use_caching_label"), self.get_translation("use_caching_desc")),
            "shorts_filter_mode": (self.get_translation("shorts_filter_mode_label"), self.get_translation("shorts_filter_mode_desc")),
            # Advanced Email Finder settings
            "use_advanced_email_finder": (self.get_translation("use_advanced_email_finder_label"), self.get_translation("use_advanced_email_finder_desc")),
            "email_finder_max_depth": (self.get_translation("email_finder_max_depth_label"), self.get_translation("email_finder_max_depth_desc")),
            "email_finder_dns_check": (self.get_translation("email_finder_dns_check_label"), self.get_translation("email_finder_dns_check_desc")),
            "email_finder_ai_heuristics": (self.get_translation("email_finder_ai_heuristics_label"), self.get_translation("email_finder_ai_heuristics_desc"))
        }
    
        # Создаем словарь для специальных типов настроек (выпадающие списки и т.д.)
        special_settings = {
            "shorts_filter_mode": {
                "type": "dropdown",
                "values": ["1", "2", "3"],
                "descriptions": {
                    "1": "shorts_mode_1_desc",
                    "2": "shorts_mode_2_desc", 
                    "3": "shorts_mode_3_desc"
                }
            },
            "parse_mode": {
                "type": "dropdown",
                "values": ["email", "social", "both"],
                "descriptions": {
                    "email": "Parse only email addresses",
                    "social": "Parse only social media links",
                    "both": "Parse both email and social media links"
                }
            },
            "use_caching": {
                "type": "dropdown",
                "values": ["true", "false"],
                "descriptions": {
                    "true": "Use caching for better performance",
                    "false": "Do not use caching"
                }
            },
            "use_advanced_email_finder": {
                "type": "dropdown",
                "values": ["true", "false"],
                "descriptions": {
                    "true": "Use advanced techniques to find more emails",
                    "false": "Use standard email detection only"
                }
            },
            "email_finder_dns_check": {
                "type": "dropdown",
                "values": ["true", "false"],
                "descriptions": {
                    "true": "Validate email domains using DNS (slower but more accurate)",
                    "false": "Skip domain validation (faster but may include invalid emails)"
                }
            },
            "email_finder_ai_heuristics": {
                "type": "dropdown",
                "values": ["true", "false"],
                "descriptions": {
                    "true": "Use AI-based techniques to find obfuscated emails",
                    "false": "Use only pattern matching for email detection"
                }
            }, 
            "email_finder_max_depth": {
                "type": "dropdown",
                "values": ["0", "1", "2", "3"],
                "descriptions": {
                    "0": "Don't scan linked websites",
                    "1": "Scan only direct links",
                    "2": "Scan up to 2 levels deep (recommended)",
                    "3": "Deep scan up to 3 levels (slower)"
                }
            }
        }
    
        # Добавляем заголовок раздела для основных настроек
        ttk.Label(scrollable_frame, text="Basic Settings", font=("Arial", 12, "bold")).grid(
            row=row, column=0, columnspan=3, sticky=tk.W, padx=5, pady=10
        )
        row += 1
    
        # Итерируем по всем настройкам и создаем их элементы управления
        for key, (label, description) in settings_info.items():
            # Пропускаем настройки Advanced Email Finder для отдельного раздела
            if key in ["use_advanced_email_finder", "email_finder_max_depth", "email_finder_dns_check", "email_finder_ai_heuristics"]:
                continue
            
            # Проверяем, есть ли специальная обработка для этого ключа
            if key in special_settings and special_settings[key]["type"] == "dropdown":
                # Создаем метку
                ttk.Label(scrollable_frame, text=f"{label}:", font=("Arial", 10, "bold")).grid(
                    row=row, column=0, sticky=tk.W, padx=5, pady=2
                )
            
                # Создаем выпадающий список вместо текстового поля
                values = special_settings[key]["values"]
                combo = ttk.Combobox(scrollable_frame, values=values, state="readonly", width=18)
                combo.grid(row=row, column=1, sticky=tk.W, padx=5, pady=2)
            
                # Устанавливаем текущее значение
                current_value = current_settings.get(key, values[-1])  # По умолчанию последнее значение
                combo.set(current_value)
            
                # Описания для режимов
                descriptions = special_settings[key]["descriptions"]
                if key in ["shorts_filter_mode"]:
                    # Для описаний, которые требуют перевода
                    descriptions = {val: self.get_translation(desc) for val, desc in descriptions.items()}
            
                desc_label = ttk.Label(scrollable_frame, text=descriptions.get(current_value, ""), 
                                  font=("Arial", 8), foreground="gray")
                desc_label.grid(row=row, column=2, sticky=tk.W, padx=5, pady=2)
            
                # Обновление описания при изменении выбора
                def update_desc(event, desc_label=desc_label, descriptions=descriptions):
                    desc_label.config(text=descriptions.get(combo.get(), ""))
            
                combo.bind("<<ComboboxSelected>>", update_desc)
            
                # Сохраняем в словаре полей
                settings_entries[key] = combo
                row += 1
                continue  # Пропускаем стандартное создание текстового поля
        
            # Label
            ttk.Label(scrollable_frame, text=f"{label}:", font=("Arial", 10, "bold")).grid(
                row=row, column=0, sticky=tk.W, padx=5, pady=2
            )
        
            # Entry
            entry = ttk.Entry(scrollable_frame, width=20)
            entry.grid(row=row, column=1, sticky=tk.W, padx=5, pady=2)
            entry.insert(0, current_settings.get(key, ""))
        
            # Description
            ttk.Label(scrollable_frame, text=description, font=("Arial", 8), foreground="gray").grid(
                row=row, column=2, sticky=tk.W, padx=5, pady=2
            )
        
            settings_entries[key] = entry
            row += 1
    
        # Добавляем разделитель
        ttk.Separator(scrollable_frame, orient=tk.HORIZONTAL).grid(
            row=row, column=0, columnspan=3, sticky=tk.EW, pady=10
        )
        row += 1
    
        # Добавляем заголовок раздела для Advanced Email Finder
        ttk.Label(scrollable_frame, text="Advanced Email Finder Settings", font=("Arial", 12, "bold")).grid(
            row=row, column=0, columnspan=3, sticky=tk.W, padx=5, pady=10
        )
        row += 1
    
        # Добавляем настройки для Advanced Email Finder
        advanced_keys = ["use_advanced_email_finder", "email_finder_max_depth", "email_finder_dns_check", "email_finder_ai_heuristics"]
        for key in advanced_keys:
            label, description = settings_info[key]
        
            # Проверяем, есть ли специальная обработка для этого ключа
            if key in special_settings and special_settings[key]["type"] == "dropdown":
                # Создаем метку
                ttk.Label(scrollable_frame, text=f"{label}:", font=("Arial", 10, "bold")).grid(
                    row=row, column=0, sticky=tk.W, padx=5, pady=2
                )
            
                # Создаем выпадающий список вместо текстового поля
                values = special_settings[key]["values"]
                combo = ttk.Combobox(scrollable_frame, values=values, state="readonly", width=18)
                combo.grid(row=row, column=1, sticky=tk.W, padx=5, pady=2)
            
                # Устанавливаем текущее значение
                current_value = current_settings.get(key, values[-1])  # По умолчанию последнее значение
                combo.set(current_value)
            
                # Описания для режимов
                descriptions = special_settings[key]["descriptions"]
            
                desc_label = ttk.Label(scrollable_frame, text=descriptions.get(current_value, ""), 
                                  font=("Arial", 8), foreground="gray")
                desc_label.grid(row=row, column=2, sticky=tk.W, padx=5, pady=2)
            
                # Обновление описания при изменении выбора
                def update_desc(event, desc_label=desc_label, descriptions=descriptions):
                    desc_label.config(text=descriptions.get(combo.get(), ""))
             
                combo.bind("<<ComboboxSelected>>", update_desc)
            
                # Сохраняем в словаре полей
                settings_entries[key] = combo
                row += 1
                continue  # Пропускаем стандартное создание текстового поля
        
            # Label
            ttk.Label(scrollable_frame, text=f"{label}:", font=("Arial", 10, "bold")).grid(
                row=row, column=0, sticky=tk.W, padx=5, pady=2
            )
        
            # Entry
            entry = ttk.Entry(scrollable_frame, width=20)
            entry.grid(row=row, column=1, sticky=tk.W, padx=5, pady=2)
            entry.insert(0, current_settings.get(key, ""))
        
            # Description
            ttk.Label(scrollable_frame, text=description, font=("Arial", 8), foreground="gray").grid(
                row=row, column=2, sticky=tk.W, padx=5, pady=2
            )
         
            settings_entries[key] = entry
            row += 1
    
        # Добавляем разделитель
        ttk.Separator(scrollable_frame, orient=tk.HORIZONTAL).grid(
            row=row, column=0, columnspan=3, sticky=tk.EW, pady=10
        )
        row += 1
    
        # Кнопка сохранения (в отдельном фрейме внизу окна)
        button_frame = ttk.Frame(settings_window)
        button_frame.pack(fill=tk.X, pady=10)
    
        save_button = ttk.Button(
            button_frame, 
            text=self.get_translation("save_settings_btn"),
            command=lambda: self.save_settings(settings_entries, settings_window)
        )
        save_button.pack(padx=10, pady=5)
    
    def load_settings_dict(self):
        """Load settings from settings.txt into a dictionary"""
        settings = {}
        
        try:
            with open(self.file_paths["settings"], 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        key, value = line.split('=', 1)
                        settings[key] = value
            
            self.update_status("Settings", self.get_translation("loaded"), "green")
        except Exception as e:
            self.gui_logger.log("ERROR", f"Error loading settings: {str(e)}")
            self.update_status("Settings", self.get_translation("error"), "red")
        
        return settings
    
    def save_settings(self, entries, window):
        """Save settings to the settings.txt file"""
        try:
            with open(self.file_paths["settings"], 'w', encoding='utf-8') as f:
                for key, entry in entries.items():
                    if hasattr(entry, 'get'):  # Для текстовых полей и выпадающих списков
                        value = entry.get()
                        f.write(f"{key}={value}\n")
            
            self.gui_logger.log("INFO", self.get_translation("settings_saved_successfully"))
            self.update_status("Settings", self.get_translation("saved"), "green")
            
            # Close the settings window
            window.destroy()
            
        except Exception as e:
            error_msg = self.get_translation("error_saving_file").format(self.file_paths["settings"], str(e))
            self.gui_logger.log("ERROR", error_msg)
            
            # Log detailed error to debug.txt
            with open("debug.txt", "a") as f:
                f.write(f"=== SETTINGS ERROR AT {datetime.now()} ===\n")
                f.write(error_msg + "\n")
                f.write(traceback.format_exc() + "\n\n")
            
            messagebox.showerror(self.get_translation("error"), error_msg)
    
    def open_file_paths(self):
        """Open the file paths editor window"""
        paths_window = tk.Toplevel(self.root)
        paths_window.title(self.get_translation("file_paths_window_title"))
        paths_window.geometry("700x500")
        paths_window.configure(bg=self.colors["bg"])
        
        # Create a frame for the paths
        frame = ttk.Frame(paths_window, padding=10)
        frame.pack(fill=tk.BOTH, expand=True)
        
        # File paths with descriptions
        path_info = {
            "keywords": (self.get_translation("keywords_file_label"), self.get_translation("keywords_file_desc")),
            "proxy": (self.get_translation("proxy_file_label"), self.get_translation("proxy_file_desc")),
            "settings": (self.get_translation("settings_file_label"), self.get_translation("settings_file_desc")),
            "blacklist": (self.get_translation("blacklist_file_label"), self.get_translation("blacklist_file_desc")),
            "api": (self.get_translation("api_file_label"), self.get_translation("api_file_desc")),
            "channels": (self.get_translation("channels_file_label"), self.get_translation("channels_file_desc")),
            "emails": (self.get_translation("emails_file_label"), self.get_translation("emails_file_desc")),
            "social_media": (self.get_translation("social_media_file_label"), self.get_translation("social_media_file_desc"))
        }
        
        # Create entries for each file path
        path_entries = {}
        row = 0
        
        for key, (label, description) in path_info.items():
            # Label
            ttk.Label(frame, text=f"{label}:", font=("Arial", 10, "bold")).grid(
                row=row, column=0, sticky=tk.W, padx=5, pady=2
            )
            
            # Entry
            entry = ttk.Entry(frame, width=40)
            entry.grid(row=row, column=1, sticky=tk.W, padx=5, pady=2)
            entry.insert(0, self.file_paths.get(key, ""))
            
            # Browse button
            browse_button = ttk.Button(
                frame,
                text=self.get_translation("browse_btn"),
                command=lambda e=entry: self.browse_file(e)
            )
            browse_button.grid(row=row, column=2, padx=5, pady=2)
            
            # Description
            ttk.Label(frame, text=description, font=("Arial", 8), foreground="gray").grid(
                row=row, column=3, sticky=tk.W, padx=5, pady=2
            )
            
            path_entries[key] = entry
            row += 1
        
        # Add a spacer
        ttk.Separator(frame, orient=tk.HORIZONTAL).grid(
            row=row, column=0, columnspan=4, sticky=tk.EW, pady=10
        )
        row += 1
        
        # Save button
        save_button = ttk.Button(
            frame, 
            text=self.get_translation("save_file_paths_btn"),
            command=lambda: self.save_file_paths(path_entries, paths_window)
        )
        save_button.grid(row=row, column=0, columnspan=4, pady=10)
    
    def browse_file(self, entry_widget):
        """Open a file browser and update the entry widget with the selected file path"""
        file_path = filedialog.asksaveasfilename(defaultextension=".txt")
        if file_path:
            entry_widget.delete(0, tk.END)
            entry_widget.insert(0, file_path)
    
    def save_file_paths(self, entries, window):
        """Save the file paths and close the window"""
        try:
            # Update the file paths dictionary
            for key, entry in entries.items():
                self.file_paths[key] = entry.get()
            
            # Save file paths to a JSON file for persistence
            with open("file_paths.json", 'w', encoding='utf-8') as f:
                json.dump(self.file_paths, f, indent=4)
            
            self.gui_logger.log("INFO", self.get_translation("file_paths_saved_successfully"))
            
            # Close the window
            window.destroy()
            
        except Exception as e:
            error_msg = self.get_translation("error_loading_file_paths").format(str(e))
            self.gui_logger.log("ERROR", error_msg)
            
            with open("debug.txt", "a") as f:
                f.write(f"=== FILE PATHS ERROR AT {datetime.now()} ===\n")
                f.write(error_msg + "\n")
                f.write(traceback.format_exc() + "\n\n")
            
            messagebox.showerror(self.get_translation("error"), error_msg)
    
    def load_file_paths(self):
        """Load file paths from file_paths.json if it exists"""
        try:
            if os.path.exists("file_paths.json"):
                with open("file_paths.json", 'r', encoding='utf-8') as f:
                    saved_paths = json.load(f)
                    
                    # Update the file paths dictionary
                    for key, path in saved_paths.items():
                        if key in self.file_paths:
                            self.file_paths[key] = path
                
                self.gui_logger.log("INFO", self.get_translation("file_paths_loaded_successfully"))
        except Exception as e:
            self.gui_logger.log("ERROR", self.get_translation("error_loading_file_paths").format(str(e)))
            
            with open("debug.txt", "a") as f:
                f.write(f"=== FILE PATHS LOAD ERROR AT {datetime.now()} ===\n")
                f.write(f"Error loading file paths: {str(e)}\n")
                f.write(traceback.format_exc() + "\n\n")
    
    def open_file_editor(self, file_key):
        """Open a simple editor for the specified file"""
        if file_key not in self.file_paths:
            messagebox.showerror(self.get_translation("error"), f"Unknown file type: {file_key}")
            return
        
        file_path = self.file_paths[file_key]
        
        # Create a new window
        editor_window = tk.Toplevel(self.root)
        editor_window.title(f"{self.get_translation('edit')} {file_key.capitalize()}")
        editor_window.geometry("800x600")
        editor_window.configure(bg=self.colors["bg"])
        
        # Create a frame for the editor
        frame = ttk.Frame(editor_window, padding=10)
        frame.pack(fill=tk.BOTH, expand=True)
        
        # Create a text widget for editing
        text_widget = scrolledtext.ScrolledText(frame, width=80, height=30)
        text_widget.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # Load the file content
        try:
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    text_widget.insert(tk.END, content)
            else:
                # Create file with default content
                default_content = ""
                if file_key == "keywords":
                    default_content = "music\ngaming\ntutorial\ntech\nvlog\n"
                elif file_key == "proxy":
                    default_content = "# Format: ip:port:login:password\n"
                elif file_key == "settings":
                    default_content = "min_subscribers=1000\nmax_subscribers=1000000\n"
                    default_content += "min_total_views=10000\ncreation_year_limit=2015\n"
                    default_content += "delay_min=0.5\ndelay_max=2\nparse_mode=email\n"
                    default_content += "max_workers=5\nbatch_size=50\nuse_caching=true\n"
                    default_content += "shorts_filter_mode=3\n"
                elif file_key == "blacklist":
                    default_content = "IN\nBR\nPK\n"
                elif file_key == "api":
                    default_content = "# Enter your YouTube API keys here, one per line\n"
                
                text_widget.insert(tk.END, default_content)
                
        except Exception as e:
            self.gui_logger.log("ERROR", self.get_translation("error_loading_file").format(file_path, str(e)))
            
            with open("debug.txt", "a") as f:
                f.write(f"=== FILE EDITOR ERROR AT {datetime.now()} ===\n")
                f.write(f"Error loading file {file_path}: {str(e)}\n")
                f.write(traceback.format_exc() + "\n\n")
        
        # Create save button
        save_button = ttk.Button(
            frame,
            text=self.get_translation("save"),
            command=lambda: self.save_file_content(file_path, text_widget, editor_window)
        )
        save_button.pack(pady=5)
    
    def save_file_content(self, file_path, text_widget, window):
        """Save the content of the text widget to the specified file"""
        try:
            content = text_widget.get(1.0, tk.END)
            
            # Make sure the directory exists
            os.makedirs(os.path.dirname(file_path) if os.path.dirname(file_path) else '.', exist_ok=True)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            self.gui_logger.log("INFO", self.get_translation("file_saved").format(file_path))
            
            # Get the file type from the path
            file_type = None
            for key, path in self.file_paths.items():
                if path == file_path:
                    file_type = key
                    break
            
            if file_type:
                self.update_status(file_type.capitalize(), self.get_translation("saved"), "green")
            
            # Close the window
            window.destroy()
            
        except Exception as e:
            error_msg = self.get_translation("error_saving_file").format(file_path, str(e))
            self.gui_logger.log("ERROR", error_msg)
            
            with open("debug.txt", "a") as f:
                f.write(f"=== FILE SAVE ERROR AT {datetime.now()} ===\n")
                f.write(error_msg + "\n")
                f.write(traceback.format_exc() + "\n\n")
            
            messagebox.showerror(self.get_translation("error"), error_msg)
    
    def open_file_viewer(self, file_path):
        """Open a simple viewer for the specified file"""
        # Create a new window
        viewer_window = tk.Toplevel(self.root)
        viewer_window.title(f"View {file_path}")
        viewer_window.geometry("800x600")
        viewer_window.configure(bg=self.colors["bg"])
        
        # Create a frame for the viewer
        frame = ttk.Frame(viewer_window, padding=10)
        frame.pack(fill=tk.BOTH, expand=True)
        
        # Create a text widget for viewing
        text_widget = scrolledtext.ScrolledText(frame, width=80, height=30)
        text_widget.pack(fill=tk.BOTH, expand=True, pady=5)
        
    # Load the file content
        try:
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    text_widget.insert(tk.END, content)
                text_widget.configure(state='disabled')  # Make read-only
            else:
                text_widget.insert(tk.END, self.get_translation("file_not_found").format(file_path))
                text_widget.configure(state='disabled')  # Make read-only
                
        except Exception as e:
            self.gui_logger.log("ERROR", self.get_translation("error_loading_file").format(file_path, str(e)))
            text_widget.insert(tk.END, self.get_translation("error_loading_file").format(file_path, str(e)))
            text_widget.configure(state='disabled')  # Make read-only
    
    def view_results(self):
        """View the results of the scraper"""
        # Create a new window
        results_window = tk.Toplevel(self.root)
        results_window.title(self.get_translation("results_window_title"))
        results_window.geometry("800x600")
        results_window.configure(bg=self.colors["bg"])
        
        # Create a notebook for tabbed viewing
        notebook = ttk.Notebook(results_window)
        notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Create tabs for different result types
        tab_info = [
            (self.get_translation("channels_tab"), self.file_paths["channels"]),
            (self.get_translation("emails_tab"), self.file_paths["emails"]),
            (self.get_translation("social_media_tab"), self.file_paths["social_media"])
        ]
        
        for tab_name, file_path in tab_info:
            # Create a frame for the tab
            tab_frame = ttk.Frame(notebook, padding=10)
            notebook.add(tab_frame, text=tab_name)
            
            # Add a text widget for displaying the content
            text_widget = scrolledtext.ScrolledText(tab_frame, width=80, height=30)
            text_widget.pack(fill=tk.BOTH, expand=True)
            
            # Load the file content
            try:
                if os.path.exists(file_path):
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        text_widget.insert(tk.END, content)
                else:
                    text_widget.insert(tk.END, self.get_translation("file_not_found").format(file_path))
            except Exception as e:
                text_widget.insert(tk.END, self.get_translation("error_loading_file").format(file_path, str(e)))
            
            text_widget.configure(state='disabled')  # Make read-only
            
            # Add export button for this tab
            export_button = ttk.Button(
                tab_frame,
                text=self.get_translation("export_btn").format(tab_name),
                command=lambda path=file_path: self.export_to_csv(path)
            )
            export_button.pack(pady=5)
    
    def export_to_csv(self, file_path):
        """Export the file to a CSV file"""
        if not os.path.exists(file_path):
            messagebox.showerror(self.get_translation("export_error"), 
                                 self.get_translation("file_not_found").format(file_path))
            return
        
        # Get the export path
        export_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        
        if not export_path:
            return
        
        try:
            # Simply copy the file since it's already in CSV format
            with open(file_path, 'r', encoding='utf-8') as src, open(export_path, 'w', encoding='utf-8') as dst:
                dst.write(src.read())
            
            self.gui_logger.log("INFO", self.get_translation("export_success_msg").format(export_path))
            messagebox.showinfo(self.get_translation("export_successful"), 
                               self.get_translation("export_success_msg").format(export_path))
            
        except Exception as e:
            error_msg = self.get_translation("export_error_msg").format(str(e))
            self.gui_logger.log("ERROR", error_msg)
            
            with open("debug.txt", "a") as f:
                f.write(f"=== EXPORT ERROR AT {datetime.now()} ===\n")
                f.write(error_msg + "\n")
                f.write(traceback.format_exc() + "\n\n")
            
            messagebox.showerror(self.get_translation("export_error"), error_msg)
    
    def show_about(self):
        """Show information about the application"""
        about_window = tk.Toplevel(self.root)
        about_window.title(self.get_translation("about_window_title"))
        about_window.geometry("400x300")
        about_window.configure(bg=self.colors["bg"])
        
        # Add logo or icon
        # (You can replace this with an actual logo if available)
        logo_label = ttk.Label(
            about_window, 
            text=self.get_translation("about_app_name"), 
            font=("Arial", 24, "bold"),
            foreground=self.colors["error"]
        )
        logo_label.pack(pady=20)
        
        # Add application info
        info_text = (
            f"{self.get_translation('about_app_version')}\n\n"
            f"{self.get_translation('about_app_description')}"
        )
        
        info_label = ttk.Label(
            about_window,
            text=info_text,
            justify=tk.CENTER,
            wraplength=350
        )
        info_label.pack(pady=10)
        
        # Add close button
        close_button = ttk.Button(
            about_window,
            text=self.get_translation("close_btn"),
            command=about_window.destroy
        )
        close_button.pack(pady=20)
    
    def on_closing(self):
        """Handle window close event"""
        if self.is_running:
            # If scraper is running, ask for confirmation
            if messagebox.askyesno(
                "Confirm Exit", 
                self.get_translation("closing_confirmation")
            ):
                self.stop_scraping()
                # Remove email duplicates when closing
                if hasattr(self.scraper, 'remove_email_duplicates'):
                    self.scraper.remove_email_duplicates()
                self.root.destroy()
        else:
            # Remove email duplicates when closing
            if hasattr(self.scraper, 'remove_email_duplicates'):
                self.scraper.remove_email_duplicates()
            self.root.destroy()
    
    def on_exit(self):
        """Handle exit menu item"""
        self.on_closing()

def main():
    """Main function to start the application"""
    try:
        # Create debug.txt if it doesn't exist
        if not os.path.exists("debug.txt"):
            with open("debug.txt", "w", encoding="utf-8") as f:
                f.write(f"=== DEBUG LOG CREATED AT {datetime.now()} ===\n\n")
        
        # Create root window
        root = tk.Tk()
        app = YouTubeScraperGUI(root)
        
        # Load file paths
        app.load_file_paths()
        
        # Start the application
        root.mainloop()
        
    except Exception as e:
        # If an unhandled exception occurs, log it to debug.txt
        with open("debug.txt", "a") as f:
            f.write(f"=== UNHANDLED ERROR AT {datetime.now()} ===\n")
            f.write(f"Error: {str(e)}\n")
            f.write(traceback.format_exc() + "\n\n")
        
        # Show an error message
        tk.messagebox.showerror(
            "Fatal Error",
            f"An unhandled error occurred: {str(e)}\n\nCheck debug.txt for details."
        )

if __name__ == "__main__":
    main()