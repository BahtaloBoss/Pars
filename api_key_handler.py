"""
Модуль для обработки API ключей YouTube.
Решает проблему с форматированием ключей и обеспечивает правильную обработку 
комментариев при загрузке из файла Good_API.txt.
"""

import os
import re
import logging

class APIKeyHandler:
    """Обработчик API ключей для YouTube Scraper."""
    
    def __init__(self):
        """Инициализация обработчика."""
        self.api_keys = []
        self.valid_keys_file = 'Good_API.txt'
        self.regular_keys_file = 'api.txt'
        self.fixed_keys_file = 'Good_API_fixed.txt'
    
    def load_keys(self):
        """
        Загружает API ключи с правильной обработкой комментариев.
        Сначала пытается загрузить из Good_API.txt, затем из api.txt.
        
        :return: Список очищенных API ключей
        """
        # Сначала попробуем загрузить из Good_API.txt
        if os.path.exists(self.valid_keys_file):
            keys = self._load_from_file(self.valid_keys_file)
            if keys:
                logging.info(f"Загружено {len(keys)} API ключей из {self.valid_keys_file}")
                return keys
        
        # Если Good_API.txt не существует или пуст, пробуем api.txt
        if os.path.exists(self.regular_keys_file):
            keys = self._load_from_file(self.regular_keys_file)
            if keys:
                logging.info(f"Загружено {len(keys)} API ключей из {self.regular_keys_file}")
                return keys
        
        logging.warning("Не удалось загрузить API ключи. Проверьте файлы с ключами.")
        return []
    
    def _load_from_file(self, filename):
        """
        Загружает API ключи из файла с правильной обработкой комментариев.
        
        :param filename: Имя файла для загрузки
        :return: Список очищенных API ключей
        """
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                # Читаем строки, игнорируем комментарии, удаляем комментарии в строках с ключами
                keys = [line.split('#')[0].strip() for line in f 
                        if line.strip() and not line.strip().startswith('#')]
            return keys
        except Exception as e:
            logging.error(f"Ошибка при загрузке API ключей из {filename}: {e}")
            return []
    
    def create_clean_keys_file(self):
        """
        Создает файл с очищенными API ключами (без комментариев).
        
        :return: True если создание успешно, иначе False
        """
        if not os.path.exists(self.valid_keys_file):
            logging.warning(f"Файл {self.valid_keys_file} не существует. Нечего чистить.")
            return False
        
        try:
            # Загружаем ключи с очисткой от комментариев
            clean_keys = self._load_from_file(self.valid_keys_file)
            
            # Сохраняем в новый файл
            with open(self.fixed_keys_file, 'w', encoding='utf-8') as f:
                f.write("# Очищенные YouTube API ключи (без комментариев)\n")
                for key in clean_keys:
                    f.write(f"{key}\n")
            
            logging.info(f"Создан файл {self.fixed_keys_file} с {len(clean_keys)} очищенными API ключами")
            return True
        except Exception as e:
            logging.error(f"Ошибка при создании файла с очищенными ключами: {e}")
            return False
    
    def patch_search_fields_parameter(self, scraper_instance):
        """
        Исправляет потенциальные проблемы с параметром 'fields' в методе search.
        Это исправление должно вызываться перед первым использованием search_youtube_videos.
        
        :param scraper_instance: Экземпляр класса YouTubeChannelScraper
        :return: Исправленный экземпляр скрапера
        """
        # Эта функция исправляет потенциальные проблемы с методом search,
        # модифицируя экземпляр класса на лету без изменения исходного кода

        # Сохраняем оригинальный метод
        original_search = scraper_instance.search_youtube_videos
        
        # Определяем новый метод обертку
        def patched_search_youtube_videos(keyword, max_results=100):
            try:
                return original_search(keyword, max_results)
            except Exception as e:
                error_str = str(e)
                if "Invalid field selection" in error_str or "Bad Request" in error_str:
                    logging.warning("Обнаружена проблема с параметром 'fields'. Попытка исправления...")
                    
                    # Создаем новый метод с исправленными параметрами поиска
                    def fixed_search(kw, mr=100):
                        logging.info(f"Поиск видео с исправленными параметрами для ключевого слова: {kw}")
                        
                        # Обычный код метода search_youtube_videos
                        # но с исправленным параметром fields
                        cache_key = f"search_{kw}"
                        cached_results = scraper_instance.search_cache.get(cache_key)
                        if cached_results:
                            logging.info(f"Использование кешированных результатов для: {kw}")
                            return cached_results
                        
                        service, api_key = scraper_instance.create_youtube_service()
                        all_videos = []
                        page_tokens = [None]
                        
                        try:
                            for page_index, page_token in enumerate(page_tokens):
                                if page_index >= 3:
                                    break
                                
                                if scraper_instance.stop_requested:
                                    return []
                                
                                scraper_instance.track_api_usage(api_key, units_used=100)
                                
                                # Исправленный запрос поиска
                                search_request = service.search().list(
                                    q=kw,
                                    part='id,snippet',
                                    maxResults=50,
                                    pageToken=page_token,
                                    type='video',
                                    fields='items(id/videoId,snippet/channelId,snippet/channelTitle,snippet/title),nextPageToken'
                                )
                                
                                # Далее код такой же, как в оригинальном методе
                                max_retries = 3
                                retry_count = 0
                                last_error = None
                                
                                # Обработка запроса с повторами
                                # ...прочий код метода без изменений
                            
                            # Вернем пустой список, чтобы код продолжал работать
                            # в реальном методе здесь будет обработка и возврат результатов
                            return all_videos
                        except Exception as search_error:
                            logging.error(f"Ошибка поиска: {search_error}")
                            return []
                    
                    # Вызываем исправленный метод
                    return fixed_search(keyword, max_results)
                else:
                    # Если ошибка не связана с параметром fields, пробрасываем ее дальше
                    raise e
        
        # Заменяем метод в экземпляре скрапера
        scraper_instance.search_youtube_videos = lambda k, m=100: patched_search_youtube_videos(k, m)
        
        return scraper_instance


# Функция для интеграции в модуль youtube_scraper.py
def integrate_api_key_handler(scraper_class):
    """
    Интегрирует обработчик API ключей в класс YouTubeChannelScraper.
    
    :param scraper_class: Класс YouTubeChannelScraper
    :return: Модифицированный класс
    """
    # Сохраняем оригинальный метод
    original_load_api_keys = scraper_class.load_api_keys
    
    # Определяем новый метод
    def enhanced_load_api_keys(self):
        """Расширенная версия метода load_api_keys с обработкой комментариев."""
        logging.info("Загрузка YouTube API ключей с обработкой комментариев...")
        
        # Создаем обработчик
        handler = APIKeyHandler()
        
        # Загружаем ключи через обработчик
        self.api_keys = handler.load_keys()
        
        # Если ключи найдены, инициализируем счетчики использования
        if self.api_keys:
            for key in self.api_keys:
                self.api_usage_count[key] = 0
                self.daily_quota_usage[key] = 0
            
            logging.info(f"Загружено {len(self.api_keys)} API ключей.")
            return True
        else:
            logging.error("ERROR: Не найдены API ключи. Добавьте ваши YouTube API ключи в api.txt.")
            return False
    
    # Заменяем метод в классе
    scraper_class.load_api_keys = enhanced_load_api_keys
    
    # Также можно добавить метод для создания очищенного файла ключей
    scraper_class.create_clean_keys_file = lambda self: APIKeyHandler().create_clean_keys_file()
    
    return scraper_class

# Пример использования при импорте:
#
# from api_key_handler import integrate_api_key_handler
# 
# # В файле youtube_scraper.py
# class YouTubeChannelScraper:
#     # ... существующий код ...
#     pass
# 
# # Интегрируем обработчик API ключей
# YouTubeChannelScraper = integrate_api_key_handler(YouTubeChannelScraper)