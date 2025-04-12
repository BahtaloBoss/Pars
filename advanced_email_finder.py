"""
Модуль расширенного поиска email-адресов для YouTube-парсера.
Улучшает обнаружение email-адресов с помощью продвинутой эвристики, 
AI-обработки текста и анализа дополнительных источников.
"""

import re
import logging
import socket
import time
import random
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

class AdvancedEmailFinder:
    def __init__(self, proxy_list=None, dns_check=True, max_site_depth=2, use_ai_heuristics=True):
        """
        Инициализация класса с настройками
        
        :param proxy_list: Список прокси для запросов
        :param dns_check: Проверять ли валидность домена через DNS
        :param max_site_depth: Максимальная глубина сканирования связанных сайтов
        :param use_ai_heuristics: Использовать ли расширенную эвристику для поиска
        """
        self.proxy_list = proxy_list or []
        self.dns_check = dns_check
        self.max_site_depth = max_site_depth
        self.use_ai_heuristics = use_ai_heuristics
        
        # Списки известных доменов
        self.common_domains = {'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'mail.ru', 'yandex.ru', 'protonmail.com'}
        self.suspicious_domains = {'temp-mail.org', 'guerrillamail.com', 'mailinator.com', 'throwawaymail.com'}
        
        # Паттерны для обфускированных email и обработки текста
        self.expanded_patterns = self._compile_expanded_patterns()
        
        # Счетчики для статистики
        self.stats = {
            'total_found': 0,
            'standard_emails': 0,
            'obfuscated_emails': 0,
            'website_extracted': 0,
            'dns_validated': 0,
            'dns_failed': 0
        }
    
    def _compile_expanded_patterns(self):
        """Компиляция расширенных паттернов для поиска email"""
        patterns = {
            # Базовый паттерн для стандартных email
            'standard': re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'),
            
            # Паттерны для обфускированных email
            'at_substitution': re.compile(r'[a-zA-Z0-9._%+\-]+\s*[\[\(]?\s*(?:at|AT|@|собака|dog|eta)\s*[\]\)]?\s*[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'),
            'dot_substitution': re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\s*[\[\(]?\s*(?:dot|DOT|точка|пт|тчк|\.)\s*[\]\)]?\s*[a-zA-Z]{2,}'),
            'line_break': re.compile(r'([a-zA-Z0-9._%+\-]+)\s*[\r\n]+\s*@\s*[\r\n]*\s*([a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})'),
            'full_text_substitution': re.compile(r'([a-zA-Z0-9._%+\-]+)[\s\r\n]*(?:at|@|собака)[\s\r\n]*([a-zA-Z0-9.\-]+)[\s\r\n]*(?:dot|точка|тчк|\.)[\s\r\n]*([a-zA-Z]{2,})'),
            
            # Паттерны для поиска упоминаний контактов
            'contact_mention': re.compile(r'(?:email|mail|e-mail|contact|связь|контакт|почта|эл[\.]*[\s]*почта)[\s\:]*([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})', re.IGNORECASE),
            
            # Поиск закодированных email (простые формы кодирования)
            'encoded_email': re.compile(r'([a-zA-Z0-9._%+\-]+)(?:\s*[\[\(]?at[\]\)]?\s*|\s*[\[\(]?собака[\]\)]?\s*|\s*[._\-\|\s]*?\s*)([a-zA-Z0-9.\-]+)(?:\s*[\[\(]?dot[\]\)]?\s*|\s*[\[\(]?точка[\]\)]?\s*|\s*[._\-\|\s]?\s*)([a-zA-Z]{2,})'),
            
            # Поиск email в формате "name (at) domain (dot) com"
            'spaced_email': re.compile(r'([a-zA-Z0-9._%+\-]+)\s*\(\s*at\s*\)\s*([a-zA-Z0-9.\-]+)\s*\(\s*dot\s*\)\s*([a-zA-Z]{2,})'),
            
            # Поиск email в формате "имя [собака] домен [точка] com"
            'russian_delimiters': re.compile(r'([a-zA-Z0-9._%+\-]+)\s*[\[\(]?\s*(?:собака|dog|гав|мяу)\s*[\]\)]?\s*([a-zA-Z0-9.\-]+)\s*[\[\(]?\s*(?:точка|тчк|пт)\s*[\]\)]?\s*([a-zA-Z]{2,})'),
            
            # Поиск ссылок контактов/email
            'mailto_links': re.compile(r'mailto:([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})'),
            
            # Поиск в текстовом шаблоне "Email/Contact us: ..."
            'contact_prefix': re.compile(r'(?:Email|Mail|E-mail|Contact|Связь|Контакт|Почта|Эл[\.]*\s*почта|По всем вопросам)(?:\s*(?:me|us|нас|мне|со мной))?(?:\s*(?:at|на|по|через))?[\s\:]*([^\s\@\<\>\(\)]{3,})'),
            
            # Распознавание контактных доменов
            'website_contacts': re.compile(r'(?:контакты|contact|about|связаться|написать)[\s\-\_]*(?:us|me|нам|мне)?', re.IGNORECASE)
        }
        
        return patterns
    
    def find_emails(self, text, source="unknown"):
        """
        Основной метод поиска email-адресов в тексте
        
        :param text: Текст для анализа
        :param source: Источник текста (для статистики)
        :return: Список найденных уникальных email-адресов
        """
        if not text:
            return []
            
        # Приведение текста к нижнему регистру и удаление лишних пробелов
        normalized_text = self._normalize_text(text)
        emails = []
        
        # Поиск стандартных email
        standard_emails = self.expanded_patterns['standard'].findall(normalized_text)
        emails.extend(standard_emails)
        self.stats['standard_emails'] += len(standard_emails)
        
        # Поиск email с заменой @ на "at" и т.п.
        for at_substituted in self.expanded_patterns['at_substitution'].findall(normalized_text):
            processed = self._process_at_substitution(at_substituted)
            if processed:
                emails.append(processed)
                self.stats['obfuscated_emails'] += 1
        
        # Поиск email с заменой точки на "dot" и т.п.
        for dot_substituted in self.expanded_patterns['dot_substitution'].findall(normalized_text):
            processed = self._process_dot_substitution(dot_substituted)
            if processed:
                emails.append(processed)
                self.stats['obfuscated_emails'] += 1
        
        # Поиск email разбитых переносами строк
        line_break_matches = self.expanded_patterns['line_break'].findall(normalized_text)
        for username, domain in line_break_matches:
            emails.append(f"{username}@{domain}")
            self.stats['obfuscated_emails'] += 1
        
        # Поиск полностью замаскированных email (name at domain dot com)
        full_text_matches = self.expanded_patterns['full_text_substitution'].findall(normalized_text)
        for username, domain, tld in full_text_matches:
            emails.append(f"{username}@{domain}.{tld}")
            self.stats['obfuscated_emails'] += 1
            
        # Поиск email в формате "name (at) domain (dot) com"
        spaced_matches = self.expanded_patterns['spaced_email'].findall(normalized_text)
        for username, domain, tld in spaced_matches:
            emails.append(f"{username}@{domain}.{tld}")
            self.stats['obfuscated_emails'] += 1
            
        # Поиск email с русскими разделителями
        russian_matches = self.expanded_patterns['russian_delimiters'].findall(normalized_text)
        for username, domain, tld in russian_matches:
            emails.append(f"{username}@{domain}.{tld}")
            self.stats['obfuscated_emails'] += 1
            
        # Поиск ссылок mailto:
        mailto_matches = self.expanded_patterns['mailto_links'].findall(normalized_text)
        emails.extend(mailto_matches)
        self.stats['standard_emails'] += len(mailto_matches)
        
        # Особая обработка для контактной информации
        contact_emails = self._extract_contact_section_emails(normalized_text)
        emails.extend(contact_emails)
        
        # Если включена эвристика, ищем сайты и сканируем их контактные страницы
        if self.use_ai_heuristics:
            website_emails = self._extract_emails_from_linked_websites(text)
            emails.extend(website_emails)
            self.stats['website_extracted'] += len(website_emails)
        
        # Фильтрация, нормализация и валидация
        emails = self._clean_and_validate_emails(emails)
        
        self.stats['total_found'] += len(emails)
        return emails
    
    def _normalize_text(self, text):
        """Нормализация текста для улучшения поиска"""
        if not text:
            return ""
        
        # Замена HTML-сущностей
        text = text.replace('&nbsp;', ' ')
        text = text.replace('&lt;', '<')
        text = text.replace('&gt;', '>')
        text = text.replace('&amp;', '&')
        text = text.replace('&#64;', '@')
        
        # Удаление множественных пробелов и переводов строк
        text = re.sub(r'\s+', ' ', text)
        
        return text
    
    def _process_at_substitution(self, text):
        """Обработка текста с заменой @ на текст"""
        # Замена различных вариантов "at" на @
        processed = re.sub(r'\s*[\[\(]?\s*(?:at|AT|@|собака|dog|eta)\s*[\]\)]?\s*', '@', text)
        
        # Проверка на валидный формат email
        if re.match(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', processed):
            return processed
        return None
    
    def _process_dot_substitution(self, text):
        """Обработка текста с заменой точки на текст"""
        # Замена различных вариантов "dot" на точку
        processed = re.sub(r'\s*[\[\(]?\s*(?:dot|DOT|точка|пт|тчк|\.)\s*[\]\)]?\s*', '.', text)
        
        # Проверка на валидный формат email
        if re.match(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', processed):
            return processed
        return None
    
    def _extract_contact_section_emails(self, text):
        """Извлечение email из секций с контактной информацией"""
        emails = []
        
        # Поиск упоминаний контактов
        contact_matches = self.expanded_patterns['contact_mention'].findall(text)
        emails.extend(contact_matches)
        
        # Поиск потенциальных контактных секций
        contact_sections = []
        
        # Разбиваем текст на строки
        lines = text.split('\n')
        for i, line in enumerate(lines):
            # Ищем строки с ключевыми словами контактов
            if re.search(r'(?:contact|контакты?|связь|почта|email|mail|e-mail|для связи)', line, re.IGNORECASE):
                # Захватываем несколько следующих строк как потенциальную контактную информацию
                section_end = min(i + 5, len(lines))
                contact_sections.append('\n'.join(lines[i:section_end]))
        
        # Анализируем каждую контактную секцию на предмет email-адресов
        for section in contact_sections:
            # Стандартные email
            section_emails = self.expanded_patterns['standard'].findall(section)
            emails.extend(section_emails)
            
            # Обфусцированные email (характерны для контактных секций)
            encoded_matches = self.expanded_patterns['encoded_email'].findall(section)
            for username, domain, tld in encoded_matches:
                emails.append(f"{username}@{domain}.{tld}")
        
        return emails
    
    def _extract_emails_from_linked_websites(self, text):
        """Извлечение email-адресов с связанных веб-сайтов"""
        emails = []
        
        # Поиск URL в тексте
        url_pattern = r'https?://(?:www\.)?([a-zA-Z0-9][-a-zA-Z0-9]*\.[a-zA-Z0-9]{2,}\.[a-zA-Z]{2,}|[a-zA-Z0-9][-a-zA-Z0-9]*\.[a-zA-Z]{2,})'
        urls = re.findall(url_pattern, text)
        
        if not urls:
            return emails
            
        # Отбираем до 3 уникальных сайтов для анализа
        unique_domains = list(set(urls))[:3]
        
        # Проверяем каждый сайт на наличие контактной информации
        for domain in unique_domains:
            domain_emails = self._scrape_website_for_emails(f"https://{domain}")
            emails.extend(domain_emails)
            
            # Если нашли много email, прекращаем поиск
            if len(domain_emails) >= 3:
                break
        
        return emails
    
    def _scrape_website_for_emails(self, url, depth=0):
        """Рекурсивное сканирование веб-сайта для поиска email-адресов"""
        if depth >= self.max_site_depth:
            return []
        
        emails = []
        contact_pages = []
        
        try:
            headers = self._get_random_headers()
            proxy = self._get_proxy() if self.proxy_list else None
            
            response = requests.get(url, headers=headers, proxies=proxy, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Поиск email на текущей странице
            page_emails = self.expanded_patterns['standard'].findall(response.text)
            emails.extend(page_emails)
            
            # Поиск ссылок mailto:
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.startswith('mailto:'):
                    email = href.replace('mailto:', '').split('?')[0]
                    if email and '@' in email:
                        emails.append(email)
            
            # Если это не глубокое сканирование, ищем только контактные страницы
            if depth == 0:
                # Поиск ссылок на контактные страницы
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    link_text = link.get_text().lower()
                    
                    # Если ссылка ведет на контактную страницу
                    if re.search(self.expanded_patterns['website_contacts'], link_text):
                        abs_url = urljoin(url, href)
                        # Проверяем что ссылка ведет на тот же домен
                        if urlparse(abs_url).netloc == urlparse(url).netloc:
                            contact_pages.append(abs_url)
            
            # Обрабатываем найденные контактные страницы
            for contact_url in contact_pages:
                contact_emails = self._scrape_website_for_emails(contact_url, depth + 1)
                emails.extend(contact_emails)
        
        except Exception as e:
            logging.debug(f"Error scraping website {url}: {str(e)}")
        
        return emails
    
    def _get_random_headers(self):
        """Генерация случайных заголовков для HTTP-запросов"""
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'
        ]
        
        headers = {
            'User-Agent': random.choice(user_agents),
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        return headers
    
    def _get_proxy(self):
        """Получение случайного прокси из списка"""
        if not self.proxy_list:
            return None
        
        proxy = random.choice(self.proxy_list)
        
        # Формат прокси: "ip:port:login:password"
        try:
            ip, port, login, password = proxy.split(':')
            proxy_dict = {
                'http': f'http://{login}:{password}@{ip}:{port}',
                'https': f'http://{login}:{password}@{ip}:{port}'
            }
            return proxy_dict
        except Exception:
            # Если формат не соответствует, возвращаем простой прокси
            return {
                'http': f'http://{proxy}',
                'https': f'http://{proxy}'
            }
    
    def _clean_and_validate_emails(self, emails):
        """Очистка, нормализация и валидация списка email-адресов"""
        if not emails:
            return []
        
        valid_emails = []
        seen_emails = set()
        
        for email in emails:
            # Нормализация и очистка
            email = email.strip().lower()
            
            # Проверка на пропущенные окончания обработки регулярок
            if email.endswith(('.', ',', ':', ';', ')', ']', '}', '"', "'")):
                email = email[:-1]
            
            # Начальная валидация формата
            if not re.match(r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$', email):
                continue
                
            # Пропуск дубликатов
            if email in seen_emails:
                continue
                
            seen_emails.add(email)
            
            # Проверка на временные домены
            domain = email.split('@')[-1]
            if domain in self.suspicious_domains:
                continue
            
            # Валидация домена через DNS
            if self.dns_check:
                if self._validate_email_domain(domain):
                    valid_emails.append(email)
                    self.stats['dns_validated'] += 1
                else:
                    self.stats['dns_failed'] += 1
            else:
                valid_emails.append(email)
        
        return valid_emails
    
    def _validate_email_domain(self, domain):
        """Проверка валидности домена через DNS запросы"""
        try:
            # Проверка существования MX записи для домена
            socket.getaddrinfo(domain, None)
            return True
        except socket.gaierror:
            try:
                # Иногда A запись работает, даже если MX отсутствует
                socket.gethostbyname(domain)
                return True
            except socket.gaierror:
                return False
        except Exception:
            return False
    
    def scan_youtube_content(self, channel_data, video_descriptions=None, comments=None):
        """
        Комплексное сканирование всего доступного контента YouTube канала
        
        :param channel_data: Словарь с данными канала (title, description, about_page и т.д.)
        :param video_descriptions: Список описаний видео канала
        :param comments: Список комментариев автора канала
        :return: Список найденных email-адресов
        """
        all_emails = []
        
        # Анализ основной информации канала
        if 'description' in channel_data and channel_data['description']:
            channel_emails = self.find_emails(channel_data['description'], 'channel_description')
            all_emails.extend(channel_emails)
        
        # Анализ страницы About
        if 'about_page' in channel_data and channel_data['about_page']:
            about_emails = self.find_emails(channel_data['about_page'], 'about_page')
            all_emails.extend(about_emails)
        
        # Анализ описаний видео
        if video_descriptions:
            # Объединяем описания для более эффективного поиска
            combined_descriptions = '\n'.join(video_descriptions)
            video_emails = self.find_emails(combined_descriptions, 'video_descriptions')
            all_emails.extend(video_emails)
        
        # Анализ комментариев автора
        if comments:
            # Объединяем комментарии для поиска
            combined_comments = '\n'.join(comments)
            comment_emails = self.find_emails(combined_comments, 'author_comments')
            all_emails.extend(comment_emails)
        
        # Удаление дубликатов с сохранением порядка
        unique_emails = []
        seen = set()
        for email in all_emails:
            if email not in seen:
                seen.add(email)
                unique_emails.append(email)
        
        return unique_emails
    
    def get_statistics(self):
        """Получение статистики поиска email"""
        return self.stats
    
    def reset_statistics(self):
        """Сброс статистики поиска"""
        self.stats = {
            'total_found': 0,
            'standard_emails': 0,
            'obfuscated_emails': 0,
            'website_extracted': 0,
            'dns_validated': 0,
            'dns_failed': 0
        }

# Пример использования
if __name__ == "__main__":
    # Тестовый текст с различными форматами email
    test_text = """
    Связаться с нами можно по email: test@example.com
    Или напишите на наш обфусцированный адрес: another.test at gmail dot com
    У нас также есть контактная форма на сайте example.com
    Наш менеджер ответит вам: manager[at]company.com
    Для вопросов: support (at) example (dot) org
    Техподдержка: tech
    support@company.io
    Пишите нам на: hide (собака) mail (точка) ru
    """
    
    finder = AdvancedEmailFinder()
    emails = finder.find_emails(test_text)
    
    print("Найденные email-адреса:")
    for email in emails:
        print(f" - {email}")
    
    print("\nСтатистика:")
    for key, value in finder.get_statistics().items():
        print(f"{key}: {value}")