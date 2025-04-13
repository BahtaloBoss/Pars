"""
This file contains the YouTubeChannelScraper class extracted from the original script.
It is used by the GUI application to perform the scraping operations.
Includes optimizations for caching, API usage, threading, and error handling.
"""

import os
import re
import time
import json
import random
import requests
import logging
import datetime
import traceback
import uuid
from collections import OrderedDict
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from bs4 import BeautifulSoup
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from advanced_email_finder import AdvancedEmailFinder
from api_key_handler import integrate_api_key_handler

class LRUCache:
    """Limited size cache with Least Recently Used eviction policy."""
    def __init__(self, max_size=1000):
        self.cache = OrderedDict()
        self.max_size = max_size
        self.lock = threading.RLock()  # Thread-safe operations
        
    def get(self, key, default=None):
        with self.lock:
            if key in self.cache:
                # Move to end (most recently used)
                self.cache.move_to_end(key)
                return self.cache[key]
            return default
        
    def put(self, key, value):
        with self.lock:
            if key in self.cache:
                # Move to end (most recently used)
                self.cache.move_to_end(key)
            elif len(self.cache) >= self.max_size:
                # Remove least recently used item
                self.cache.popitem(last=False)
            self.cache[key] = value
    
    def __contains__(self, key):
        with self.lock:
            return key in self.cache
            
    def __len__(self):
        with self.lock:
            return len(self.cache)
            
    def keys(self):
        with self.lock:
            return list(self.cache.keys())
            
    def values(self):
        with self.lock:
            return list(self.cache.values())
            
    def items(self):
        with self.lock:
            return list(self.cache.items())
            
    def clear(self):
        with self.lock:
            self.cache.clear()

class PrioritizedThreadPoolExecutor(ThreadPoolExecutor):
    """Thread pool executor with task prioritization."""
    def __init__(self, max_workers):
        super().__init__(max_workers=max_workers)
        self.tasks = []
        self.tasks_lock = threading.Lock()
        
    def submit(self, fn, *args, priority=5, **kwargs):
        """Submit a task with priority (lower number = higher priority)."""
        future = Future()
        
        def task_wrapper():
            if not future.cancelled():
                try:
                    result = fn(*args, **kwargs)
                    future.set_result(result)
                except Exception as exc:
                    future.set_exception(exc)
        
        with self.tasks_lock:
            self.tasks.append((priority, future, self._submit(task_wrapper)))
            # Sort by priority (lower first)
            self.tasks.sort(key=lambda x: x[0])
        
        return future

class YouTubeChannelScraper:
    def __init__(self):
        self.settings = {}
        self.keywords = []
        self.blacklist_countries = []
        self.proxies = []
        self.api_keys = []
        self.current_api_key_index = 0
        self.parsed_channels = set()
        self.parsed_emails = set()
        self.parsed_social_media = set()
        self.required_files = ['keywords.txt', 'proxy.txt', 'settings.txt', 'blacklist.txt', 'api.txt']
        self.api_usage_count = {}      # Track usage of each API key
        self.api_last_used = {}        # Track when each key was last used
        self.api_errors = {}           # Track errors per API key
        self.batch_size = 50           # Default batch size for group requests
        
        # Caches with size limits
        self.channel_cache = LRUCache(max_size=1000)
        self.video_tags_cache = LRUCache(max_size=2000)
        self.about_page_cache = LRUCache(max_size=500)
        self.search_cache = LRUCache(max_size=100)
        
        self.max_workers = 5          # Default maximum number of worker threads
        self.all_keywords = []
        self.processed_keywords = set()
        self.stop_requested = False
        self.min_api_cooldown = 2      # Minimum seconds between API key usages
        
        # For email similarity detection
        self.email_fingerprints = {}   # For storing normalized forms of emails
        self.similarity_threshold = 0.85  # Similarity threshold (0.0 to 1.0)
        self.email_domains = {}        # Track email domains for statistics
        
        # Daily quota tracking
        self.daily_quota_usage = {}    # Track {api_key: quota_used_today}
        self.daily_quota_limit = 10000 # Default YouTube API daily quota
        self.last_quota_reset_day = datetime.datetime.now().date().isoformat()
        
        # For advanced email finder integration
        self.advanced_email_finder = None  # Will be initialized after loading settings
        
        # Compile email and social media patterns for better performance
        self._compile_patterns()
        
    def _compile_patterns(self):
        """Pre-compile regex patterns for better performance."""
        # Email patterns
        self.email_patterns = {
            'standard': re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'),
            'obfuscated_at': re.compile(r'[a-zA-Z0-9._%+-]+\s*(?:at|AT|\(at\)|\[at\]|@)\s*[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', re.IGNORECASE),
            'obfuscated_dot': re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\s*(?:dot|DOT|\(dot\)|\[dot\]|\.)\s*[a-zA-Z]{2,}', re.IGNORECASE),
            'broken': re.compile(r'([a-zA-Z0-9._%+-]+)\s*@\s*([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'),
            'contact_section': re.compile(r'(?:Email|Contact|E-mail|Mail)[\s\-:]*\s*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', re.IGNORECASE),
            'full_text_sub': re.compile(r'([a-zA-Z0-9._%+-]+)[\s\r\n]*(?:at|@|собака)[\s\r\n]*([a-zA-Z0-9.-]+)[\s\r\n]*(?:dot|точка|тчк|\.)[\s\r\n]*([a-zA-Z]{2,})'),
            'line_break': re.compile(r'([a-zA-Z0-9._%+-]+)\s*[\r\n]+\s*@\s*[\r\n]*\s*([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})')
        }
        
        # Social media patterns
        self.social_patterns = {
            'facebook': [
                re.compile(r'https?://(?:www\.)?facebook\.com/[a-zA-Z0-9._%+-]+'),
                re.compile(r'https?://(?:www\.)?fb\.com/[a-zA-Z0-9._%+-]+'),
                re.compile(r'facebook\.com/[a-zA-Z0-9._%+-]+'),
                re.compile(r'fb\.com/[a-zA-Z0-9._%+-]+')
            ],
            'twitter': [
                re.compile(r'https?://(?:www\.)?twitter\.com/[a-zA-Z0-9_]+'),
                re.compile(r'https?://(?:www\.)?x\.com/[a-zA-Z0-9_]+'),
                re.compile(r'twitter\.com/[a-zA-Z0-9_]+'),
                re.compile(r'x\.com/[a-zA-Z0-9_]+')
            ],
            'instagram': [
                re.compile(r'https?://(?:www\.)?instagram\.com/[a-zA-Z0-9_.]+'),
                re.compile(r'https?://(?:www\.)?instagr\.am/[a-zA-Z0-9_.]+'),
                re.compile(r'instagram\.com/[a-zA-Z0-9_.]+'),
                re.compile(r'instagr\.am/[a-zA-Z0-9_.]+')
            ],
            'linkedin': [
                re.compile(r'https?://(?:www\.)?linkedin\.com/in/[a-zA-Z0-9_-]+'),
                re.compile(r'https?://(?:www\.)?linkedin\.com/company/[a-zA-Z0-9_-]+'),
                re.compile(r'linkedin\.com/in/[a-zA-Z0-9_-]+'),
                re.compile(r'linkedin\.com/company/[a-zA-Z0-9_-]+')
            ],
            'telegram': [
                re.compile(r'https?://(?:www\.)?t\.me/[a-zA-Z0-9_]+'),
                re.compile(r'https?://(?:www\.)?telegram\.me/[a-zA-Z0-9_]+'),
                re.compile(r't\.me/[a-zA-Z0-9_]+'),
                re.compile(r'telegram\.me/[a-zA-Z0-9_]+')
            ],
            'youtube': [
                re.compile(r'https?://(?:www\.)?youtube\.com/@[a-zA-Z0-9_-]+'),
                re.compile(r'https?://(?:www\.)?youtube\.com/c/[a-zA-Z0-9_-]+'),
                re.compile(r'youtube\.com/@[a-zA-Z0-9_-]+'),
                re.compile(r'youtube\.com/c/[a-zA-Z0-9_-]+')
            ],
            'generic': [
                re.compile(r'\.com/(?:user|profile|u|channel)/[a-zA-Z0-9_-]{3,30}')
            ]
        }
        
        # Social media handle patterns
        self.social_handle_patterns = {
            'instagram': re.compile(r'(?:instagram|ig)[\s:]+[@]?([a-zA-Z0-9._]{3,30})\b', re.IGNORECASE),
            'twitter': re.compile(r'(?:twitter|x|tweet)[\s:]+[@]?([a-zA-Z0-9_]{3,30})\b', re.IGNORECASE),
            'facebook': re.compile(r'(?:facebook|fb)[\s:]+([a-zA-Z0-9.]{3,50})\b', re.IGNORECASE)
        }
        
    def load_settings(self):
        """Load settings from settings.txt file with improved error handling."""
        logging.info("Loading settings...")
        try:
            if not os.path.exists('settings.txt'):
                logging.warning("Settings file not found, creating with defaults.")
                self._create_default_settings()
                return
                
            with open('settings.txt', 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        try:
                            key, value = line.split('=', 1)
                            key = key.strip()
                            value = value.strip()
                            
                            if key in ['min_subscribers', 'max_subscribers', 'min_total_views', 'max_workers', 
                                      'batch_size', 'email_finder_max_depth']:
                                self.settings[key] = int(value)
                            elif key == 'creation_year_limit':
                                self.settings[key] = int(value)
                            elif key in ['delay_min', 'delay_max', 'min_api_cooldown', 'similarity_threshold']:
                                self.settings[key] = float(value)
                            elif key in ['use_caching', 'filter_similar_emails', 'use_advanced_email_finder', 
                                        'email_finder_dns_check', 'email_finder_ai_heuristics']:
                                self.settings[key] = value.lower() == 'true'
                            else:
                                self.settings[key] = value
                        except ValueError as e:
                            logging.error(f"Error parsing setting '{line}': {e}")
            
            # Apply settings to instance variables
            self.max_workers = self.settings.get('max_workers', 5)
            self.batch_size = self.settings.get('batch_size', 50)
            self.min_api_cooldown = self.settings.get('min_api_cooldown', 2.0)
            self.similarity_threshold = self.settings.get('similarity_threshold', 0.85)
            
            # Initialize AdvancedEmailFinder if enabled
            if self.settings.get('use_advanced_email_finder', False):
                try:
                    self.advanced_email_finder = AdvancedEmailFinder(
                        proxy_list=self.proxies,
                        dns_check=self.settings.get('email_finder_dns_check', True),
                        max_site_depth=self.settings.get('email_finder_max_depth', 2),
                        use_ai_heuristics=self.settings.get('email_finder_ai_heuristics', True)
                    )
                    logging.info("Advanced Email Finder initialized with settings: "
                               f"dns_check={self.settings.get('email_finder_dns_check', True)}, "
                               f"max_site_depth={self.settings.get('email_finder_max_depth', 2)}, "
                               f"use_ai_heuristics={self.settings.get('email_finder_ai_heuristics', True)}")
                except Exception as e:
                    logging.error(f"Failed to initialize Advanced Email Finder: {e}")
            
            logging.info(f"Settings loaded: {self.settings}")
        except Exception as e:
            logging.error(f"Error loading settings: {e}")
            logging.debug(traceback.format_exc())
            logging.info("Creating default settings...")
            self._create_default_settings()
    
    def _create_default_settings(self):
        """Create default settings file."""
        self.settings = {
            'min_subscribers': 1000,
            'max_subscribers': 1000000,
            'min_total_views': 10000,
            'creation_year_limit': 2015,
            'delay_min': 0.5,
            'delay_max': 2,
            'parse_mode': 'email',
            'max_workers': 5,
            'batch_size': 50,
            'use_caching': True,
            'min_api_cooldown': 2.0,     # Minimum seconds between uses of the same API key
            'smart_batching': True,       # Enable smart batching to reduce API calls
            'filter_similar_emails': True, # Enable similar email filtering
            'similarity_threshold': 0.85,  # Threshold for email similarity (0.0 to 1.0)
            'use_advanced_email_finder': False,  # Use advanced email finder
            'email_finder_max_depth': 2,   # Maximum depth for website scanning
            'email_finder_dns_check': True,  # Check email domain validity via DNS
            'email_finder_ai_heuristics': True  # Use AI heuristics for email finding
        }
        
        # Apply settings to instance variables
        self.max_workers = self.settings.get('max_workers', 5)
        self.batch_size = self.settings.get('batch_size', 50)
        
        # Write default settings to file
        try:
            with open('settings.txt', 'w', encoding='utf-8') as f:
                for key, value in self.settings.items():
                    f.write(f"{key}={value}\n")
            logging.info("Created settings.txt with default settings.")
        except Exception as e:
            logging.error(f"Failed to create default settings file: {e}")
            logging.debug(traceback.format_exc())
    
    def load_existing_data(self):
        """Load existing data to avoid duplicates with improved error handling."""
        # Load existing channels
        self._load_existing_channels()
        
        # Load existing emails
        self._load_existing_emails()
        
        # Load existing social media links
        self._load_existing_social_media()
        
    def _load_existing_channels(self):
        """Load existing channel data."""
        if os.path.exists('channels.txt'):
            try:
                with open('channels.txt', 'r', encoding='utf-8') as f:
                    next(f, None)  # Skip header
                    for line in f:
                        parts = line.strip().split(',')
                        if parts and len(parts) > 0:
                            self.parsed_channels.add(parts[0])
                logging.info(f"Loaded {len(self.parsed_channels)} existing channels.")
            except Exception as e:
                self._log_error("CHANNEL LOAD ERROR", f"Error loading existing channels: {e}")
    
    def _load_existing_emails(self):
        """Load existing email data."""
        if os.path.exists('emails.txt'):
            try:
                with open('emails.txt', 'r', encoding='utf-8') as f:
                    for line in f:
                        email = line.strip()
                        if email and not email.startswith('#'):
                            self.parsed_emails.add(email)
                logging.info(f"Loaded {len(self.parsed_emails)} existing emails.")
            except Exception as e:
                self._log_error("EMAIL LOAD ERROR", f"Error loading existing emails: {e}")
    
    def _load_existing_social_media(self):
        """Load existing social media data."""
        if os.path.exists('social_media.txt'):
            try:
                with open('social_media.txt', 'r', encoding='utf-8') as f:
                    next(f, None)  # Skip header
                    for line in f:
                        parts = line.strip().split(',')
                        if parts and len(parts) > 0:
                            self.parsed_social_media.add(parts[0])
                logging.info(f"Loaded {len(self.parsed_social_media)} existing social media links.")
            except Exception as e:
                self._log_error("SOCIAL MEDIA LOAD ERROR", f"Error loading existing social media links: {e}")
    
    def _log_error(self, error_type, message):
        """Log an error to both the logging system and debug.txt."""
        logging.error(message)
        
        try:
            with open("debug.txt", "a", encoding="utf-8") as f:
                f.write(f"=== {error_type} AT {datetime.datetime.now()} ===\n")
                f.write(f"{message}\n")
                f.write(traceback.format_exc() + "\n\n")
        except Exception as e:
            logging.error(f"Failed to write to debug.txt: {e}")
    
    def load_keywords(self):
        """Load keywords from keywords.txt file with improved error recovery."""
        logging.info("Loading keywords...")
        try:
            if not os.path.exists('keywords.txt'):
                self._create_default_keywords()
                return
                
            with open('keywords.txt', 'r', encoding='utf-8') as f:
                self.keywords = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
            
            if not self.keywords:
                logging.warning("No keywords found in keywords.txt, using defaults.")
                self._create_default_keywords()
                return
                
            logging.info(f"Loaded {len(self.keywords)} keywords.")
        except Exception as e:
            logging.error(f"Error loading keywords: {e}")
            self._create_default_keywords()
    
    def _create_default_keywords(self):
        """Create default keywords file."""
        self.keywords = ["music", "gaming", "tutorial", "tech", "vlog"]
        logging.info(f"Using default keywords: {self.keywords}")
        
        try:
            with open('keywords.txt', 'w', encoding='utf-8') as f:
                for keyword in self.keywords:
                    f.write(f"{keyword}\n")
            logging.info("Created keywords.txt with default keywords.")
        except Exception as e:
            logging.error(f"Failed to create default keywords file: {e}")
    
    def load_blacklist(self):
        """Load blacklisted countries from blacklist.txt file with improved error handling."""
        logging.info("Loading blacklisted countries...")
        try:
            if not os.path.exists('blacklist.txt'):
                self._create_default_blacklist()
                return
                
            with open('blacklist.txt', 'r', encoding='utf-8') as f:
                self.blacklist_countries = [line.strip().upper() for line in f if line.strip() and not line.strip().startswith('#')]
            
            if not self.blacklist_countries:
                logging.warning("No countries found in blacklist.txt, using defaults.")
                self._create_default_blacklist()
                return
                
            logging.info(f"Loaded {len(self.blacklist_countries)} blacklisted countries: {', '.join(self.blacklist_countries)}")
        except Exception as e:
            logging.error(f"Error loading blacklist: {e}")
            self._create_default_blacklist()
    
    def _create_default_blacklist(self):
        """Create default blacklist file."""
        self.blacklist_countries = ["IN", "BR", "PK"]
        logging.info(f"Using default blacklist: {self.blacklist_countries}")
        
        try:
            with open('blacklist.txt', 'w', encoding='utf-8') as f:
                for country in self.blacklist_countries:
                    f.write(f"{country}\n")
            logging.info("Created blacklist.txt with default countries.")
        except Exception as e:
            logging.error(f"Failed to create default blacklist file: {e}")
    
    def load_proxies(self):
        """Load proxies from proxy.txt file with improved error handling."""
        logging.info("Loading proxies...")
        try:
            if not os.path.exists('proxy.txt'):
                self._create_empty_proxy_file()
                return
                
            with open('proxy.txt', 'r', encoding='utf-8') as f:
                self.proxies = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
            
            if self.proxies:
                logging.info(f"Loaded {len(self.proxies)} proxies.")
            else:
                logging.info("No proxies loaded. Will use direct connection.")
        except Exception as e:
            logging.error(f"Error loading proxies: {e}")
            self._create_empty_proxy_file()
    
    def _create_empty_proxy_file(self):
        """Create empty proxy.txt file."""
        self.proxies = []
        logging.info("Will use direct connection.")
        
        try:
            with open('proxy.txt', 'w', encoding='utf-8') as f:
                f.write("# Format: ip:port:login:password\n")
            logging.info("Created empty proxy.txt file.")
        except Exception as e:
            logging.error(f"Failed to create proxy file: {e}")
    
    def load_api_keys(self):
        """Load YouTube API keys with improved validation and error handling."""
        logging.info("Loading YouTube API keys...")
        
        # First check for validated keys
        good_api_file = 'Good_API.txt'
        if os.path.exists(good_api_file):
            try:
                with open(good_api_file, 'r', encoding='utf-8') as f:
                    self.api_keys = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
                
                if self.api_keys:
                    logging.info(f"Loaded {len(self.api_keys)} validated API keys from {good_api_file}.")
                    
                    # Initialize usage counters
                    for key in self.api_keys:
                        self.api_usage_count[key] = 0
                        self.daily_quota_usage[key] = 0
                        
                    return True
                else:
                    logging.warning(f"No API keys found in {good_api_file}. Trying api.txt.")
            except Exception as e:
                logging.error(f"Error loading API keys from {good_api_file}: {e}")
                logging.warning("Trying api.txt instead.")
        
        # If no validated keys, try regular api.txt
        try:
            if not os.path.exists('api.txt'):
                self._create_empty_api_file()
                return False
                
            with open('api.txt', 'r', encoding='utf-8') as f:
                self.api_keys = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
            
            if not self.api_keys:
                logging.error("ERROR: No API keys found. Please add your YouTube API keys to api.txt.")
                return False
            
            # Initialize usage counters
            for key in self.api_keys:
                self.api_usage_count[key] = 0
                self.daily_quota_usage[key] = 0
                
            logging.info(f"Loaded {len(self.api_keys)} API keys from api.txt.")
            return True
        except Exception as e:
            logging.error(f"Error loading API keys: {e}")
            self._create_empty_api_file()
            return False
    
    def _create_empty_api_file(self):
        """Create empty api.txt file."""
        self.api_keys = []
        
        try:
            with open('api.txt', 'w', encoding='utf-8') as f:
                f.write("# Enter your YouTube API keys here, one per line\n")
            logging.info("Created empty api.txt file.")
        except Exception as e:
            logging.error(f"Failed to create API keys file: {e}")
            
        logging.error("ERROR: Please add your YouTube API keys to api.txt file.")
    
    def reset_daily_quota_usage(self):
        """Reset daily quota usage at the start of a new day."""
        today = datetime.datetime.now().date().isoformat()
        if self.last_quota_reset_day != today:
            self.daily_quota_usage = {key: 0 for key in self.api_keys}
            self.last_quota_reset_day = today
            logging.info(f"Reset daily quota tracking for new day: {today}")
    
    def track_api_usage(self, api_key, units_used=1):
        """Track API usage for quota management."""
        # Check if we need to reset for a new day
        self.reset_daily_quota_usage()
        
        # Track usage
        self.api_usage_count[api_key] = self.api_usage_count.get(api_key, 0) + 1
        self.daily_quota_usage[api_key] = self.daily_quota_usage.get(api_key, 0) + units_used
        
        # Log if approaching quota limit
        if self.daily_quota_usage[api_key] > self.daily_quota_limit * 0.9:
            logging.warning(f"API key {api_key[:4]}...{api_key[-4:]} is approaching daily quota limit")
            
        return self.daily_quota_usage[api_key]
    
    def get_next_api_key(self):
        """Get the next API key with improved selection based on errors, quota, and cooldown."""
        if not self.api_keys:
            raise ValueError("No API keys available.")
        
        # Get current time for cooldown calculation
        current_time = time.time()
        
        # Reset daily quota usage if needed
        self.reset_daily_quota_usage()
        
        # Filter out keys that have recently been used (respect cooldown)
        available_keys = []
        for key in self.api_keys:
            last_used = self.api_last_used.get(key, 0)
            if current_time - last_used >= self.min_api_cooldown:
                available_keys.append(key)
        
        # If no keys are available due to cooldown, wait for the first one to become available
        if not available_keys and self.api_keys:
            next_available_time = min([self.api_last_used.get(key, 0) for key in self.api_keys]) + self.min_api_cooldown
            sleep_time = max(0, next_available_time - current_time)
            if sleep_time > 0:
                logging.info(f"All API keys on cooldown, waiting {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
            return self.get_next_api_key()  # Try again
        
        # Choose key with fewest errors, least quota used, then fewest usages
        best_key = None
        min_errors = float('inf')
        min_quota_ratio = float('inf')
        min_usage = float('inf')
        
        for key in available_keys:
            errors = self.api_errors.get(key, 0)
            usages = self.api_usage_count.get(key, 0)
            
            # Calculate quota usage ratio (used / limit)
            quota_used = self.daily_quota_usage.get(key, 0)
            quota_ratio = quota_used / self.daily_quota_limit
            
            # Composite selection criteria
            if (errors < min_errors or 
                (errors == min_errors and quota_ratio < min_quota_ratio) or
                (errors == min_errors and quota_ratio == min_quota_ratio and usages < min_usage)):
                min_errors = errors
                min_quota_ratio = quota_ratio
                min_usage = usages
                best_key = key
        
        if not best_key and self.api_keys:
            # If no key is available but we have keys, use random one as fallback
            best_key = random.choice(self.api_keys)
            logging.warning(f"No optimal API key available, using random key")
        
        # Update usage statistics
        self.api_usage_count[best_key] = self.api_usage_count.get(best_key, 0) + 1
        self.api_last_used[best_key] = current_time
        
        return best_key
    
    def get_proxy(self):
        """Get a random proxy from the list with improved error handling."""
        if not self.proxies:
            return None
        
        # Select a random proxy
        proxy = random.choice(self.proxies)
        try:
            # Parse proxy string (ip:port:login:password format)
            parts = proxy.split(':')
            
            # Handle different formats
            if len(parts) == 4:  # Full format with auth
                ip, port, login, password = parts
                proxy_dict = {
                    'http': f'http://{login}:{password}@{ip}:{port}',
                    'https': f'http://{login}:{password}@{ip}:{port}'
                }
            elif len(parts) == 2:  # Simple ip:port format
                ip, port = parts
                proxy_dict = {
                    'http': f'http://{ip}:{port}',
                    'https': f'http://{ip}:{port}'
                }
            else:
                logging.warning(f"Invalid proxy format: {proxy}")
                return None
                
            return proxy_dict
        except Exception as e:
            logging.error(f"Error parsing proxy {proxy}: {e}")
            return None
    
    def get_dynamic_batch_size(self, items_count, base_batch_size=None, min_batch_size=10):
        """Calculate optimal batch size based on remaining quota and items count."""
        if base_batch_size is None:
            base_batch_size = self.batch_size
            
        # If we have very few items, just use them directly
        if items_count <= min_batch_size:
            return items_count
            
        # Get current API key
        api_key = self.get_next_api_key()
        
        # Estimate remaining quota
        used_quota = self.daily_quota_usage.get(api_key, 0)
        remaining_quota = max(0, self.daily_quota_limit - used_quota)
        
        # If quota is very low, use smaller batches
        if remaining_quota < 100:
            return min_batch_size
        
        # Calculate batch size based on remaining items and quota
        # Aim to use no more than 10% of remaining quota for this operation
        quota_based_size = int(remaining_quota * 0.1 / (items_count / base_batch_size))
        
        return max(min_batch_size, min(base_batch_size, quota_based_size))
    
    def get_optimal_thread_count(self):
        """Determine optimal thread count based on system resources and workload."""
        import os
        
        # Get CPU count but leave some resources for the system
        cpu_count = os.cpu_count() or 4
        available_cpus = max(1, cpu_count - 1)
        
        # Try to check system memory if psutil is available
        try:
            import psutil
            memory = psutil.virtual_memory()
            # If memory usage is high, reduce threads
            if memory.percent > 80:
                return max(1, min(2, available_cpus))
        except ImportError:
            # psutil not available, continue without memory check
            pass
        except Exception as e:
            logging.debug(f"Error checking system memory: {e}")
        
        # Default to cpu_count - 1, with minimum of 1 and maximum of self.max_workers
        return max(1, min(available_cpus, self.max_workers))
    
    def create_youtube_service(self):
        """Create a YouTube API service with enhanced error handling and backoff."""
        api_key = self.get_next_api_key()
        max_retries = 3
        retry_count = 0
        base_delay = 1  # Starting delay in seconds
        
        while retry_count < max_retries:
            try:
                # Use cache_discovery=False to avoid unnecessary HTTP requests
                service = build('youtube', 'v3', developerKey=api_key, cache_discovery=False)
                return service, api_key
            except HttpError as e:
                error_code = getattr(e, 'status_code', 0)
                error_reason = str(e)
                
                # Update error counter for this key
                self.api_errors[api_key] = self.api_errors.get(api_key, 0) + 1
                
                # Enhanced error handling with specific strategies
                if "quota" in error_reason.lower():
                    # Quota exceeded - remove key and try another
                    logging.warning(f"Quota exceeded for API key: {api_key[:4]}...{api_key[-4:]}")
                    self.api_keys.remove(api_key)
                    if not self.api_keys:
                        raise ValueError("All API keys have exceeded their quota. Please try again later.")
                    return self.create_youtube_service()
                    
                elif error_code == 403:  # Forbidden
                    if "accessNotConfigured" in error_reason:
                        # API not enabled for this key
                        logging.error(f"YouTube API not enabled for key: {api_key[:4]}...{api_key[-4:]}")
                        self.api_keys.remove(api_key)
                        if not self.api_keys:
                            raise ValueError("No working API keys available. Please check API configuration.")
                        return self.create_youtube_service()
                    else:
                        # Other 403 errors - try backoff
                        retry_count += 1
                        delay = base_delay * (2 ** retry_count) + random.uniform(0, 1)
                        logging.warning(f"API access forbidden. Backing off for {delay:.2f} seconds. Retry {retry_count}/{max_retries}")
                        time.sleep(delay)
                
                elif error_code == 429:  # Too many requests
                    retry_count += 1
                    delay = base_delay * (2 ** retry_count) + random.uniform(0, 1)
                    logging.warning(f"Rate limit hit. Backing off for {delay:.2f} seconds. Retry {retry_count}/{max_retries}")
                    time.sleep(delay)
                    
                    # Try a different key after multiple rate limit hits
                    if retry_count >= 2:
                        logging.warning(f"Multiple rate limit hits with key {api_key[:4]}..., trying another key")
                        return self.create_youtube_service()
                
                elif error_code >= 500:  # Server errors
                    retry_count += 1
                    delay = base_delay * (2 ** retry_count) + random.uniform(0, 1)
                    logging.warning(f"Server error: {error_code}. Retrying in {delay:.2f} seconds. Retry {retry_count}/{max_retries}")
                    time.sleep(delay)
                
                elif error_code == 400:  # Bad request
                    logging.error(f"Bad request error: {error_reason}")
                    # Try a different key as this one might be invalid
                    self.api_keys.remove(api_key)
                    if not self.api_keys:
                        raise ValueError("All API keys are invalid. Please check your API keys.")
                    return self.create_youtube_service()
                
                else:  # Other errors
                    logging.error(f"Error creating YouTube service: {error_reason}")
                    retry_count += 1
                    if retry_count >= max_retries:
                        raise e
                    delay = base_delay * (2 ** retry_count) + random.uniform(0, 1)
                    time.sleep(delay)
                
            except Exception as e:
                logging.error(f"Unexpected error creating YouTube service: {str(e)}")
                logging.debug(traceback.format_exc())
                raise e
        
        # If we reach here, we've exceeded our retry attempts
        raise Exception(f"Failed to create YouTube service after {max_retries} retries")
    
    def random_delay(self):
        """Wait for a random delay with jitter to avoid detection patterns."""
        min_delay = self.settings.get('delay_min', 0.5)
        max_delay = self.settings.get('delay_max', 2)
        
        # Add jitter to make delays less predictable
        base_delay = random.uniform(min_delay, max_delay)
        jitter = random.uniform(-0.1, 0.1) * base_delay  # ±10% jitter
        
        delay = max(0.1, base_delay + jitter)  # Ensure minimum delay of 0.1s
        
        # Occasionally add a slightly longer delay to mimic human behavior
        if random.random() < 0.1:  # 10% chance
            delay += random.uniform(1.0, 2.0)
            logging.debug(f"Adding extra delay: {delay:.2f}s")
            
        logging.debug(f"Waiting for {delay:.2f}s")
        time.sleep(delay)
    
    def search_youtube_videos(self, keyword, max_results=100):
        """Search for YouTube videos with optimized API usage and pagination."""
        logging.info(f"Searching for videos with keyword: {keyword}")
        
        # Use cache if available
        cache_key = f"search_{keyword}"
        
        # Check if in cache
        cached_results = self.search_cache.get(cache_key)
        if cached_results:
            logging.info(f"Using cached search results for: {keyword}")
            return cached_results
            
        service, api_key = self.create_youtube_service()
        all_videos = []
        page_tokens = [None]  # Start with no page token
        
        try:
            # We'll get multiple pages to go deeper into search results
            for page_index, page_token in enumerate(page_tokens):
                if page_index >= 3:  # Stop after 3 pages
                    break
                    
                if self.stop_requested:
                    return []
                    
                # Track API usage
                self.track_api_usage(api_key, units_used=100)  # Search operation costs 100 units
                    
                # Create search request with optimized parameters
                search_request = service.search().list(
                    q=keyword,
                    part='id,snippet',
                    maxResults=50,  # Max allowed per page
                    pageToken=page_token,
                    type='video',
                    relevanceLanguage='en',
                    videoCaption='any',
                    videoDefinition='any',
                    videoDimension='any',
                    videoDuration='any',
                    videoEmbeddable='any',
                    videoLicense='any',
                    videoSyndicated='any',
                    videoType='any',
                    fields='items(id/videoId,snippet/channelId,snippet/channelTitle,snippet/title),nextPageToken'
                )
                
                # Execute request with error handling
                max_retries = 3
                retry_count = 0
                last_error = None
                
                while retry_count < max_retries:
                    try:
                        search_response = search_request.execute()
                        
                        videos_page = []
                        for item in search_response.get('items', []):
                            if 'videoId' in item.get('id', {}):
                                video_id = item['id']['videoId']
                                videos_page.append({
                                    'id': video_id,
                                    'title': item['snippet']['title'],
                                    'channel_id': item['snippet']['channelId'],
                                    'channel_title': item['snippet']['channelTitle'],
                                    'page': page_index  # Track which page this video came from
                                })
                        
                        # Add videos from this page to our collection
                        all_videos.extend(videos_page)
                        
                        # Get the next page token if available
                        next_page_token = search_response.get('nextPageToken')
                        if next_page_token and len(page_tokens) < 3:  # Limit to 3 pages
                            page_tokens.append(next_page_token)
                        
                        logging.info(f"Found {len(videos_page)} videos for keyword: {keyword} on page {page_index+1}")
                        
                        # Add a small delay between page requests
                        if page_index < 2:  # Don't delay after the last page
                            time.sleep(0.5)
                        
                        break  # Success, exit retry loop
                        
                    except HttpError as e:
                        retry_count += 1
                        last_error = e
                        error_code = getattr(e, 'status_code', 0)
                        
                        if error_code in [403, 429]:  # Rate limiting
                            delay = 2 ** retry_count + random.uniform(0, 1)
                            logging.warning(f"Rate limit hit during search. Retrying in {delay:.2f}s ({retry_count}/{max_retries})")
                            time.sleep(delay)
                        elif error_code >= 500:  # Server errors
                            delay = 2 ** retry_count + random.uniform(0, 1)
                            logging.warning(f"Server error: {error_code}. Retrying in {delay:.2f}s ({retry_count}/{max_retries})")
                            time.sleep(delay)
                        else:
                            logging.error(f"Error searching for videos: {e}")
                            raise e
            
            if last_error and len(all_videos) == 0:
                logging.error(f"Failed to search for videos after {max_retries} retries: {last_error}")
                return []
            
            # Process the collected videos
            logging.info(f"Total videos found across all pages: {len(all_videos)}")
            
            # Remove duplicate videos (same video ID)
            unique_videos = {}
            for video in all_videos:
                if video['id'] not in unique_videos:
                    unique_videos[video['id']] = video
                elif video['page'] > unique_videos[video['id']]['page']:
                    # Prefer videos from later pages if duplicate
                    unique_videos[video['id']] = video
            
            # Convert back to list
            videos = list(unique_videos.values())
            
            # Now we'll group channels and prioritize ones deeper in search results
            channel_videos = {}
            for video in videos:
                channel_id = video['channel_id']
                if channel_id not in channel_videos:
                    channel_videos[channel_id] = []
                channel_videos[channel_id].append(video)
            
            # Sort channels by the highest page they appear on
            sorted_channels = sorted(
                channel_videos.items(),
                key=lambda x: max(v['page'] for v in x[1]),
                reverse=True  # Higher page numbers first
            )
            
            # Skip the first 30 channels (already discovered by other scrapers)
            sorted_channels = sorted_channels[30:] if len(sorted_channels) > 30 else []
            
            # Rebuild our video list, prioritizing videos from channels on later pages
            final_videos = []
            for channel_id, channel_vids in sorted_channels:
                # Sort videos within each channel by page (higher pages first)
                sorted_vids = sorted(channel_vids, key=lambda x: x['page'], reverse=True)
                # Use the highest page video for this channel
                final_videos.append(sorted_vids[0])
            
            logging.info(f"After filtering and removing first 30 channels: {len(final_videos)} videos")
            
            # Cache the results
            self.search_cache.put(cache_key, final_videos)
            
            return final_videos
            
        except Exception as e:
            logging.error(f"Unexpected error searching for videos: {str(e)}")
            logging.debug(traceback.format_exc())
            return []
    
    def get_video_tags_batch(self, video_ids, min_views=1000):
        """Get tags for multiple videos efficiently with batching."""
        if not video_ids:
            return {}
        
        # Filter out video IDs that are already in cache
        uncached_video_ids = []
        for vid in video_ids:
            if vid not in self.video_tags_cache:
                uncached_video_ids.append(vid)
        
        # If all video IDs are in cache, return cached results
        if not uncached_video_ids:
            logging.info("All video tags found in cache")
            return {vid: self.video_tags_cache.get(vid, []) for vid in video_ids}
        
        logging.info(f"Getting tags for {len(uncached_video_ids)} uncached videos in batch")
        service, api_key = self.create_youtube_service()
        
        try:
            # Determine dynamic batch size
            batch_size = self.get_dynamic_batch_size(len(uncached_video_ids), base_batch_size=25, min_batch_size=10)
            results = {}
            video_views = {}
            
            # Process in batches
            for i in range(0, len(uncached_video_ids), batch_size):
                if self.stop_requested:
                    return {}
                    
                batch_ids = uncached_video_ids[i:i+batch_size]
                id_str = ','.join(batch_ids)
                
                # Track API usage - each video costs 1 unit, requesting statistics + snippet
                self.track_api_usage(api_key, units_used=len(batch_ids))
                
                # Make request with retries
                max_retries = 3
                retry_count = 0
                success = False
                
                while retry_count < max_retries and not success:
                    try:
                        # Get both tags and statistics to filter by view count
                        video_response = service.videos().list(
                            part='snippet,statistics',
                            id=id_str,
                            fields='items(id,snippet/tags,statistics/viewCount)'  # Only request fields we need
                        ).execute()
                        
                        for item in video_response.get('items', []):
                            video_id = item['id']
                            tags = item['snippet'].get('tags', [])
                            view_count = int(item['statistics'].get('viewCount', 0))
                            
                            # Store view count
                            video_views[video_id] = view_count
                            
                            # Only process tags for videos with sufficient views
                            if view_count >= min_views:
                                results[video_id] = tags
                                # Store in cache
                                self.video_tags_cache.put(video_id, tags)
                            else:
                                # Still cache but with empty tags
                                self.video_tags_cache.put(video_id, [])
                                
                        success = True
                        
                    except HttpError as e:
                        retry_count += 1
                        error_code = getattr(e, 'status_code', 0)
                        
                        if error_code in [403, 429]:  # Rate limiting
                            delay = 2 ** retry_count + random.uniform(0, 1)
                            logging.warning(f"Rate limit hit during video tag fetch. Retrying in {delay:.2f}s ({retry_count}/{max_retries})")
                            time.sleep(delay)
                        elif error_code >= 500:  # Server errors
                            delay = 2 ** retry_count + random.uniform(0, 1)
                            logging.warning(f"Server error: {error_code}. Retrying in {delay:.2f}s ({retry_count}/{max_retries})")
                            time.sleep(delay)
                        else:
                            logging.error(f"Error getting video tags: {e}")
                            break
                
                # Add a delay between batches to avoid rate limiting
                if i + batch_size < len(uncached_video_ids):
                    time.sleep(0.5)
            
            # Merge cached results with new results
            for vid in video_ids:
                if vid not in results and vid in self.video_tags_cache:
                    tags = self.video_tags_cache.get(vid)
                    if tags:
                        results[vid] = tags
            
            # Log some statistics about popular tags
            self._analyze_tag_popularity(results, video_views)
            
            return results
            
        except Exception as e:
            logging.error(f"Unexpected error getting video tags in batch: {str(e)}")
            logging.debug(traceback.format_exc())
            
            # Return cached results for any videos we have
            cached_results = {}
            for vid in video_ids:
                tags = self.video_tags_cache.get(vid)
                if tags:
                    cached_results[vid] = tags
            return cached_results
    
    def _analyze_tag_popularity(self, video_tags, video_views):
        """Analyze tag popularity across videos for trend identification."""
        # Skip if no data
        if not video_tags or not video_views:
            return
            
        # Count tag occurrences weighted by view count
        tag_stats = {}
        for video_id, tags in video_tags.items():
            views = video_views.get(video_id, 0)
            for tag in tags:
                tag_lower = tag.lower()
                if tag_lower not in tag_stats:
                    tag_stats[tag_lower] = {
                        'count': 0,
                        'views': 0,
                        'videos': []
                    }
                tag_stats[tag_lower]['count'] += 1
                tag_stats[tag_lower]['views'] += views
                tag_stats[tag_lower]['videos'].append(video_id)
        
        # Sort tags by occurrence count
        popular_tags = sorted(tag_stats.items(), key=lambda x: (x[1]['count'], x[1]['views']), reverse=True)
        
        # Log the most popular tags
        if popular_tags:
            top_tags = popular_tags[:10]
            logging.info(f"Most popular tags from current batch:")
            for tag, stats in top_tags:
                logging.info(f"  - {tag}: {stats['count']} videos, {stats['views']} views")
            
            # Store popular tags for future reference
            try:
                with open('popular_tags.txt', 'a', encoding='utf-8') as f:
                    for tag, stats in top_tags:
                        if stats['count'] > 1 and stats['views'] > 5000:  # Only truly popular tags
                            f.write(f"{tag},{stats['count']},{stats['views']}\n")
            except Exception as e:
                logging.debug(f"Error writing to popular_tags.txt: {e}")
    
    def get_channels_info_batch(self, channel_ids):
        """Get detailed information about multiple YouTube channels efficiently."""
        if not channel_ids:
            return {}
        
        # Remove duplicates and filter already processed channels
        unique_ids = list(set(channel_ids))
        uncached_ids = []
        
        for cid in unique_ids:
            if cid not in self.parsed_channels:
                info = self.channel_cache.get(cid)
                if not info:
                    uncached_ids.append(cid)
        
        # If all channels are already processed or in cache, return cached results
        if not uncached_ids:
            logging.info(f"All {len(unique_ids)} channels already processed or in cache")
            results = {}
            for cid in unique_ids:
                info = self.channel_cache.get(cid)
                if info:
                    results[cid] = info
            return results
        
        logging.info(f"Getting info for {len(uncached_ids)} uncached channels in batch")
        service, api_key = self.create_youtube_service()
        
        try:
            # Determine optimal batch size
            batch_size = self.get_dynamic_batch_size(len(uncached_ids), base_batch_size=25, min_batch_size=10)
            results = {}
            
            # Process in batches
            for i in range(0, len(uncached_ids), batch_size):
                if self.stop_requested:
                    return {}
                    
                batch_ids = uncached_ids[i:i+batch_size]
                id_str = ','.join(batch_ids)
                
                # Track API usage - channel.list with these parts costs about 1 unit per channel
                self.track_api_usage(api_key, units_used=len(batch_ids))
                
                # Make request with retries
                max_retries = 3
                retry_count = 0
                success = False
                
                while retry_count < max_retries and not success:
                    try:
                        channel_response = service.channels().list(
                            part='snippet,statistics,contentDetails',
                            id=id_str,
                            fields='items(id,snippet/title,snippet/description,snippet/publishedAt,snippet/country,statistics/subscriberCount,statistics/viewCount,statistics/videoCount)'
                        ).execute()
                        
                        for channel_info in channel_response.get('items', []):
                            channel_id = channel_info['id']
                            
                            # Get country of the channel
                            country = channel_info['snippet'].get('country', 'Unknown')
                            
                            # Check if country is in blacklist
                            if country in self.blacklist_countries:
                                logging.info(f"Channel {channel_id} from country {country} is blacklisted. Skipping.")
                                continue
                            
                            # Get subscriber count
                            subscriber_count = int(channel_info['statistics'].get('subscriberCount', 0))
                            view_count = int(channel_info['statistics'].get('viewCount', 0))
                            
                            # Check subscriber count against settings
                            if subscriber_count < self.settings.get('min_subscribers', 1000) or subscriber_count > self.settings.get('max_subscribers', 1000000):
                                logging.info(f"Channel {channel_id} has {subscriber_count} subscribers, which is outside the specified range. Skipping.")
                                continue
                            
                            # Check view count against settings
                            if view_count < self.settings.get('min_total_views', 10000):
                                logging.info(f"Channel {channel_id} has {view_count} total views, which is below the minimum. Skipping.")
                                continue
                            
                            # Check channel creation date
                            published_at = channel_info['snippet']['publishedAt']
                            creation_year = int(published_at.split('-')[0])
                            
                            if creation_year < self.settings.get('creation_year_limit', 2015):
                                logging.info(f"Channel {channel_id} was created in {creation_year}, which is before the limit. Skipping.")
                                continue
                            
                            # Store channel info in results
                            results[channel_id] = {
                                'id': channel_id,
                                'title': channel_info['snippet']['title'],
                                'description': channel_info['snippet']['description'],
                                'published_at': published_at,
                                'country': country,
                                'subscriber_count': subscriber_count,
                                'view_count': view_count,
                                'video_count': int(channel_info['statistics'].get('videoCount', 0))
                            }
                            
                            # Cache channel information
                            self.channel_cache.put(channel_id, results[channel_id])
                        
                        success = True
                        
                    except HttpError as e:
                        retry_count += 1
                        error_code = getattr(e, 'status_code', 0)
                        
                        if error_code in [403, 429]:  # Rate limiting
                            delay = 2 ** retry_count + random.uniform(0, 1)
                            logging.warning(f"Rate limit hit during channel info fetch. Retrying in {delay:.2f}s ({retry_count}/{max_retries})")
                            time.sleep(delay)
                        elif error_code >= 500:  # Server errors
                            delay = 2 ** retry_count + random.uniform(0, 1)
                            logging.warning(f"Server error: {error_code}. Retrying in {delay:.2f}s ({retry_count}/{max_retries})")
                            time.sleep(delay)
                        else:
                            logging.error(f"Error getting channel info: {e}")
                            break
                
                # Add a delay between batches to avoid rate limiting
                if i + batch_size < len(uncached_ids):
                    time.sleep(0.5)
            
            # Add any cached channels to results
            for cid in unique_ids:
                if cid not in results:
                    info = self.channel_cache.get(cid)
                    if info:
                        results[cid] = info
            
            return results
            
        except Exception as e:
            logging.error(f"Unexpected error getting channels info in batch: {str(e)}")
            logging.debug(traceback.format_exc())
            
            # Return any cached results we have
            cached_results = {}
            for cid in unique_ids:
                info = self.channel_cache.get(cid)
                if info:
                    cached_results[cid] = info
                    
            return cached_results
    
    def _get_random_headers(self):
        """Generate random headers to avoid detection."""
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
            'Referer': 'https://www.youtube.com/results?search_query=' + str(random.randint(1000, 9999)),
            'Viewport-Width': str(random.randint(1024, 1920)),
            'Viewport-Height': str(random.randint(768, 1080)),
            'Cookie': f'VISITOR_INFO1_LIVE={uuid.uuid4().hex[:16]}; CONSENT=YES+; SID={uuid.uuid4().hex}'
        }
        
        return headers
    
    def get_channel_about_page(self, channel_id):
        """Scrape the about page of a YouTube channel with improved error handling and caching."""
        # Check if we already have this page in cache
        cached_page = self.about_page_cache.get(channel_id)
        if cached_page:
            logging.info(f"Using cached about page for channel ID: {channel_id}")
            return cached_page
            
        logging.info(f"Scraping about page for channel ID: {channel_id}")
        max_retries = 3
        retry_count = 0
        base_delay = 1
        
        # We'll try to scrape both the about page and the channel homepage for maximum data
        about_content = None
        home_content = None
        
        # Try to get about page
        while retry_count < max_retries and not about_content:
            if self.stop_requested:
                return None
                
            try:
                about_url = f"https://www.youtube.com/channel/{channel_id}/about"
                
                proxy = self.get_proxy()
                headers = self._get_random_headers()
                
                session = requests.Session()
                
                # Add a timeout to avoid hanging
                response = session.get(about_url, headers=headers, proxies=proxy, timeout=15)
                response.raise_for_status()
                
                # Check if we got a proper response (not an error page)
                if "This page isn't available" in response.text or "Error 404" in response.text:
                    logging.warning(f"Channel {channel_id} about page returned error page (404)")
                    # Try with different URL format
                    about_url = f"https://www.youtube.com/c/{channel_id}/about"
                    response = session.get(about_url, headers=headers, proxies=proxy, timeout=15)
                    response.raise_for_status()
                    
                    if "This page isn't available" in response.text or "Error 404" in response.text:
                        logging.warning(f"Channel {channel_id} also not found with /c/ URL format")
                        # Try one more format
                        about_url = f"https://www.youtube.com/@{channel_id}/about"
                        try:
                            response = session.get(about_url, headers=headers, proxies=proxy, timeout=15)
                            response.raise_for_status()
                            
                            if "This page isn't available" in response.text or "Error 404" in response.text:
                                logging.warning(f"Channel {channel_id} not found with any URL format")
                                about_content = None
                            else:
                                about_content = response.text
                        except:
                            about_content = None
                    else:
                        about_content = response.text
                else:
                    about_content = response.text
                
            except requests.exceptions.RequestException as e:
                retry_count += 1
                delay = base_delay * (2 ** retry_count) + random.uniform(0, 1)
                
                if isinstance(e, requests.exceptions.Timeout):
                    logging.warning(f"Timeout when scraping channel {channel_id} about page. Retrying in {delay:.2f}s ({retry_count}/{max_retries})")
                elif isinstance(e, requests.exceptions.ConnectionError):
                    logging.warning(f"Connection error when scraping channel {channel_id} about page. Retrying in {delay:.2f}s ({retry_count}/{max_retries})")
                else:
                    logging.warning(f"Error scraping channel {channel_id} about page: {e}. Retrying in {delay:.2f}s ({retry_count}/{max_retries})")
                
                time.sleep(delay)
                
                # Try a different proxy if available
                if self.proxies and retry_count > 1:
                    logging.info(f"Switching proxy for retry {retry_count}")
            
            except Exception as e:
                logging.error(f"Unexpected error scraping channel about page: {str(e)}")
                logging.debug(traceback.format_exc())
                about_content = None
                break
        
        # Reset retry counter 
        retry_count = 0
        
        # Try to get channel homepage too (sometimes contains additional info)
        while retry_count < max_retries and not home_content:
            if self.stop_requested:
                return about_content  # Return what we have so far
                
            try:
                home_url = f"https://www.youtube.com/channel/{channel_id}"
                
                proxy = self.get_proxy()
                headers = self._get_random_headers()
                
                session = requests.Session()
                
                # Add a timeout to avoid hanging
                response = session.get(home_url, headers=headers, proxies=proxy, timeout=15)
                response.raise_for_status()
                
                # Check if we got a proper response (not an error page)
                if "This page isn't available" in response.text or "Error 404" in response.text:
                    logging.warning(f"Channel {channel_id} homepage returned error page (404)")
                    # Try with different URL format
                    home_url = f"https://www.youtube.com/c/{channel_id}"
                    response = session.get(home_url, headers=headers, proxies=proxy, timeout=15)
                    response.raise_for_status()
                    
                    if "This page isn't available" in response.text or "Error 404" in response.text:
                        logging.warning(f"Channel {channel_id} also not found with /c/ URL format for homepage")
                        # Try one more format
                        home_url = f"https://www.youtube.com/@{channel_id}"
                        try:
                            response = session.get(home_url, headers=headers, proxies=proxy, timeout=15)
                            response.raise_for_status()
                            
                            if "This page isn't available" in response.text or "Error 404" in response.text:
                                logging.warning(f"Channel {channel_id} not found with any URL format for homepage")
                                home_content = None
                            else:
                                home_content = response.text
                        except:
                            home_content = None
                    else:
                        home_content = response.text
                else:
                    home_content = response.text
                
            except requests.exceptions.RequestException as e:
                retry_count += 1
                delay = base_delay * (2 ** retry_count) + random.uniform(0, 1)
                
                if isinstance(e, requests.exceptions.Timeout):
                    logging.warning(f"Timeout when scraping channel {channel_id} homepage. Retrying in {delay:.2f}s ({retry_count}/{max_retries})")
                elif isinstance(e, requests.exceptions.ConnectionError):
                    logging.warning(f"Connection error when scraping channel {channel_id} homepage. Retrying in {delay:.2f}s ({retry_count}/{max_retries})")
                else:
                    logging.warning(f"Error scraping channel {channel_id} homepage: {e}. Retrying in {delay:.2f}s ({retry_count}/{max_retries})")
                
                time.sleep(delay)
                
                # Try a different proxy if available
                if self.proxies and retry_count > 1:
                    logging.info(f"Switching proxy for retry {retry_count}")
            
            except Exception as e:
                logging.error(f"Unexpected error scraping channel homepage: {str(e)}")
                logging.debug(traceback.format_exc())
                home_content = None
                break
        
        # Parse the content from both pages
        about_text = ""
        home_text = ""
        
        if about_content:
            try:
                about_soup = BeautifulSoup(about_content, 'html.parser')
                about_text = about_soup.get_text()
                
                # Also look for specific structured data
                about_script_tags = about_soup.find_all('script', type='application/ld+json')
                for script in about_script_tags:
                    try:
                        json_data = json.loads(script.string)
                        if 'description' in json_data:
                            about_text += "\n" + json_data['description']
                        if 'sameAs' in json_data:
                            for link in json_data['sameAs']:
                                about_text += "\n" + link
                    except (json.JSONDecodeError, AttributeError):
                        pass
            except Exception as e:
                logging.error(f"Error parsing about page HTML: {e}")
                logging.debug(traceback.format_exc())
        
        if home_content:
            try:
                home_soup = BeautifulSoup(home_content, 'html.parser')
                home_text = home_soup.get_text()
                
                # Look for social links in meta tags
                meta_tags = home_soup.find_all('meta')
                for tag in meta_tags:
                    if tag.get('content') and ('http://' in tag['content'] or 'https://' in tag['content']):
                        home_text += "\n" + tag['content']
                
                # Also extract from JSON-LD data
                home_script_tags = home_soup.find_all('script', type='application/ld+json')
                for script in home_script_tags:
                    try:
                        json_data = json.loads(script.string)
                        self._extract_json_data(json_data, home_text)
                    except (json.JSONDecodeError, AttributeError):
                        pass
            except Exception as e:
                logging.error(f"Error parsing homepage HTML: {e}")
                logging.debug(traceback.format_exc())
        
        # Combine texts from both pages
        combined_text = about_text + "\n\n" + home_text
        
        # Store in cache
        self.about_page_cache.put(channel_id, combined_text)
        
        return combined_text
    
    def _extract_json_data(self, json_data, text_buffer):
        """Helper method to recursively extract useful data from JSON-LD."""
        if isinstance(json_data, dict):
            for key, value in json_data.items():
                if key in ['description', 'email', 'url', 'sameAs', 'contactPoint', 'social', 'link']:
                    if isinstance(value, str):
                        text_buffer += "\n" + value
                    elif isinstance(value, list):
                        for item in value:
                            if isinstance(item, str):
                                text_buffer += "\n" + item
                            else:
                                self._extract_json_data(item, text_buffer)
                elif isinstance(value, (dict, list)):
                    self._extract_json_data(value, text_buffer)
        elif isinstance(json_data, list):
            for item in json_data:
                self._extract_json_data(item, text_buffer)
    
    def extract_social_media(self, text):
        """Extract social media links with improved pattern matching."""
        if not text:
            return []
        
        social_links = []
        
        # Use pre-compiled patterns for better performance
        for platform, patterns in self.social_patterns.items():
            for pattern in patterns:
                matches = pattern.findall(text)
                for match in matches:
                    # Ensure links have http/https prefix
                    if not match.startswith(('http://', 'https://')):
                        if '/' in match and not match.startswith('/'):
                            domain = match.split('/')[0]
                            if '.' in domain:  # It's likely a domain
                                match = 'https://' + match
                    social_links.append(match)
        
        # Extract social media handles
        for platform, pattern in self.social_handle_patterns.items():
            for match in pattern.finditer(text):
                if match.group(1):
                    username = match.group(1)
                    if platform == 'instagram':
                        social_links.append(f"https://instagram.com/{username}")
                    elif platform == 'twitter':
                        social_links.append(f"https://twitter.com/{username}")
                    elif platform == 'facebook':
                        social_links.append(f"https://facebook.com/{username}")
        
        # Also look for social media mentions in text format
        social_media_prefixes = {
            'fb:': 'https://facebook.com/',
            'facebook:': 'https://facebook.com/',
            'twitter:': 'https://twitter.com/',
            'x:': 'https://x.com/',
            'ig:': 'https://instagram.com/',
            'instagram:': 'https://instagram.com/',
            'linkedin:': 'https://linkedin.com/in/',
            'tiktok:': 'https://tiktok.com/@',
            'tt:': 'https://tiktok.com/@',
            'telegram:': 'https://t.me/',
            'tg:': 'https://t.me/',
            'discord:': 'https://discord.gg/',
            'snap:': 'https://snapchat.com/add/',
            'snapchat:': 'https://snapchat.com/add/',
            'youtube:': 'https://youtube.com/@',
            'yt:': 'https://youtube.com/@',
        }
        
        # Look for handle mentions
        lines = text.split('\n')
        for line in lines:
            # Check for colon format (e.g., "Twitter: @username")
            for prefix, url_prefix in social_media_prefixes.items():
                match = re.search(rf'{prefix}\s*[@]?([a-zA-Z0-9._-]+)', line, re.IGNORECASE)
                if match:
                    username = match.group(1)
                    if len(username) >= 3:  # Avoid very short usernames
                        social_links.append(f"{url_prefix}{username}")
        
        # Remove duplicates while preserving order
        unique_links = []
        seen = set()
        for link in social_links:
            normalized_link = link.lower().rstrip('/')
            if normalized_link not in seen:
                seen.add(normalized_link)
                unique_links.append(link)
        
        return unique_links
    
    def parse_channel_contacts(self, channel_info):
        """Parse contact information from a channel with improved error handling."""
        if not channel_info:
            return
            
        try:
            channel_id = channel_info['id']
            channel_title = channel_info['title']
            
            # Use AdvancedEmailFinder's scan_youtube_content if enabled
            if self.settings.get('use_advanced_email_finder', False) and self.advanced_email_finder:
                # Prepare channel data for scan_youtube_content
                channel_data = {
                    'id': channel_id,
                    'title': channel_title,
                    'description': channel_info.get('description', '')
                }
                
                # Get about page content if needed
                about_page_text = None
                if self.settings.get('parse_mode') in ['email', 'both']:
                    about_page_text = self.get_channel_about_page(channel_id)
                    if about_page_text:
                        channel_data['about_page'] = about_page_text
                
                # Collect video descriptions and comments (if available)
                video_descriptions = []
                # Here we could implement collection of video descriptions if needed
                
                # Use advanced email finder to scan channel content
                emails = self.advanced_email_finder.scan_youtube_content(
                    channel_data, 
                    video_descriptions=video_descriptions, 
                    comments=None
                )
                
                # Get social media links using standard method
                social_links = []
                if self.settings.get('parse_mode') in ['social', 'both']:
                    if about_page_text:
                        social_links = self.extract_social_media(about_page_text)
                    if not social_links:
                        social_links = self.extract_social_media(channel_info.get('description', ''))
            else:
                # Use standard methods for email and social media extraction
                # First try to extract from the channel description
                description = channel_info.get('description', '')
                emails = self.extract_emails(description)
                social_links = self.extract_social_media(description)
                
                # If no emails found in description, try scraping the about page
                if (self.settings.get('parse_mode') in ['email', 'both'] and not emails) or \
                   (self.settings.get('parse_mode') in ['social', 'both'] and not social_links):
                    about_page_text = self.get_channel_about_page(channel_id)
                    
                    if about_page_text:
                        if self.settings.get('parse_mode') in ['email', 'both'] and not emails:
                            emails = self.extract_emails(about_page_text)
                        
                        if self.settings.get('parse_mode') in ['social', 'both'] and not social_links:
                            social_links = self.extract_social_media(about_page_text)
            
            # Apply email similarity filtering if enabled
            if emails and self.settings.get('filter_similar_emails', True):
                emails = self._filter_similar_emails(emails)
                
            # Save emails if found and in the correct parse mode - simplified format
            if self.settings.get('parse_mode') in ['email', 'both'] and emails:
                self._save_emails(emails, channel_title, channel_id)
            
            # Save social links if found and in the correct parse mode
            if self.settings.get('parse_mode') in ['social', 'both'] and social_links:
                self._save_social_media(social_links, channel_title, channel_id)
                
        except Exception as e:
            logging.error(f"Error parsing contacts for channel {channel_info.get('id', 'unknown')}: {str(e)}")
            logging.debug(traceback.format_exc())
    
    def _filter_similar_emails(self, emails):
        """Filter out similar emails based on Levenshtein distance."""
        if not emails or len(emails) <= 1:
            return emails
            
        # Normalize emails
        normalized_emails = {}
        for email in emails:
            normalized = self.normalize_email(email)
            normalized_emails[email] = normalized
        
        # Group by similarity
        unique_emails = []
        used_emails = set()
        
        for email in emails:
            if email in used_emails:
                continue
                
            similar_group = [email]
            used_emails.add(email)
            
            # Compare with other emails
            for other_email in emails:
                if other_email != email and other_email not in used_emails:
                    similarity = self.calculate_email_similarity(
                        normalized_emails[email],
                        normalized_emails[other_email]
                    )
                    
                    if similarity >= self.similarity_threshold:
                        similar_group.append(other_email)
                        used_emails.add(other_email)
            
            # Choose the best email from the group (shortest, most common domain, or first)
            if len(similar_group) > 1:
                # Sort by domain popularity and length
                best_email = min(similar_group, key=lambda e: (
                    e.split('@')[-1] not in self.common_domains,  # Prefer common domains
                    len(e),  # Prefer shorter emails
                    e  # Lexicographic order as tiebreaker
                ))
                unique_emails.append(best_email)
            else:
                unique_emails.append(email)
        
        return unique_emails
    
    def _save_emails(self, emails, channel_title, channel_id):
        """Save discovered emails to files."""
        try:
            with open('emails.txt', 'a', encoding='utf-8') as f:
                for email in emails:
                    # Skip email if it's already in the parsed_emails set
                    if email not in self.parsed_emails:
                        self.parsed_emails.add(email)
                        # Save only the email address, one per line
                        f.write(f"{email}\n")
                        logging.info(f"Found email: {email} for channel: {channel_title}")
                        
                        # Track email domains for statistics
                        domain = email.split('@')[-1]
                        self.email_domains[domain] = self.email_domains.get(domain, 0) + 1
            
            # Save detailed email info to a separate file for reference
            with open('emails_detailed.txt', 'a', encoding='utf-8') as f:
                for email in emails:
                    if email in self.parsed_emails:
                        f.write(f"{email},{channel_title},{channel_id}\n")
        except Exception as e:
            logging.error(f"Error saving emails: {e}")
            logging.debug(traceback.format_exc())
    
    def _save_social_media(self, social_links, channel_title, channel_id):
        """Save discovered social media links to file."""
        try:
            with open('social_media.txt', 'a', encoding='utf-8') as f:
                for link in social_links:
                    if link not in self.parsed_social_media:
                        self.parsed_social_media.add(link)
                        f.write(f"{link},{channel_title},{channel_id}\n")
                        logging.info(f"Found social link: {link} for channel: {channel_title}")
        except Exception as e:
            logging.error(f"Error saving social media links: {e}")
            logging.debug(traceback.format_exc())
    
    def save_channel(self, channel_info):
        """Save channel information to file with error handling."""
        if not channel_info:
            return
        
        channel_id = channel_info['id']
        
        if channel_id in self.parsed_channels:
            return
        
        try:
            self.parsed_channels.add(channel_id)
            
            with open('channels.txt', 'a', encoding='utf-8') as f:
                f.write(f"{channel_id},{channel_info['title']},{channel_info['subscriber_count']},{channel_info['view_count']},{channel_info['country']}\n")
                
            logging.info(f"Saved channel: {channel_info['title']} ({channel_id})")
        except Exception as e:
            logging.error(f"Error saving channel {channel_id}: {e}")
            logging.debug(traceback.format_exc())
    
    def process_search_results(self, keyword):
        """Process search results with improved structure and error handling."""
        if self.stop_requested:
            return []
            
        # Step 1: Search for videos with the keyword
        videos = self.search_youtube_videos(keyword)
        
        if not videos:
            logging.info(f"No videos to process for keyword: {keyword} after filtering.")
            return []
        
        # Step 2: Extract channel IDs and get channel information
        channel_ids = [video['channel_id'] for video in videos]
        channels_info = self.get_channels_info_batch(channel_ids)
        
        if not channels_info:
            logging.info(f"No suitable channels found for keyword: {keyword}")
            return []
        
        # Step 3: Save channel information
        self._save_channels_info(channels_info)
        
        # Step 4: Process channels for contact information
        self._process_channel_contacts(channels_info)
        
        # Step 5: Extract and process video tags for new keywords
        new_keywords = self._process_video_tags(channels_info, videos, keyword)
        
        return new_keywords
    
    def _save_channels_info(self, channels_info):
        """Save channel information to file."""
        for channel_id, channel_info in channels_info.items():
            if self.stop_requested:
                return
            self.save_channel(channel_info)
    
    def _process_channel_contacts(self, channels_info):
        """Process channels to extract contact information with optimal threading."""
        if not channels_info:
            return
            
        if self.stop_requested:
            return
            
        # Determine if we should use threading
        if len(channels_info) > 1:
            # Use adaptive thread count
            optimal_threads = self.get_optimal_thread_count()
            
            with ThreadPoolExecutor(max_workers=optimal_threads) as executor:
                futures = []
                
                # Submit tasks
                for channel_info in channels_info.values():
                    if self.stop_requested:
                        break
                    futures.append(executor.submit(self.parse_channel_contacts, channel_info))
                
                # Process results with proper error handling
                completed_count = 0
                for future in as_completed(futures):
                    if self.stop_requested:
                        # Cancel remaining futures
                        for f in futures:
                            if not f.done():
                                try:
                                    f.cancel()
                                except:
                                    pass
                        return
                        
                    try:
                        future.result()
                        completed_count += 1
                        
                        # Add a small delay every few completions
                        if completed_count % 3 == 0:
                            time.sleep(0.5)
                            
                    except Exception as e:
                        logging.error(f"Error in thread for parsing contacts: {e}")
                        logging.debug(traceback.format_exc())
        else:
            # For just one channel, process directly
            for channel_info in channels_info.values():
                if self.stop_requested:
                    return
                self.parse_channel_contacts(channel_info)
    
    def _process_video_tags(self, channels_info, videos, keyword):
        """Process video tags to discover new keywords."""
        if self.stop_requested:
            return []
            
        # Get channel videos or fall back to search videos
        channel_video_ids = self._get_channel_videos(list(channels_info.keys()))
        
        if not channel_video_ids:
            video_ids = [video['id'] for video in videos[:20]]  # Limit to first 20 videos
        else:
            video_ids = channel_video_ids
        
        # Skip occasionally to save quota (80% chance to process)
        if random.random() < 0.8:
            # Get video tags
            video_tags_dict = self.get_video_tags_batch(video_ids, min_views=1000)
            
            # Extract keywords from tags
            new_keywords = self._extract_keywords_from_tags(video_tags_dict)
            
            # Limit to avoid explosion
            if len(new_keywords) > 20:
                new_keywords = set(random.sample(list(new_keywords), 20))
                
            # Save discovered keywords
            try:
                with open('keywords_discovered.txt', 'a', encoding='utf-8') as f:
                    for keyword in new_keywords:
                        f.write(f"{keyword}\n")
            except Exception as e:
                logging.error(f"Error saving discovered keywords: {e}")
                logging.debug(traceback.format_exc())
            
            return list(new_keywords)
        else:
            logging.info(f"Skipping video tag processing for keyword: {keyword} to save API quota")
            return []
            
    def _get_channel_videos(self, channel_ids, max_results_per_channel=5):
        """Get videos from specific channels to analyze deeper content."""
        if not channel_ids:
            return []
            
        if self.stop_requested:
            return []
            
        service, api_key = self.create_youtube_service()
        video_ids = []
        
        # Limit to a few channels to save quota
        sample_size = min(3, len(channel_ids))
        sampled_channels = random.sample(channel_ids, sample_size)
        
        try:
            for channel_id in sampled_channels:
                if self.stop_requested:
                    return []
                    
                # Track API usage - search operation costs 100 units
                self.track_api_usage(api_key, units_used=100)
                    
                try:
                    # Search for videos from this channel
                    channel_videos_request = service.search().list(
                        part='id',
                        channelId=channel_id,
                        maxResults=max_results_per_channel,
                        type='video',
                        order='viewCount',  # Get most viewed videos
                        fields='items(id/videoId)'
                    )
                    
                    response = channel_videos_request.execute()
                    
                    for item in response.get('items', []):
                        if 'videoId' in item.get('id', {}):
                            video_ids.append(item['id']['videoId'])
                            
                    # Add a small delay between requests
                    time.sleep(0.5)
                    
                except HttpError as e:
                    logging.warning(f"Error getting videos for channel {channel_id}: {e}")
                    continue
            
            return video_ids
            
        except Exception as e:
            logging.error(f"Error getting channel videos: {e}")
            logging.debug(traceback.format_exc())
            return []
            
    def _extract_keywords_from_tags(self, video_tags_dict):
        """Extract valuable keywords from video tags with popularity analysis."""
        if not video_tags_dict:
            return set()
            
        # Count occurrences of each tag
        tag_counts = {}
        for video_id, tags in video_tags_dict.items():
            for tag in tags:
                tag_lower = tag.lower()
                tag_counts[tag_lower] = tag_counts.get(tag_lower, 0) + 1
        
        # Filter tags that appear in multiple videos
        popular_tags = {tag for tag, count in tag_counts.items() if count > 1}
        
        # Process tags to get high-quality keywords
        keywords = set()
        for tag in popular_tags:
            # Keep only tags with reasonable length and word count
            words = tag.split()
            word_count = len(words)
            
            if 1 <= word_count <= 3 and 3 <= len(tag) <= 30:
                # Skip tags that are just numbers or very generic
                if not tag.isdigit() and not any(generic in tag.lower() for generic in ['subscribe', 'channel', 'video', 'follow']):
                    keywords.add(tag)
        
        logging.info(f"Extracted {len(keywords)} quality keywords from {len(video_tags_dict)} videos")
        return keywords

    def normalize_email(self, email):
        """Normalize email address for similarity comparison."""
        if not email or '@' not in email:
            return email
            
        # Extract username and domain
        try:
            username, domain = email.lower().strip().split('@', 1)
            
            # Track email domains for statistics
            self.email_domains[domain] = self.email_domains.get(domain, 0) + 1
            
            # Gmail-specific normalization (remove dots, ignore everything after +)
            if domain in ['gmail.com', 'googlemail.com']:
                # Remove dots from username
                username = username.replace('.', '')
                # Remove everything after + in username
                if '+' in username:
                    username = username.split('+', 1)[0]
                # Normalize googlemail to gmail
                domain = 'gmail.com'
            
            # Remove common prefixes and suffixes
            for prefix in ['contact', 'info', 'support', 'admin', 'mail', 'email', 'hello', 'business']:
                if username.startswith(prefix) and len(username) > len(prefix) + 1:
                    if username[len(prefix)] in ['.', '-', '_']:
                        username = username[len(prefix)+1:]
            
            # Remove digits at the end if there are more than 2 characters before them
            username_no_digits = re.sub(r'\d+$', '', username)
            if len(username_no_digits) > 2 and username_no_digits != username:
                username = username_no_digits
            
            # Return normalized email
            return f"{username}@{domain}"
        except Exception as e:
            logging.debug(f"Error normalizing email {email}: {e}")
            return email
            
    def calculate_email_similarity(self, email1, email2):
        """Calculate similarity between two email addresses."""
        if email1 == email2:
            return 1.0
        
        if '@' not in email1 or '@' not in email2:
            return 0.0
        
        # Split into username and domain
        try:
            username1, domain1 = email1.lower().split('@', 1)
            username2, domain2 = email2.lower().split('@', 1)
            
            # If domains don't match, they're different emails
            if domain1 != domain2:
                return 0.0
                
            # Calculate username similarity
            # For very short usernames, require exact match
            if len(username1) <= 3 or len(username2) <= 3:
                return 1.0 if username1 == username2 else 0.0
                
            # Levenshtein distance ratio for longer usernames
            similarity = self._levenshtein_ratio(username1, username2)
            
            # Check if one username is a prefix of the other
            if username1.startswith(username2) or username2.startswith(username1):
                prefix_bonus = 0.15  # Boost similarity for prefix matches
                similarity = min(1.0, similarity + prefix_bonus)
                
            return similarity
        except Exception:
            # If any errors in calculation, treat as different emails
            return 0.0
            
    def _levenshtein_ratio(self, s1, s2):
        """Calculate normalized Levenshtein distance between two strings."""
        if s1 == s2:
            return 1.0
        
        # Calculate Levenshtein distance
        if len(s1) < len(s2):
            s1, s2 = s2, s1
            
        # Length of longer string
        len_s1 = len(s1)
        
        if len_s1 == 0:
            return 0.0
            
        # Simple dynamic programming implementation
        previous_row = list(range(len(s2) + 1))
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                # Calculate cost of operations
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
            
        # Normalize distance to ratio (0.0 to 1.0)
        distance = previous_row[-1]
        max_len = max(len(s1), len(s2))
        ratio = 1.0 - (distance / max_len)
        
        return ratio

    def remove_email_duplicates(self):
        """Remove duplicate emails from emails.txt and update statistics."""
        try:
            if os.path.exists('emails.txt'):
                # Read all emails from the file
                emails = []
                with open('emails.txt', 'r', encoding='utf-8') as f:
                    for line in f:
                        email = line.strip()
                        if email and not email.startswith('#'):
                            emails.append(email)
                
                if not emails:
                    logging.info("No emails to process for duplicate removal.")
                    return
                
                # Apply similarity-based filtering if enabled
                if self.settings.get('filter_similar_emails', True):
                    # First normalize all emails
                    normalized_emails = {}
                    for email in emails:
                        normalized_emails[email] = self.normalize_email(email)
                    
                    # Group similar emails
                    email_groups = []
                    processed = set()
                    
                    for email in emails:
                        if email in processed:
                            continue
                            
                        group = [email]
                        processed.add(email)
                        
                        # Find similar emails
                        for other in emails:
                            if other != email and other not in processed:
                                similarity = self.calculate_email_similarity(
                                    normalized_emails[email],
                                    normalized_emails[other]
                                )
                                
                                if similarity >= self.similarity_threshold:
                                    group.append(other)
                                    processed.add(other)
                        
                        email_groups.append(group)
                    
                    # Select best email from each group
                    best_emails = []
                    for group in email_groups:
                        if len(group) == 1:
                            best_emails.append(group[0])
                        else:
                            # Choose the best email (common domain, shorter, etc.)
                            best_email = min(group, key=lambda e: (
                                e.split('@')[-1] not in ['gmail.com', 'yahoo.com', 'hotmail.com'],
                                len(e),
                                e
                            ))
                            best_emails.append(best_email)
                    
                    unique_emails = best_emails
                    logging.info(f"Removed {len(emails) - len(unique_emails)} similar/duplicate emails.")
                else:
                    # Simple duplicate removal
                    unique_emails = []
                    seen = set()
                    for email in emails:
                        if email not in seen:
                            seen.add(email)
                            unique_emails.append(email)
                    
                    logging.info(f"Removed {len(emails) - len(unique_emails)} duplicate emails.")
                
                # Write unique emails back to the file
                with open('emails.txt', 'w', encoding='utf-8') as f:
                    for email in unique_emails:
                        f.write(f"{email}\n")
                
                # Update the parsed_emails set
                self.parsed_emails = set(unique_emails)
                
                # Update domain statistics
                self.email_domains = {}
                for email in unique_emails:
                    try:
                        domain = email.split('@')[-1]
                        self.email_domains[domain] = self.email_domains.get(domain, 0) + 1
                    except:
                        pass
                
                # Save updated domain statistics
                self.save_email_stats()
                
        except Exception as e:
            self._log_error("EMAIL DEDUP ERROR", f"Error removing email duplicates: {e}")
    
    def run(self):
        """Run the YouTube channel scraper with optimization and error handling."""
        logging.info("Starting YouTube Channel Scraper...")
        
        # Check if output files exist, create them if not
        self._initialize_output_files()
        
        # Reset stop flag
        self.stop_requested = False
        
        # Process initial keywords
        self.all_keywords = self.keywords.copy()
        self.processed_keywords = set()
        
        while self.all_keywords and not self.stop_requested:
            # Get the next keyword
            keyword = self.all_keywords.pop(0)
            
            # Skip already processed keywords
            if keyword in self.processed_keywords:
                continue
            
            self.processed_keywords.add(keyword)
            logging.info(f"\nProcessing keyword: {keyword}")
            logging.info(f"Remaining keywords: {len(self.all_keywords)}")
            logging.info(f"Processed keywords: {len(self.processed_keywords)}")
            
            try:
                # Process search results and get new keywords
                new_keywords = self.process_search_results(keyword)
                
                # Add new keywords to the queue if they haven't been processed yet
                for new_keyword in new_keywords:
                    if new_keyword not in self.processed_keywords:
                        self.all_keywords.append(new_keyword)
                
                # Print progress
                logging.info(f"Channels found: {len(self.parsed_channels)}")
                logging.info(f"Emails found: {len(self.parsed_emails)}")
                logging.info(f"Social media links found: {len(self.parsed_social_media)}")
                
                # API usage statistics
                logging.info("API key usage statistics:")
                for key, count in self.api_usage_count.items():
                    if key in self.api_keys:  # Only show active keys
                        masked_key = key[:4] + '*' * (len(key) - 8) + key[-4:]
                        quota_used = self.daily_quota_usage.get(key, 0)
                        logging.info(f"  Key {masked_key}: {count} uses, {quota_used} quota units")
                
                # Save email domain statistics periodically
                self.save_email_stats()
                
                # Add delay between different keywords
                if self.all_keywords and not self.stop_requested:
                    self.random_delay()
                    
            except Exception as e:
                logging.error(f"Error processing keyword '{keyword}': {e}")
                logging.debug(traceback.format_exc())
                
                # Continue with next keyword
                continue
        
        if self.stop_requested:
            logging.info("Scraper stopped by user request.")
            self.save_progress()
        else:
            logging.info("Scraping completed. All keywords processed.")
            
        # Save final email stats
        self.save_email_stats()
        
        # Remove email duplicates when scraping completes
        if not self.stop_requested:
            self.remove_email_duplicates()
            
    def _initialize_output_files(self):
        """Initialize output files with headers if they don't exist."""
        try:
            # Define files with their headers
            files_with_headers = {
                'channels.txt': "channel_id,title,subscriber_count,view_count,country\n",
                'emails_detailed.txt': "email,channel_title,channel_id\n",
                'social_media.txt': "link,channel_title,channel_id\n",
                'keywords_discovered.txt': "# Discovered keywords\n",
                'email_stats.txt': "domain,count\n"
            }
            
            # Simple files without headers
            simple_files = ['emails.txt']
            
            # Create files with headers if they don't exist
            for filename, header in files_with_headers.items():
                if not os.path.exists(filename):
                    with open(filename, 'w', encoding='utf-8') as f:
                        f.write(header)
            
            # Create simple files if they don't exist
            for filename in simple_files:
                if not os.path.exists(filename):
                    with open(filename, 'w', encoding='utf-8') as f:
                        pass
                        
        except Exception as e:
            logging.error(f"Error initializing output files: {e}")
            logging.debug(traceback.format_exc())
        
    def save_email_stats(self):
        """Save statistics about email domains."""
        try:
            # Sort domains by frequency
            sorted_domains = sorted(self.email_domains.items(), key=lambda x: x[1], reverse=True)
            
            with open('email_stats.txt', 'w', encoding='utf-8') as f:
                f.write("domain,count\n")
                for domain, count in sorted_domains:
                    f.write(f"{domain},{count}\n")
                    
            logging.info(f"Email domain statistics saved to email_stats.txt")
        except Exception as e:
            self._log_error("EMAIL STATS ERROR", f"Error saving email statistics: {e}")
    
    def save_progress(self):
        """Save current progress data for resume capability."""
        try:
            with open('progress_data.json', 'w', encoding='utf-8') as f:
                progress = {
                    'processed_keywords': list(self.processed_keywords),
                    'pending_keywords': self.all_keywords,
                    'api_usage': self.api_usage_count,
                    'daily_quota_usage': self.daily_quota_usage,
                    'channels_count': len(self.parsed_channels),
                    'emails_count': len(self.parsed_emails),
                    'social_count': len(self.parsed_social_media),
                    'timestamp': datetime.datetime.now().isoformat()
                }
                json.dump(progress, f, indent=2)
            logging.info("Progress saved successfully")
        except Exception as e:
            self._log_error("PROGRESS SAVE ERROR", f"Error saving progress: {e}")
    
    def load_progress(self):
        """Load saved progress if available."""
        if not os.path.exists('progress_data.json'):
            return False
            
        try:
            with open('progress_data.json', 'r', encoding='utf-8') as f:
                progress = json.load(f)
                
            self.processed_keywords = set(progress.get('processed_keywords', []))
            self.all_keywords = progress.get('pending_keywords', [])
            
            # Load API usage data
            api_usage = progress.get('api_usage', {})
            self.api_usage_count = {k: v for k, v in api_usage.items()}
            
            # Load daily quota usage data
            daily_quota = progress.get('daily_quota_usage', {})
            self.daily_quota_usage = {k: v for k, v in daily_quota.items()}
            
            logging.info(f"Progress loaded: {len(self.processed_keywords)} processed keywords, {len(self.all_keywords)} pending keywords")
            return True
        except Exception as e:
            self._log_error("PROGRESS LOAD ERROR", f"Error loading progress: {e}")
            return False
    
    def stop(self):
        """Stop the scraper gracefully."""
        self.stop_requested = True
        logging.info("Stop requested. Scraper will finish current operation and save progress.")
        
    def initialize(self):
        """Initialize the scraper by loading configuration files."""
        try:
            # Load settings, keywords, blacklist, proxies, and API keys
            self.load_settings()
            self.load_keywords()
            self.load_blacklist()
            self.load_proxies()
            
            if not self.load_api_keys():
                logging.error("ERROR: Please add your YouTube API keys to api.txt file.")
                return False
            
            # Load existing data
            self.load_existing_data()
            
            # Try to load saved progress
            self.load_progress()
            
            # Reset daily quota usage if needed
            self.reset_daily_quota_usage()
            
            return True
        except Exception as e:
            logging.error(f"Error initializing scraper: {e}")
            logging.debug(traceback.format_exc())
            return False
            
# Интегрируем обработчик API ключей
YouTubeChannelScraper = integrate_api_key_handler(YouTubeChannelScraper)
