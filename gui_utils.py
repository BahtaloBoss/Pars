"""
Utilities for YouTube Scraper GUI application.
Contains reusable components for file operations, UI updates, and error handling.
"""

import os
import logging
import traceback
import json
from datetime import datetime
import tkinter as tk
from tkinter import messagebox, filedialog

class FileManager:
    """Manages file operations with consistent error handling."""
    
    @staticmethod
    def load_file(file_path, default_content="", encoding="utf-8"):
        """
        Load content from a file with error handling.
        
        Args:
            file_path: Path to the file to load
            default_content: Content to return if file doesn't exist
            encoding: File encoding
            
        Returns:
            Content of the file or default_content if file doesn't exist
        """
        if not os.path.exists(file_path):
            return default_content
            
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                content = f.read()
            return content
        except Exception as e:
            logging.error(f"Error loading file {file_path}: {str(e)}")
            logging.debug(traceback.format_exc())
            return default_content
    
    @staticmethod
    def load_file_lines(file_path, skip_comments=True, encoding="utf-8"):
        """
        Load lines from a file with error handling.
        
        Args:
            file_path: Path to the file to load
            skip_comments: Whether to skip comment lines (starting with #)
            encoding: File encoding
            
        Returns:
            List of non-empty lines from the file
        """
        if not os.path.exists(file_path):
            return []
            
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                if skip_comments:
                    lines = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
                else:
                    lines = [line.strip() for line in f if line.strip()]
            return lines
        except Exception as e:
            logging.error(f"Error loading lines from file {file_path}: {str(e)}")
            logging.debug(traceback.format_exc())
            return []
    
    @staticmethod
    def save_file(file_path, content, encoding="utf-8"):
        """
        Save content to a file with error handling.
        
        Args:
            file_path: Path to save the file
            content: Content to save
            encoding: File encoding
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Make sure the directory exists
            directory = os.path.dirname(file_path)
            if directory and not os.path.exists(directory):
                os.makedirs(directory)
                
            with open(file_path, 'w', encoding=encoding) as f:
                f.write(content)
            return True
        except Exception as e:
            logging.error(f"Error saving file {file_path}: {str(e)}")
            logging.debug(traceback.format_exc())
            return False
    
    @staticmethod
    def load_json(file_path, default_value=None):
        """
        Load and parse JSON from a file with error handling.
        
        Args:
            file_path: Path to the JSON file
            default_value: Value to return if file doesn't exist or parsing fails
            
        Returns:
            Parsed JSON data or default_value
        """
        if default_value is None:
            default_value = {}
            
        if not os.path.exists(file_path):
            return default_value
            
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading JSON from {file_path}: {str(e)}")
            logging.debug(traceback.format_exc())
            return default_value
    
    @staticmethod
    def save_json(file_path, data, indent=4):
        """
        Save data as JSON to a file with error handling.
        
        Args:
            file_path: Path to save the JSON file
            data: Data to save as JSON
            indent: JSON indentation level
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=indent)
            return True
        except Exception as e:
            logging.error(f"Error saving JSON to {file_path}: {str(e)}")
            logging.debug(traceback.format_exc())
            return False
    
    @staticmethod
    def open_file_dialog(title="Select File", filetypes=None, initialdir=None):
        """
        Open a file selection dialog with error handling.
        
        Args:
            title: Dialog title
            filetypes: List of file types to show
            initialdir: Initial directory
            
        Returns:
            Selected file path or None if canceled or error
        """
        if filetypes is None:
            filetypes = [("All files", "*.*")]
            
        try:
            file_path = filedialog.askopenfilename(
                title=title, 
                filetypes=filetypes,
                initialdir=initialdir
            )
            return file_path
        except Exception as e:
            logging.error(f"Error opening file dialog: {str(e)}")
            logging.debug(traceback.format_exc())
            return None
    
    @staticmethod
    def save_file_dialog(title="Save File", defaultextension=".txt", filetypes=None, initialdir=None):
        """
        Open a save file dialog with error handling.
        
        Args:
            title: Dialog title
            defaultextension: Default file extension
            filetypes: List of file types to show
            initialdir: Initial directory
            
        Returns:
            Selected file path or None if canceled or error
        """
        if filetypes is None:
            filetypes = [("Text files", "*.txt"), ("All files", "*.*")]
            
        try:
            file_path = filedialog.asksaveasfilename(
                title=title, 
                defaultextension=defaultextension,
                filetypes=filetypes,
                initialdir=initialdir
            )
            return file_path
        except Exception as e:
            logging.error(f"Error opening save file dialog: {str(e)}")
            logging.debug(traceback.format_exc())
            return None
    
    @staticmethod
    def ensure_directory(dir_path):
        """Create a directory if it doesn't exist."""
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
            logging.info(f"Created directory: {dir_path}")

class UIHelper:
    """Helper class for common UI operations."""
    
    @staticmethod
    def show_error(title, message, log=True):
        """
        Show an error message box and optionally log the error.
        
        Args:
            title: Message box title
            message: Error message
            log: Whether to log the error
        """
        if log:
            logging.error(message)
        messagebox.showerror(title, message)
    
    @staticmethod
    def show_info(title, message, log=True):
        """
        Show an information message box and optionally log the message.
        
        Args:
            title: Message box title
            message: Information message
            log: Whether to log the message
        """
        if log:
            logging.info(message)
        messagebox.showinfo(title, message)
    
    @staticmethod
    def show_warning(title, message, log=True):
        """
        Show a warning message box and optionally log the warning.
        
        Args:
            title: Message box title
            message: Warning message
            log: Whether to log the warning
        """
        if log:
            logging.warning(message)
        messagebox.showwarning(title, message)
    
    @staticmethod
    def ask_yes_no(title, message, log=True):
        """
        Show a yes/no question dialog and optionally log the question.
        
        Args:
            title: Dialog title
            message: Question message
            log: Whether to log the question
            
        Returns:
            True if yes, False if no
        """
        if log:
            logging.info(f"Asking user: {message}")
        return messagebox.askyesno(title, message)
    
    @staticmethod
    def set_text_content(text_widget, content):
        """
        Set the content of a text widget safely.
        
        Args:
            text_widget: The text widget to update
            content: Content to set
        """
        text_widget.config(state=tk.NORMAL)
        text_widget.delete(1.0, tk.END)
        text_widget.insert(tk.END, content)
        text_widget.config(state=tk.DISABLED)
    
    @staticmethod
    def append_text(text_widget, content, tag=None):
        """
        Append text to a text widget, optionally with a tag.
        
        Args:
            text_widget: The text widget to append to
            content: Content to append
            tag: Optional tag to apply to the text
        """
        text_widget.config(state=tk.NORMAL)
        if tag:
            text_widget.insert(tk.END, content, tag)
        else:
            text_widget.insert(tk.END, content)
        text_widget.see(tk.END)
        text_widget.config(state=tk.DISABLED)

class ErrorHandler:
    """Handles errors with consistent logging and user feedback."""
    
    @staticmethod
    def log_error(error_type, message, exception=None):
        """
        Log an error with detailed information.
        
        Args:
            error_type: Type of error
            message: Error message
            exception: Optional exception object
        """
        logging.error(message)
        
        try:
            with open("debug.txt", "a", encoding="utf-8") as f:
                f.write(f"=== {error_type} AT {datetime.now()} ===\n")
                f.write(f"{message}\n")
                if exception:
                    f.write(f"Exception: {str(exception)}\n")
                    f.write(traceback.format_exc())
                f.write("\n\n")
        except Exception as e:
            logging.error(f"Failed to write to debug.txt: {e}")
    
    @staticmethod
    def handle_exception(func):
        """
        Decorator to handle exceptions in functions.
        
        Args:
            func: Function to decorate
        
        Returns:
            Wrapped function with exception handling
        """
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                error_msg = f"Error in {func.__name__}: {str(e)}"
                logging.error(error_msg)
                
                with open("debug.txt", "a", encoding="utf-8") as f:
                    f.write(f"=== FUNCTION ERROR AT {datetime.now()} ===\n")
                    f.write(f"Function: {func.__name__}\n")
                    f.write(f"Args: {args}, Kwargs: {kwargs}\n")
                    f.write(f"Error: {str(e)}\n")
                    f.write(traceback.format_exc())
                    f.write("\n\n")
                
                # Show error to user
                messagebox.showerror("Error", f"An error occurred: {str(e)}\nSee debug.txt for details.")
                return None
        return wrapper