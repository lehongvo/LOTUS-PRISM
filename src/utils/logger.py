import logging
import os
from datetime import datetime

def get_logger(name, log_level=None):
    """
    Create and configure a logger instance.
    
    Args:
        name (str): Name of the logger
        log_level (str, optional): Logging level. Defaults to LOG_LEVEL from environment or INFO.
    
    Returns:
        logging.Logger: Configured logger instance
    """
    # Get log level from environment or use default
    if log_level is None:
        log_level = os.getenv('LOG_LEVEL', 'INFO')
    
    # Convert string log level to logging constant
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(numeric_level)
    
    # Create handlers if they don't exist
    if not logger.handlers:
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(numeric_level)
        
        # File handler
        os.makedirs('logs', exist_ok=True)
        log_file = os.getenv('LOG_FILE', 'logs/scraper.log')
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(numeric_level)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Add formatter to handlers
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)
        
        # Add handlers to logger
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
    
    return logger 