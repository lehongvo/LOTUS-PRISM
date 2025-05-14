import time
import random
from functools import wraps
from utils.logger import get_logger

logger = get_logger(__name__)

def retry_with_backoff(max_retries=3, backoff_factor=1, max_backoff=60):
    """
    Decorator that retries a function with exponential backoff.
    
    Args:
        max_retries (int): Maximum number of retries
        backoff_factor (int): Initial backoff time in seconds
        max_backoff (int): Maximum backoff time in seconds
        
    Returns:
        function: Decorated function
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries == max_retries:
                        logger.error(f"Max retries ({max_retries}) reached. Last error: {str(e)}")
                        raise
                    
                    # Calculate backoff time with jitter
                    backoff = min(backoff_factor * (2 ** (retries - 1)), max_backoff)
                    jitter = random.uniform(0, 0.1 * backoff)
                    sleep_time = backoff + jitter
                    
                    logger.warning(f"Retry {retries}/{max_retries} after {sleep_time:.2f}s. Error: {str(e)}")
                    time.sleep(sleep_time)
            
            return func(*args, **kwargs)
        return wrapper
    return decorator 