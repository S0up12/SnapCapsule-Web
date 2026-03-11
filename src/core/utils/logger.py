import logging
import sys
from logging.handlers import RotatingFileHandler

from core.utils.paths import get_app_data_dir

class FlushRotatingFileHandler(RotatingFileHandler):
    def emit(self, record):
        super().emit(record)
        try:
            self.flush()
        except Exception:
            pass

def get_logger(module_name: str) -> logging.Logger:
    logger = logging.getLogger(f"SnapCapsule.{module_name}")
    
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        
        # Format: Time - Module - Level - Message
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        # Console Handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # File Handler (Rotates at 5MB)
        # Added delay=False to create file immediately
        log_file = get_app_data_dir() / "app.log"
        file_handler = FlushRotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=3, delay=False)
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        
    return logger
