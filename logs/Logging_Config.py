import os 
import logging
from logging.handlers import RotatingFileHandler

# ---------------------- Logging Config ------------------------ #

def setup_logging():

    LOGS_DIR = os.path.join(os.getcwd(), 'logs')
    os.makedirs(LOGS_DIR, exist_ok=True)  

    LOG_FILE_PATH = os.path.join(LOGS_DIR, 'System.log')

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG) # LEVEL 

  
    if not root_logger.handlers:

        file_handler = RotatingFileHandler(
            LOG_FILE_PATH,
            maxBytes=5*1024*1024,  
            backupCount=25,         
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG) # LEVEL

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG) # LEVEL

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)