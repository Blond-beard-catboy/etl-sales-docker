import logging
import os
import sys
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def main():
    logger.info("=== ETL Pipeline START ===")
    # Здесь будет код всех этапов
    logger.info("=== ETL Pipeline FINISH ===")

if __name__ == "__main__":
    main()