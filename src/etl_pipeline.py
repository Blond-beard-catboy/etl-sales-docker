"""
ETL Pipeline for Retail Sales Dataset
- Extract: CSV file
- Transform: pandas (cleaning, validation, enrichment, column normalization)
- Load: PostgreSQL in Docker
- Analytics & Visualization
"""

import os
import sys
import logging
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path

# ------------------------------------------------------------
# 1. Загрузка переменных окружения
# ------------------------------------------------------------
load_dotenv()

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'sales_db')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'etl_user')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'etl_pass')

# ------------------------------------------------------------
# 2. Настройка логирования (сквозное)
# ------------------------------------------------------------
LOG_DIR = Path('logs')
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'etl_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------
# 3. Константы и пути
# ------------------------------------------------------------
DATA_DIR = Path('data')
OUTPUT_DIR = Path('outputs')
OUTPUT_DIR.mkdir(exist_ok=True)

CSV_FILE = DATA_DIR / 'retail_sales_dataset.csv'
TABLE_NAME = 'sales'

# ------------------------------------------------------------
# 4. Функции этапов
# ------------------------------------------------------------

def extract() -> pd.DataFrame:
    """Этап 1: Извлечение и валидация CSV"""
    logger.info("=== Этап 1: Извлечение данных ===")
    if not CSV_FILE.exists():
        logger.error(f"Файл не найден: {CSV_FILE}")
        sys.exit(1)
    
    try:
        df = pd.read_csv(CSV_FILE)
        logger.info(f"Файл прочитан. Строк: {len(df)}, колонок: {len(df.columns)}")
    except Exception as e:
        logger.error(f"Ошибка чтения CSV: {e}")
        sys.exit(1)
    
    expected_cols = [
        'Transaction ID', 'Date', 'Customer ID', 'Gender', 'Age',
        'Product Category', 'Quantity', 'Price per Unit', 'Total Amount'
    ]
    missing = set(expected_cols) - set(df.columns)
    if missing:
        logger.warning(f"Отсутствуют колонки: {missing}. Продолжаем, но возможны ошибки.")
    
    if df.isnull().values.any():
        logger.warning("Обнаружены пропуски. Будут обработаны на этапе трансформации.")
    
    logger.info(f"Этап 1 завершён. Размер данных: {df.shape}")
    return df


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Приводит имена колонок к snake_case (нижний регистр + подчёркивания)"""
    df.columns = [col.lower().strip().replace(' ', '_') for col in df.columns]
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Этап 2: Трансформация и очистка"""
    logger.info("=== Этап 2: Трансформация данных ===")
    initial_rows = len(df)
    
    before_dedup = len(df)
    df = df.drop_duplicates(subset=['Transaction ID'])
    after_dedup = len(df)
    logger.info(f"Удалено дубликатов транзакций: {before_dedup - after_dedup}")
    
    df['calc_total'] = df['Quantity'] * df['Price per Unit']
    mismatches = df[df['Total Amount'] != df['calc_total']]
    if not mismatches.empty:
        logger.warning(f"Обнаружено {len(mismatches)} записей с несовпадением суммы. Исправляем.")
        df['Total Amount'] = df['calc_total']
    df.drop(columns=['calc_total'], inplace=True)
    
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    if df['Date'].isnull().any():
        logger.warning("Некорректные даты будут заменены на 1970-01-01")
        df['Date'] = df['Date'].fillna(pd.Timestamp('1970-01-01'))
    
    df['Age'] = df['Age'].astype(int)
    df['Quantity'] = df['Quantity'].astype(int)
    df['Price per Unit'] = df['Price per Unit'].astype(float)
    df['Total Amount'] = df['Total Amount'].astype(float)
    
    before_filter = len(df)
    df = df[(df['Price per Unit'] > 0) & (df['Quantity'] > 0)]
    after_filter = len(df)
    if before_filter - after_filter > 0:
        logger.warning(f"Удалено {before_filter - after_filter} записей с некорректными ценой/количеством")
    
    df['Year'] = df['Date'].dt.year
    df['Month'] = df['Date'].dt.month
    df['Weekday'] = df['Date'].dt.dayofweek
    
    # Нормализация имён колонок в snake_case
    df = normalize_columns(df)
    
    logger.info(f"Этап 2 завершён. Исходно: {initial_rows}, после очистки: {len(df)}")
    return df


def load_to_db(df: pd.DataFrame):
    """Этапы 3-5: Подключение, создание таблицы, загрузка"""
    logger.info("=== Этап 3: Подключение к БД ===")
    try:
        engine = create_engine(
            f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'
        )
        with engine.connect() as conn:
            logger.info(f"Подключение к БД {POSTGRES_DB} на {POSTGRES_HOST}:{POSTGRES_PORT} успешно.")
    except Exception as e:
        logger.error(f"Не удалось подключиться к БД: {e}")
        sys.exit(1)
    
    logger.info("=== Этап 4: Подготовка таблицы ===")
    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TABLE_NAME} CASCADE"))
        conn.commit()
        logger.info(f"Таблица {TABLE_NAME} удалена (если существовала).")
    
    logger.info("=== Этап 5: Загрузка данных ===")
    try:
        df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False, method=None)
        logger.info(f"Загружено {len(df)} записей в таблицу {TABLE_NAME}")
    except Exception as e:
        logger.error(f"Ошибка загрузки: {e}")
        sys.exit(1)
    
    logger.info("Загрузка завершена.")
    return engine


def analyze_and_visualize(engine):
    """Этап 6: Аналитика и визуализация"""
    logger.info("=== Этап 6: Аналитика и визуализация ===")
    
    queries = {
        'total_revenue': """
            SELECT SUM(total_amount) AS total_revenue FROM sales;
        """,
        'top_categories': """
            SELECT product_category, SUM(total_amount) AS revenue
            FROM sales
            GROUP BY product_category
            ORDER BY revenue DESC;
        """,
        'avg_by_gender': """
            SELECT gender, AVG(total_amount) AS avg_transaction
            FROM sales
            GROUP BY gender;
        """,
        'monthly_sales': """
            SELECT year, month, SUM(total_amount) AS monthly_revenue
            FROM sales
            GROUP BY year, month
            ORDER BY year, month;
        """
    }
    
    results = {}
    with engine.connect() as conn:
        for name, sql in queries.items():
            try:
                df_res = pd.read_sql(sql, conn)
                results[name] = df_res
                logger.info(f"Запрос '{name}' выполнен, записей: {len(df_res)}")
            except Exception as e:
                logger.error(f"Ошибка запроса {name}: {e}")
                conn.rollback()
    
    for name, df_res in results.items():
        if not df_res.empty:
            df_res.to_csv(OUTPUT_DIR / f'{name}.csv', index=False)
            logger.info(f"Результат '{name}' сохранён в {OUTPUT_DIR / f'{name}.csv'}")
    
    if 'top_categories' in results and not results['top_categories'].empty:
        df_cat = results['top_categories']
        plt.figure(figsize=(10, 6))
        plt.bar(df_cat['product_category'], df_cat['revenue'], color='skyblue')
        plt.title('Выручка по категориям товаров')
        plt.xlabel('Категория')
        plt.ylabel('Выручка (руб.)')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plot_path = OUTPUT_DIR / 'top_categories.png'
        plt.savefig(plot_path)
        logger.info(f"График сохранён: {plot_path}")
        plt.close()
    
    if 'monthly_sales' in results and not results['monthly_sales'].empty:
        df_month = results['monthly_sales']
        df_month['date'] = pd.to_datetime(df_month['year'].astype(str) + '-' + df_month['month'].astype(str) + '-01')
        plt.figure(figsize=(12, 6))
        plt.plot(df_month['date'], df_month['monthly_revenue'], marker='o', linestyle='-')
        plt.title('Динамика выручки по месяцам')
        plt.xlabel('Месяц')
        plt.ylabel('Выручка (руб.)')
        plt.grid(True)
        plt.tight_layout()
        plot_path = OUTPUT_DIR / 'monthly_sales.png'
        plt.savefig(plot_path)
        logger.info(f"График сохранён: {plot_path}")
        plt.close()
    
    logger.info("Этап 6 завершён.")


def finalize():
    """Этап 7: Завершение"""
    logger.info("=== Этап 7: Завершение работы ===")
    logger.info("ETL-пайплайн выполнен успешно.")
    logger.info("Логи сохранены в logs/etl_pipeline.log")
    logger.info("Результаты и графики в папке outputs/")


# ------------------------------------------------------------
# 5. Основной пайплайн
# ------------------------------------------------------------
def main():
    logger.info("===== ЗАПУСК ETL-ПАЙПЛАЙНА =====")
    try:
        df_raw = extract()
        df_clean = transform(df_raw)
        engine = load_to_db(df_clean)
        analyze_and_visualize(engine)
        finalize()
    except Exception as e:
        logger.exception("Критическая ошибка в пайплайне")
        sys.exit(1)
    logger.info("===== ETL-ПАЙПЛАЙН ЗАВЕРШЁН =====")


if __name__ == "__main__":
    main()