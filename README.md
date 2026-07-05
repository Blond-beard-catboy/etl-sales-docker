# ETL-пайплайн для данных о продажах с Docker и PostgreSQL

## Описание
Проект реализует ETL-процесс:
- Извлечение (Extract) из CSV-файла `retail_sales_dataset.csv`
- Трансформация (Transform) с помощью Pandas (очистка, проверка расчётов, агрегация)
- Загрузка (Load) в PostgreSQL, запущенную в контейнере Docker

## Технологии
- Python 3.10+
- Pandas, SQLAlchemy, psycopg2
- Docker & docker-compose
- PostgreSQL 15

## Как запустить
1. Скопируйте `.env.example` в `.env` и при необходимости измените пароли.
2. Запустите контейнер с БД:  
   `docker-compose up -d`
3. Установите зависимости:  
   `pip install -r requirements.txt`
4. Запустите ETL-скрипт:  
   `python src/etl_pipeline.py`

Логи сохраняются в `logs/etl_pipeline.log`.