import asyncio
import re
import time
from datetime import datetime
from io import BytesIO

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

from database.database import create_db, session
from database.models import SpimexTradingResults


def time_execution(func):
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        result = await func(*args, **kwargs)
        end_time = time.time()
        print(
            f"Время выполнения {func.__name__}:"
            f" {end_time - start_time:.2f} секунд"
        )
        return result

    return wrapper


@time_execution
async def create_database(parse_spimex_results: bool = True):
    create_db()
    if parse_spimex_results:
        links = await _parse_spimex_results()
        data_to_insert = await asyncio.gather(
            *(parse_xls_files(link) for link in links)
        )
        all_data = [
            create_spimex_trading_results(df, date)
            for date, df in data_to_insert
        ]
        await insert_data_to_db_bulk(
            [item for sublist in all_data for item in sublist]
        )
    session.close()


@time_execution
async def insert_data_to_db_bulk(
    data_list: list[SpimexTradingResults],
) -> None:
    """Вставляет результаты торгов в базу данных в пакетном режиме."""
    try:
        session.bulk_save_objects(data_list)
        session.commit()
        print(f"Вставлено {len(data_list)} записей.")
    except Exception as e:
        session.rollback()
        print(f"Ошибка вставки данных: {e}")


def create_spimex_trading_results(
    df: pd.DataFrame, date: str
) -> list[SpimexTradingResults]:
    """Создает список объектов SpimexTradingResults из DataFrame."""
    return [
        SpimexTradingResults(
            exchange_product_id=row["Код\nИнструмента"],
            exchange_product_name=row["Наименование\nИнструмента"],
            oil_id=row["Код\nИнструмента"][:4],
            delivery_basis_id=row["Код\nИнструмента"][4:7],
            delivery_basis_name=row["Базис\nпоставки"],
            delivery_type_id=row["Код\nИнструмента"][-1],
            volume=float(row["Объем\nДоговоров\nв единицах\nизмерения"]),
            total=float(row["Обьем\nДоговоров,\nруб."]),
            count=float(row["Количество\nДоговоров,\nшт."]),
            date=datetime.strptime(date, "%d.%m.%Y").date(),
        )
        for _, row in df.iterrows()
    ]


@time_execution
async def _parse_spimex_results() -> list[str]:
    """Парсит страницы сайта с итогами торгов с начала 2023 года."""
    base_url = (
        "https://spimex.com/markets/oil_products/trades/results/?page=page-"
    )
    xls_links = []

    async with aiohttp.ClientSession() as async_session:
        tasks = []
        for page in range(1, 44):
            url = f"{base_url}{page}"
            tasks.append(fetch_links(async_session, url))

        results = await asyncio.gather(*tasks)
        for result in results:
            xls_links.extend(result)

    return xls_links


async def fetch_links(session, url) -> list[str]:
    """Функция для получения ссылок с одной страницы."""
    async with session.get(url) as response:
        soup = BeautifulSoup(await response.text(), "html.parser")
        return [
            f"https://spimex.com{a.attrs.get('href')}"
            for a in soup.find_all(
                "a", class_="accordeon-inner__item-title link xls", href=True
            )
            if (
                a.attrs.get("href").startswith("/upload")
                and not a.attrs.get("href").startswith(
                    "/upload/reports/oil_xls/oil_xls_2022"
                )
            )
        ]


@time_execution
async def parse_xls_files(xls_link: str) -> tuple[str, pd.DataFrame]:
    """Парсит бюллетени из формата xls в dataframe."""
    async with aiohttp.ClientSession() as async_session:
        async with async_session.get(xls_link) as xls_response:
            match = re.search(r"/([^/?]+\.xls)", xls_link)
            table_name = "Единица измерения: Метрическая тонна"

            if match:
                content = await xls_response.read()
                initial_df = pd.read_excel(
                    BytesIO(content), usecols="B", nrows=30
                )
                date = initial_df.iat[2, 0].split()[-1]

                metric_row_index = initial_df[
                    initial_df.apply(
                        lambda row: row.astype(str)
                        .str.contains(table_name)
                        .any(),
                        axis=1,
                    )
                ].index

                if not metric_row_index.empty:
                    skip_rows = range(metric_row_index[0] + 2)
                    df = pd.read_excel(
                        BytesIO(content), usecols="B:F,O", skiprows=skip_rows
                    )
                else:
                    df = pd.DataFrame()

                df["Код\nИнструмента"] = df["Код\nИнструмента"].astype(str)
                filtered_df = df[
                    (df["Количество\nДоговоров,\nшт."] != "-")
                    & (~df["Код\nИнструмента"].str.startswith("Итого"))
                ].iloc[1:]

                return date, filtered_df
        return "", pd.DataFrame()
