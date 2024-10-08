import asyncio
import logging
import re
import time
from datetime import datetime
from io import BytesIO
from typing import Any

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import insert

from database.database import Session
from models.trading_results import SpimexTradingResults

logger = logging.getLogger(__name__)
logging.basicConfig(format="%(message)s", level=logging.INFO)


def time_execution(func: callable) -> callable:
    """Декоратор для измерения времени выполнения функции.

    Returns:
        callable: Обернутая функция с измерением времени выполнения.

    """

    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        start_time = time.time()
        result = await func(*args, **kwargs)
        end_time = time.time()
        logger.info(
            f"Время выполнения {func.__name__}: "
            f"{end_time - start_time:.2f} секунд"
        )
        return result

    return wrapper


class SpimexParser:
    INSTRUMENT_CODE_COL = "Код\nИнструмента"
    INSTRUMENT_NAME_COL = "Наименование\nИнструмента"
    DELIVERY_BASIS_COL = "Базис\nпоставки"
    VOLUME_COL = "Объем\nДоговоров\nв единицах\nизмерения"
    TOTAL_COL = "Обьем\nДоговоров,\nруб."
    COUNT_COL = "Количество\nДоговоров,\nшт."

    def __init__(self) -> None:
        self.base_url = "https://spimex.com/markets/oil_products/trades/results/?page=page-"

    @time_execution
    async def parse(self) -> None:
        """Основная функция парсинга."""
        links = await self.parse_spimex_results()
        data_to_insert = await self.parse_xls_files_from_links(links)
        await self.insert_data_to_db(data_to_insert)

    @time_execution
    async def parse_spimex_results(self) -> list[str]:
        """Парсит страницы сайта с итогами торгов с начала 2023 года.

        Returns:
            list[str]: Список ссылок на XLS файлы.

        """
        async with aiohttp.ClientSession() as async_session:
            tasks = [
                self.fetch_links(async_session, f"{self.base_url}{page}")
                for page in range(1, 45)
            ]
            results = await asyncio.gather(*tasks)
        return self.collect_xls_links(results)

    @staticmethod
    def collect_xls_links(results: tuple[list[str]]) -> list[str]:
        """Собирает все XLS ссылки из результатов парсинга страниц.

        Returns:
            list[str]: Список собранных XLS ссылок.

        """
        xls_links = []
        for result in results:
            xls_links.extend(result)
        return xls_links

    async def fetch_links(
        self, session: aiohttp.ClientSession, url: str
    ) -> list[str]:
        """Функция для получения ссылок с одной страницы.

        Returns:
            list[str]: Список ссылок на XLS файлы.

        """
        page_content = await self.get_page_content(session, url)
        return self.extract_xls_links_from_page(page_content)

    @staticmethod
    async def get_page_content(
        session: aiohttp.ClientSession, url: str
    ) -> str:
        """Получает содержимое страницы по URL.

        Returns:
            str: Содержимое страницы в виде строки.

        """
        async with session.get(url) as response:
            return await response.text()

    def extract_xls_links_from_page(self, page_content: str) -> list[str]:
        """Извлекает XLS ссылки из содержимого страницы.

        Returns:
            list[str]: Список найденных XLS ссылок.

        """
        soup = BeautifulSoup(page_content, "html.parser")
        links = soup.find_all(
            "a", class_="accordeon-inner__item-title link xls", href=True
        )

        return [
            self.create_full_link(link.attrs.get("href"))
            for link in links
            if self.is_valid_link(link.attrs.get("href"))
        ]

    @staticmethod
    def create_full_link(href: str) -> str:
        """Создает полный адрес ссылки.

        Returns:
            str: Полная ссылка.

        """
        return f"https://spimex.com{href}"

    @staticmethod
    def is_valid_link(href: str) -> bool:
        """Проверяет, является ли ссылка валидной.

        Returns:
            bool: True, если ссылка валидная; иначе False.

        """
        return (
            href
            and href.startswith("/upload")
            and not href.startswith("/upload/reports/oil_xls/oil_xls_2022")
        )

    @time_execution
    async def parse_xls_files_from_links(self, links: list[str]) -> tuple[Any]:
        """Парсит все XLS файлы и возвращает данные для вставки.

        Returns:
            tuple[str, pd.DataFrame]: Дата и соответствующий DataFrame.

        """
        return await asyncio.gather(
            *(self.parse_xls_files(link) for link in links)
        )

    async def parse_xls_files(self, xls_link: str) -> tuple[str, pd.DataFrame]:
        """Парсит бюллетени из формата xls в dataframe.

        Returns:
            tuple[str, pd.DataFrame]: Дата и соответствующий DataFrame.

        """
        async with (
            aiohttp.ClientSession() as async_session,
            async_session.get(xls_link) as response,
        ):
            match = re.search(r"/([^/?]+\.xls)", xls_link)
            table_name = "Единица измерения: Метрическая тонна"

            if match:
                content = await response.read()

                initial_df = pd.read_excel(
                    BytesIO(content), usecols="B", nrows=30
                )
                date = self.get_date_from_df(initial_df)

                metric_row_index = self.find_metric_row_index(
                    initial_df, table_name=table_name
                )
                valid_df = self.process_valid_dataframe(
                    content, metric_row_index
                )
                filtered_df = self.filter_dataframe(valid_df)
                return date, filtered_df
            return "", pd.DataFrame()

    @staticmethod
    def get_date_from_df(df: pd.DataFrame) -> str:
        """Извлекает дату из DataFrame.

        Returns:
            str: Извлеченная дата в строковом формате.

        """
        return df.iat[2, 0].split()[-1]

    @staticmethod
    def find_metric_row_index(df: pd.DataFrame, table_name: str) -> pd.Index:
        """Находит индекс строки с указанной таблицей.

        Returns:
            pd.Index: Индекс строки с таблицей.

        """
        return df[
            df.apply(
                lambda row: row.astype(str).str.contains(table_name).any(),
                axis=1,
            )
        ].index

    @staticmethod
    def process_valid_dataframe(
        content: bytes, metric_row_index: pd.Index
    ) -> pd.DataFrame:
        """Обрабатывает Excel файл, возвращая DataFrame.

        Returns:
            pd.DataFrame: Обработанный DataFrame.

        """
        if not metric_row_index.empty:
            skip_rows = range(metric_row_index[0] + 2)
            return pd.read_excel(
                BytesIO(content), usecols="B:F,O", skiprows=skip_rows
            )
        return pd.DataFrame()

    def filter_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Фильтрует DataFrame по определенным критериям.

        Returns:
            pd.DataFrame: Отфильтрованный DataFrame.

        """
        df[self.INSTRUMENT_CODE_COL] = df[self.INSTRUMENT_CODE_COL].astype(str)

        total_index = df[
            df[self.INSTRUMENT_CODE_COL].str.startswith("Итого")
        ].index
        if not total_index.empty:
            df = df.iloc[: total_index[0]]
        return df[(df[self.COUNT_COL] != "-")].iloc[1:]

    @time_execution
    async def insert_data_to_db(self, data_to_insert: tuple[Any]) -> None:
        """Вставляет данные в БД."""
        all_data = [
            self.create_spimex_trading_results(df, date)
            for date, df in data_to_insert
        ]
        await self.bulk_insert_data_to_db(
            [item for sublist in all_data for item in sublist]
        )

    @staticmethod
    @time_execution
    async def bulk_insert_data_to_db(
        data_list: list[SpimexTradingResults],
    ) -> None:
        """Вставляет результаты торгов в базу данных в пакетном режиме."""
        if not data_list:
            logger.info("Нет данных для вставки.")
            return

        batch_size = 3000
        total_records = len(data_list)

        async with Session() as async_session:
            try:
                for i in range(0, total_records, batch_size):
                    batch = data_list[i : i + batch_size]
                    query = insert(SpimexTradingResults).values(
                        [
                            {
                                "exchange_product_id": result.exchange_product_id,
                                "exchange_product_name": result.exchange_product_name,
                                "oil_id": result.oil_id,
                                "delivery_basis_id": result.delivery_basis_id,
                                "delivery_basis_name": result.delivery_basis_name,
                                "delivery_type_id": result.delivery_type_id,
                                "volume": result.volume,
                                "total": result.total,
                                "count": result.count,
                                "date": result.date,
                            }
                            for result in batch
                        ]
                    )
                    await async_session.execute(query)
                    await async_session.commit()
                    logger.info("Вставлено %s записей.", len(batch))
            except Exception:
                await async_session.rollback()
                logger.exception("Ошибка вставки данных: %s")

    def create_spimex_trading_results(
        self, df: pd.DataFrame, date: str
    ) -> list[SpimexTradingResults]:
        """Создает список объектов SpimexTradingResults из DataFrame.

        Returns:
            list[SpimexTradingResults]: Список созданных объектов.

        """
        parsed_date = datetime.strptime(date, "%d.%m.%Y").astimezone().date()
        return [
            SpimexTradingResults(
                exchange_product_id=row[self.INSTRUMENT_CODE_COL],
                exchange_product_name=row[self.INSTRUMENT_NAME_COL],
                oil_id=row[self.INSTRUMENT_CODE_COL][:4],
                delivery_basis_id=row[self.INSTRUMENT_CODE_COL][4:7],
                delivery_basis_name=row[self.DELIVERY_BASIS_COL],
                delivery_type_id=row[self.INSTRUMENT_CODE_COL][-1],
                volume=float(row[self.VOLUME_COL]),
                total=float(row[self.TOTAL_COL]),
                count=float(row[self.COUNT_COL]),
                date=parsed_date,
            )
            for _, row in df.iterrows()
        ]


async def main() -> None:
    """Основной метод запуска парсера."""
    parser = SpimexParser()
    await parser.parse()
