from datetime import date

from sqlalchemy import Date
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base
from utils.custom_types import (
    created_at,
    integer_pk,
    not_nullable_float,
    not_nullable_str,
    updated_at,
)


class SpimexTradingResults(Base):
    __tablename__ = "spimex_trading_results"

    id: Mapped[integer_pk]
    exchange_product_id: Mapped[not_nullable_str]
    exchange_product_name: Mapped[not_nullable_str]
    oil_id: Mapped[not_nullable_str]
    delivery_basis_id: Mapped[not_nullable_str]
    delivery_basis_name: Mapped[not_nullable_str]
    delivery_type_id: Mapped[not_nullable_str]
    volume: Mapped[not_nullable_float]
    total: Mapped[not_nullable_float]
    count: Mapped[not_nullable_float]
    date: Mapped[date] = mapped_column(Date, nullable=False)
    created_on: Mapped[updated_at]
    updated_on: Mapped[created_at]
