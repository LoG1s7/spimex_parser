from datetime import datetime

from sqlalchemy import TIMESTAMP, Column, Date, Float, Integer, String

from database.database import Base


class SpimexTradingResults(Base):
    __tablename__ = "spimex_trading_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange_product_id = Column(String, nullable=False)
    exchange_product_name = Column(String, nullable=False)
    oil_id = Column(String, nullable=False)
    delivery_basis_id = Column(String, nullable=False)
    delivery_basis_name = Column(String, nullable=False)
    delivery_type_id = Column(String, nullable=False)
    volume = Column(Float, nullable=False)
    total = Column(Float, nullable=False)
    count = Column(Float, nullable=False)
    date = Column(Date, nullable=False)
    created_on = Column(TIMESTAMP, default=datetime.utcnow)
    updated_on = Column(
        TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow
    )
