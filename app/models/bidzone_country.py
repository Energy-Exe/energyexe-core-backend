from sqlalchemy import Boolean, Column, ForeignKey, Integer, Table

from app.core.database import Base

# Association table for many-to-many relationship between bidzones and countries
bidzone_countries = Table(
    "bidzone_countries",
    Base.metadata,
    Column("bidzone_id", Integer, ForeignKey("bidzones.id"), primary_key=True),
    Column("country_id", Integer, ForeignKey("countries.id"), primary_key=True),
    Column("is_primary", Boolean, default=False, nullable=False),
)
