from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, String
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from ...base import Base


class Expenses(Base):
    """
    Modelo ORM de Salario.
    Representa un registro de salario en la base de datos.
    """

    __tablename__ = "expenses"
    __table_args__ = {"schema": "people"}

    # ---- Columns ----
    id: Mapped[int] = mapped_column(primary_key=True)
    employee_id: Mapped[int] = mapped_column(nullable=False)
    payment_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    distance: Mapped[float] = mapped_column(nullable=False)
    transport: Mapped[float] = mapped_column(nullable=False)
    food: Mapped[float] = mapped_column(nullable=False)
    sleep: Mapped[float] = mapped_column(nullable=False)
    aud_creation_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    aud_user_creation: Mapped[Optional[str]] = mapped_column(
        String(150), nullable=True
    )
