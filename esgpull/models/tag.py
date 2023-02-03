from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

from esgpull.models.base import Base


class Tag(Base):
    __tablename__ = "tag"

    name: Mapped[str] = mapped_column(sa.String(255))
    description: Mapped[str | None] = mapped_column(sa.Text, default=None)

    def _as_bytes(self) -> bytes:
        return self.name.encode()

    def __hash__(self) -> int:
        return hash(self._as_bytes())
