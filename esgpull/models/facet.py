import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

from esgpull.models.base import Base


class Facet(Base):
    __tablename__ = "facet"

    name: Mapped[str] = mapped_column(sa.String(64))
    value: Mapped[str] = mapped_column(sa.String(255))

    def _as_bytes(self) -> bytes:
        self_tuple = (self.name, self.value)
        return str(self_tuple).encode()

    def __hash__(self) -> int:
        return hash(self._as_bytes())
