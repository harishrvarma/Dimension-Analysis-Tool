from sqlalchemy import DateTime
from sqlalchemy.sql import func
from .base import Base


class BaseModel(Base):
    __abstract__ = True

    created_at = DateTime(timezone=True), {"server_default": func.now()}
    updated_at = DateTime(timezone=True), {"onupdate": func.now()}

    # -----------------------------
    # Primary Key Helpers
    # -----------------------------

    def get_primary_key_column(self):
        return list(self.__table__.primary_key.columns)[0]

    def get_primary_key_name(self):
        return self.get_primary_key_column().name

    def get_primary_key_value(self):
        return getattr(self, self.get_primary_key_name())

    # -----------------------------
    # Common Helpers
    # -----------------------------

    def to_dict(self):
        return {
            column.name: getattr(self, column.name)
            for column in self.__table__.columns
        }
