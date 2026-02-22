from sqlalchemy import text, insert
from sqlalchemy import and_, or_
from sqlalchemy.orm import Session


class BaseRepository:

    def __init__(self, db: Session, model):
        self.db = db
        self.model = model
        self.pk_column = list(model.__table__.primary_key.columns)[0]

    # =====================================================
    # RAW QUERY METHODS
    # =====================================================

    def fetch_all(self, query: str, params: dict = None):
        result = self.db.execute(text(query), params or {})
        return result.fetchall()

    def fetch_row(self, query: str, params: dict = None):
        result = self.db.execute(text(query), params or {})
        return result.first()

    def fetch_one(self, query: str, params: dict = None):
        result = self.db.execute(text(query), params or {})
        return result.scalar()

    # =====================================================
    # BASIC CRUD (Dynamic Primary Key)
    # =====================================================

    def load(self, pk_value):
        return self.db.get(self.model, pk_value)

    def insert(self, data: dict):
        stmt = insert(self.model).values(**data)
        result = self.db.execute(stmt)
        self.db.commit()
        return result.inserted_primary_key

    def insert_multiple(self, data_list: list[dict]):
        stmt = insert(self.model)
        self.db.execute(stmt, data_list)
        self.db.commit()
        return True

    def update_by_pk(self, pk_value, data: dict):
        result = (
            self.db.query(self.model)
            .filter(self.pk_column == pk_value)
            .update(data, synchronize_session=False)
        )
        self.db.commit()
        return result

    def delete_by_pk(self, pk_value):
        result = (
            self.db.query(self.model)
            .filter(self.pk_column == pk_value)
            .delete(synchronize_session=False)
        )
        self.db.commit()
        return result

    # =====================================================
    # SEARCH CRITERIA ENGINE
    # =====================================================

    def _build_condition(self, field, operator, value):
        column = getattr(self.model, field)

        if operator == "=":
            return column == value
        elif operator == "!=":
            return column != value
        elif operator == ">":
            return column > value
        elif operator == "<":
            return column < value
        elif operator == ">=":
            return column >= value
        elif operator == "<=":
            return column <= value
        elif operator == "like":
            return column.like(value)
        elif operator == "ilike":
            return column.ilike(value)
        elif operator == "in":
            return column.in_(value)
        elif operator == "not_in":
            return ~column.in_(value)
        elif operator == "between":
            return column.between(value[0], value[1])
        elif operator == "is_null":
            return column.is_(None)
        else:
            raise ValueError(f"Unsupported operator: {operator}")

    def _build_criteria(self, criteria: dict):

        and_conditions = [
            self._build_condition(c["field"], c["operator"], c["value"])
            for c in criteria.get("and", [])
        ]

        or_conditions = [
            self._build_condition(c["field"], c["operator"], c["value"])
            for c in criteria.get("or", [])
        ]

        if and_conditions and or_conditions:
            return or_(
                and_(*and_conditions),
                or_(*or_conditions)
            )
        elif and_conditions:
            return and_(*and_conditions)
        elif or_conditions:
            return or_(*or_conditions)
        else:
            return None

    def update_criteria(self, criteria: dict, data: dict):
        condition = self._build_criteria(criteria)

        result = (
            self.db.query(self.model)
            .filter(condition)
            .update(data, synchronize_session=False)
        )

        self.db.commit()
        return result

    def delete_criteria(self, criteria: dict):
        condition = self._build_criteria(criteria)

        result = (
            self.db.query(self.model)
            .filter(condition)
            .delete(synchronize_session=False)
        )

        self.db.commit()
        return result
