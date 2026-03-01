# core/core_session_manager.py

from flask import session as core_session


class Session:
    
    # ==============================
    # Basic Operations
    # ==============================

    @staticmethod
    def set(key: str, value) -> None:
        core_session[key] = value
        core_session.modified = True

    @staticmethod
    def get(key: str, default=None):
        return core_session.get(key, default)

    @staticmethod
    def has(key: str) -> bool:
        return key in core_session

    @staticmethod
    def remove(key: str) -> None:
        core_session.pop(key, None)
        core_session.modified = True

    @staticmethod
    def clear() -> None:
        core_session.clear()
   

    # ==============================
    # Temporary (One-Time) Data
    # ==============================

    @staticmethod
    def set_temp(key: str, value) -> None:
        core_session[f"_temp_{key}"] = value
        core_session.modified = True

    @staticmethod
    def get_temp(key: str):
        value = core_session.get(f"_temp_{key}")
        core_session.pop(f"_temp_{key}", None)
        return value

    # ==============================
    # core_session Metadata
    # ==============================

    @staticmethod
    def all():
        return dict(core_session)

    @staticmethod
    def get_core_session_id():
        # Only works if server-side core_session is used (Redis etc.)
        return core_session.get("_id")