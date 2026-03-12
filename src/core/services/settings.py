from __future__ import annotations

from core.database.schema import DatabaseManager


class SettingsManager:
    def __init__(self, db: DatabaseManager):
        self.db = db

    def get_auto_import_enabled(self) -> bool:
        value = self.db.get_config("auto_import_enabled")
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    def set_auto_import_enabled(self, enabled: bool) -> bool:
        self.db.set_config("auto_import_enabled", "true" if enabled else "false")
        return self.get_auto_import_enabled()

    def get_settings(self) -> dict[str, bool]:
        return {
            "auto_import_enabled": self.get_auto_import_enabled(),
        }
