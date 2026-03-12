from __future__ import annotations

from pydantic import BaseModel
from fastapi import APIRouter, Depends

from core.services.settings import SettingsManager
from routers.dependencies import get_settings_manager

router = APIRouter(prefix="/api/settings", tags=["settings"])


class SettingsPayload(BaseModel):
    auto_import_enabled: bool


@router.get("/")
def get_settings(
    settings: SettingsManager = Depends(get_settings_manager),
) -> dict[str, bool]:
    return settings.get_settings()


@router.post("/")
def update_settings(
    payload: SettingsPayload,
    settings: SettingsManager = Depends(get_settings_manager),
) -> dict[str, bool]:
    enabled = settings.set_auto_import_enabled(payload.auto_import_enabled)
    return {"auto_import_enabled": enabled}
