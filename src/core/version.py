from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

BASE_VERSION = "0.1.0"


def is_dev_build() -> bool:
    override = os.environ.get("SNAPCAPSULE_DEV")
    if override is not None:
        return override.strip().lower() in {"1", "true", "yes", "on"}

    release_flag = os.environ.get("SNAPCAPSULE_RELEASE")
    if release_flag is not None:
        return release_flag.strip().lower() not in {"1", "true", "yes", "on"}

    if getattr(sys, "frozen", False):
        return False

    return True


BUILD_CHANNEL = "dev" if is_dev_build() else "release"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _get_git_hash() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(_repo_root()),
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip() or "nogit"
    except Exception:
        return "nogit"


def _build_metadata() -> str:
    stamp = datetime.utcnow().strftime("%Y%m%d")
    short_hash = _get_git_hash()
    return f"{stamp}.g{short_hash}"


def app_version() -> str:
    if is_dev_build():
        return f"{BASE_VERSION}+{_build_metadata()}"
    return BASE_VERSION


APP_VERSION = app_version()
