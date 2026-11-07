from __future__ import annotations

import os

import pytest

from lakehouse.settings import Settings


@pytest.fixture(scope="session")
def settings() -> Settings:
    """Settings for a running stack (docker compose up), or skip."""
    if not os.environ.get("LH_INTEGRATION"):
        pytest.skip("LH_INTEGRATION is not set; start the stack with docker compose and set it to 1")
    return Settings.from_env()
