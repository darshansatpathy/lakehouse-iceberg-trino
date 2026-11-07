"""Run the dbt project against Trino."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

from lakehouse.resources import resource
from lakehouse.settings import Settings


class DbtError(RuntimeError):
    pass


def dbt_executable() -> str:
    local = Path(sys.executable).parent / "dbt"
    return str(local) if local.exists() else shutil.which("dbt") or "dbt"


def build(settings: Settings, select: str | None = None, quiet: bool = True) -> str:
    project = resource("transform")
    args = [dbt_executable(), "build", "--project-dir", str(project), "--profiles-dir", str(project)]
    if select:
        args += ["--select", select]
    if quiet:
        args.append("--quiet")
    env = {**os.environ, **settings.dbt_env()}
    proc = subprocess.run(args, capture_output=True, text=True, env=env, check=False)  # noqa: S603 - fixed arguments
    if proc.returncode != 0:
        raise DbtError(f"dbt build failed:\n{proc.stdout[-4000:]}\n{proc.stderr[-2000:]}")
    return proc.stdout
