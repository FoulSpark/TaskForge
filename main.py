"""Entrypoint script for TaskForge deployment (e.g. on Render, Fly.io, Railway, Docker, or local).

This file exposes the FastAPI application instance `app` and can also be
executed directly via:
    python main.py
which starts uvicorn on the host and port specified by environment variables
(PORT, API_PORT, API_HOST) or defaults to 0.0.0.0:8000.
"""

import os
import sys
from pathlib import Path

# Ensure 'src' directory is in Python path for package resolution
SRC_DIR = Path(__file__).resolve().parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import uvicorn
from taskforge.main import app  # noqa: E402

if __name__ == "__main__":
    from taskforge.core.config import get_settings

    settings = get_settings()

    # Port precedence: $PORT (Render/Heroku/Fly.io) -> settings.api_port (from .env/default)
    port = int(os.environ.get("PORT", settings.api_port))
    # Host precedence: $API_HOST -> settings.api_host (fallback to 0.0.0.0 for public accessibility)
    host = os.environ.get("API_HOST", settings.api_host if settings.app_env != "production" else "0.0.0.0")

    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=False,
    )
