"""TaskForge Development Server Launcher

Starts TaskForge with Uvicorn, enabling live code reloading,
automatic database migrations, and active background worker processing.
"""
import sys
from pathlib import Path

# Ensure 'src' is discoverable on python path
SRC_DIR = Path(__file__).resolve().parent.parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import uvicorn
from taskforge.core.config import get_settings


def main():
    settings = get_settings()
    print("=" * 60)
    print(f"Starting {settings.app_name} on http://{settings.api_host}:{settings.api_port}")
    print(f"Mission Control Dashboard: http://{settings.api_host}:{settings.api_port}/ui")
    print(f"Interactive Swagger Docs: http://{settings.api_host}:{settings.api_port}/docs")
    print(f"Prometheus Metrics:       http://{settings.api_host}:{settings.api_port}/metrics")
    print("=" * 60)

    uvicorn.run(
        "taskforge.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.debug,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
