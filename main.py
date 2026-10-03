"""Launcher: run the gateway with uvicorn.

    UPSTREAM_BASE_URL=http://upstream:8080 uvicorn main:app
"""

from app.main import app

__all__ = ["app"]
