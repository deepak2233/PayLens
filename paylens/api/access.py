"""Private analytics access. Public replays never touch the live store."""
from __future__ import annotations

import os
from secrets import compare_digest
from typing import Annotated

from fastapi import HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from paylens.config import SETTINGS

HOSTED = os.getenv("VERCEL") == "1"
_bearer = HTTPBearer(auto_error=False)


def require_access(credentials: Annotated[HTTPAuthorizationCredentials | None, Security(_bearer)]):
    if not SETTINGS.api_token:
        if HOSTED or SETTINGS.is_postgres:
            raise HTTPException(503, "Private analytics is disabled on this deployment")
        return
    if credentials is None or not compare_digest(credentials.credentials.encode(), SETTINGS.api_token.encode()):
        raise HTTPException(401, "Analytics access token required", headers={"WWW-Authenticate": "Bearer"})


def require_live():
    if not SETTINGS.is_postgres or not SETTINGS.analytics_database_url:
        raise HTTPException(503, "Live analytics requires PostgreSQL and a read-only analytics connection")
