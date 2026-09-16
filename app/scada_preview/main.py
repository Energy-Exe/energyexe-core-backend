"""Minimal local-only preview. Deliberately independent of app.main and app.core."""

import secrets
import time
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from starlette.middleware.trustedhost import TrustedHostMiddleware

from app.scada_preview.config import PreviewSettings
from app.scada_preview.schemas import IngestionSummary
from app.scada_preview.service import IngestionSummaryService

PREVIEW_USER = {
    "id": 1,
    "username": "admin",
    "email": "admin@sfe-preview.invalid",
    "first_name": "SFE",
    "last_name": "Preview",
    "role": "admin",
    "status": "active",
    "is_active": True,
    "is_superuser": True,
    "is_approved": True,
    "email_verified": True,
    "created_at": "2026-09-16T00:00:00Z",
    "updated_at": "2026-09-16T00:00:00Z",
    "features": {},
}
bearer = HTTPBearer(auto_error=False)


class Login(BaseModel):
    username: str = Field(max_length=100)
    password: str = Field(max_length=100)


async def get_preview_db(request: Request):
    async with request.app.state.sessions() as session:
        # Even an accidental write in a future read service fails at the DB layer.
        from sqlalchemy import text

        await session.execute(text("SET TRANSACTION READ ONLY"))
        yield session


async def preview_superuser(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
):
    try:
        if credentials is None:
            raise ValueError("Missing bearer token")
        claims = jwt.decode(
            credentials.credentials,
            request.app.state.token_key,
            algorithms=["HS256"],
            audience="sfe-local-preview",
            issuer="sfe-local-preview",
            options={"require_exp": True, "require_sub": True},
        )
        if claims["sub"] != "admin" or claims.get("session") != request.app.state.session_id:
            raise ValueError("Invalid preview session")
    except (JWTError, ValueError):
        raise HTTPException(
            401, "Local preview login required", headers={"WWW-Authenticate": "Bearer"}
        ) from None
    return PREVIEW_USER


def create_preview_app(settings: PreviewSettings | None = None) -> FastAPI:
    settings = settings or PreviewSettings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engine = create_async_engine(settings.database_url, pool_pre_ping=True)
        app.state.sessions = async_sessionmaker(engine, expire_on_commit=False)
        try:
            yield
        finally:
            await engine.dispose()

    app = FastAPI(title="SFE isolated local preview", lifespan=lifespan)
    app.state.token_key = secrets.token_urlsafe(48)
    app.state.session_id = secrets.token_hex(16)
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=["127.0.0.1", "localhost", "[::1]"])
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://127.0.0.1:3016", "http://localhost:3016"],
        allow_credentials=False,
        allow_methods=["GET", "POST"],
        allow_headers=["Authorization", "Content-Type"],
    )

    @app.get("/health")
    async def health():
        return {"status": "ok", "profile": "sfe-local-preview", "agents_enabled": False}

    @app.post("/api/v1/auth/login")
    async def login(data: Login):
        if not (
            secrets.compare_digest(data.username.encode(), b"admin")
            and secrets.compare_digest(data.password.encode(), b"adminenergyexe")
        ):
            raise HTTPException(401, "Incorrect local fixture credentials")
        token = jwt.encode(
            {
                "sub": "admin",
                "exp": int(time.time()) + 3600,
                "aud": "sfe-local-preview",
                "iss": "sfe-local-preview",
                "session": app.state.session_id,
            },
            app.state.token_key,
            algorithm="HS256",
        )
        return {"access_token": token, "token_type": "bearer", "user": PREVIEW_USER}

    @app.get("/api/v1/auth/me")
    @app.get("/api/v1/users/me")
    async def me(user=Depends(preview_superuser)):
        return user

    @app.post("/api/v1/auth/logout")
    async def logout(user=Depends(preview_superuser)):
        app.state.session_id = secrets.token_hex(16)
        return {"message": "Local preview sessions cleared"}

    @app.get("/api/v1/scada/farms")
    async def farms(user=Depends(preview_superuser), db: AsyncSession = Depends(get_preview_db)):
        return await IngestionSummaryService(db).farms()

    @app.get("/api/v1/scada/ingestion-summary", response_model=IngestionSummary)
    async def ingestion_summary(
        farm: str = Query("lutelandet", min_length=1, max_length=100, pattern=r"^[a-z0-9_]+$"),
        run_id: str | None = Query(None, min_length=1, max_length=128),
        user=Depends(preview_superuser),
        db: AsyncSession = Depends(get_preview_db),
    ):
        return await IngestionSummaryService(db).get(farm, run_id)

    @app.api_route("/api/v1/scada/{unsupported:path}", methods=["GET", "POST"])
    async def unsupported_scada(unsupported: str, user=Depends(preview_superuser)):
        raise HTTPException(409, "This analysis is unavailable in the measured SFE preview")

    return app


if __name__ == "__main__":
    import uvicorn

    settings = PreviewSettings.from_env()
    uvicorn.run(create_preview_app(settings), host=settings.host, port=settings.port)
