"""Gzip for complete responses only; streamed responses (SSE, exports) stay as sent."""

import asyncio

from fastapi import FastAPI
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.testclient import TestClient

from app.core.middleware import GZipWholeResponseMiddleware, LoggingMiddleware
from app.main import create_application

BIG = {"farms": [{"id": i, "name": f"Wind farm {i}"} for i in range(500)]}


def _app(*, with_logging: bool = False) -> FastAPI:
    app = FastAPI()

    @app.get("/big")
    async def big():
        return JSONResponse(BIG)

    @app.get("/small")
    async def small():
        return {"ok": True}

    @app.get("/events")
    async def events():
        async def gen():
            for i in range(3):
                yield f"data: {i}\n\n"
                await asyncio.sleep(0)

        return StreamingResponse(gen(), media_type="text/event-stream")

    app.add_middleware(GZipWholeResponseMiddleware)
    if with_logging:
        app.add_middleware(LoggingMiddleware)
    return app


def test_large_json_is_gzipped():
    response = TestClient(_app()).get("/big", headers={"Accept-Encoding": "gzip"})
    assert response.headers["content-encoding"] == "gzip"
    assert "Accept-Encoding" in response.headers["vary"]
    assert int(response.headers["content-length"]) < len(response.content)
    assert response.json() == BIG


def test_small_json_is_left_alone():
    response = TestClient(_app()).get("/small", headers={"Accept-Encoding": "gzip"})
    assert "content-encoding" not in response.headers
    assert response.json() == {"ok": True}


def test_no_gzip_without_accept_encoding():
    response = TestClient(_app()).get("/big", headers={"Accept-Encoding": "identity"})
    assert "content-encoding" not in response.headers
    assert response.json() == BIG


def test_server_sent_events_are_not_compressed():
    response = TestClient(_app()).get("/events", headers={"Accept-Encoding": "gzip"})
    assert "content-encoding" not in response.headers
    assert response.text == "data: 0\n\ndata: 1\n\ndata: 2\n\n"


def test_still_gzipped_under_a_base_http_middleware():
    response = TestClient(_app(with_logging=True)).get("/big", headers={"Accept-Encoding": "gzip"})
    assert response.headers["content-encoding"] == "gzip"
    assert response.json() == BIG


def test_gzip_sits_innermost_in_the_real_app():
    # Starlette runs user_middleware first-to-last from the outside in, so the
    # last entry is the one next to the routes.
    app = create_application()
    assert app.user_middleware[-1].cls is GZipWholeResponseMiddleware
