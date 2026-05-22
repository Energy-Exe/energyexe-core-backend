"""Tests for the first-login Terms / Privacy acceptance flow."""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.exceptions import ValidationException
from app.models.user import User
from app.models.user_consent import UserConsent
from app.services.consent import ConsentService

settings = get_settings()


@pytest.fixture
def consent_user_data():
    return {
        "email": "consent-user@example.com",
        "username": "consentuser",
        "password": "testpassword123",
        "first_name": "Consent",
        "last_name": "Tester",
    }


async def _make_user(session: AsyncSession, **overrides) -> User:
    user = User(
        email=overrides.get("email", "service-test@example.com"),
        username=overrides.get("username", "servicetest"),
        hashed_password="x" * 60,
        is_active=True,
        is_superuser=False,
    )
    session.add(user)
    await session.commit()
    await session.refresh(user)
    return user


@pytest.mark.asyncio
async def test_first_login_requires_acceptance(test_session: AsyncSession):
    """A brand-new user has accepted nothing → both docs need acceptance, no 'changed' flag."""
    user = await _make_user(test_session)
    status = await ConsentService(test_session).get_status(user.id)

    assert status.requires_acceptance is True
    assert status.changed_documents == []
    assert status.terms.current_version == settings.TERMS_VERSION
    assert status.terms.accepted_version is None
    assert status.privacy.accepted_version is None


@pytest.mark.asyncio
async def test_record_acceptance_inserts_both_docs(test_session: AsyncSession):
    user = await _make_user(test_session, email="record@example.com", username="recorduser")
    service = ConsentService(test_session)

    after = await service.record_acceptance(
        user.id,
        terms_version=settings.TERMS_VERSION,
        privacy_version=settings.PRIVACY_VERSION,
        ip_address="127.0.0.1",
        user_agent="pytest",
    )

    assert after.requires_acceptance is False
    assert after.terms.accepted_version == settings.TERMS_VERSION
    assert after.privacy.accepted_version == settings.PRIVACY_VERSION

    rows = (
        await test_session.execute(
            select(UserConsent).where(UserConsent.user_id == user.id)
        )
    ).scalars().all()
    assert {r.document_type for r in rows} == {"terms", "privacy"}
    assert all(r.ip_address == "127.0.0.1" for r in rows)


@pytest.mark.asyncio
async def test_version_mismatch_is_rejected(test_session: AsyncSession):
    user = await _make_user(test_session, email="mismatch@example.com", username="mismatch")
    with pytest.raises(ValidationException):
        await ConsentService(test_session).record_acceptance(
            user.id,
            terms_version="not-the-real-version",
            privacy_version=settings.PRIVACY_VERSION,
        )


@pytest.mark.asyncio
async def test_changed_documents_flagged_after_version_bump(
    test_session: AsyncSession, monkeypatch
):
    """If only Privacy is bumped, get_status flags Privacy but not Terms."""
    user = await _make_user(test_session, email="bump@example.com", username="bumpuser")
    service = ConsentService(test_session)

    # First acceptance at current versions.
    await service.record_acceptance(
        user.id,
        terms_version=settings.TERMS_VERSION,
        privacy_version=settings.PRIVACY_VERSION,
    )

    # Simulate a new Privacy version being deployed. ``ConsentService``
    # resolves the version lazily via ``get_settings()`` so an env override is
    # picked up on the next call.
    monkeypatch.setenv("PRIVACY_VERSION", "9999-01-01")

    status = await service.get_status(user.id)
    assert status.requires_acceptance is True
    assert status.changed_documents == ["privacy"]
    assert status.terms.accepted_version == status.terms.current_version


def test_get_consents_me_endpoint_requires_auth(client: TestClient):
    response = client.get("/api/v1/consents/me")
    # Missing bearer token → 401/403 depending on FastAPI security wiring; both
    # signal "not authenticated".
    assert response.status_code in (401, 403)


@pytest.mark.asyncio
async def test_full_acceptance_flow_via_http(
    client: TestClient, test_session: AsyncSession, consent_user_data
):
    """End-to-end: register, login, observe pending consent, accept, observe clear."""
    from sqlalchemy import update

    register = client.post("/api/v1/auth/register", json=consent_user_data)
    assert register.status_code == 201, register.text

    # Bypass the client-portal email-verification + approval gates for this test.
    await test_session.execute(
        update(User)
        .where(User.username == consent_user_data["username"])
        .values(email_verified=True, is_approved=True)
    )
    await test_session.commit()

    login = client.post(
        "/api/v1/auth/login",
        json={
            "username": consent_user_data["username"],
            "password": consent_user_data["password"],
        },
    )
    assert login.status_code == 200, login.text
    token = login.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    status = client.get("/api/v1/consents/me", headers=headers)
    assert status.status_code == 200, status.text
    body = status.json()
    assert body["requires_acceptance"] is True
    assert body["changed_documents"] == []

    accept = client.post(
        "/api/v1/consents/me/accept",
        headers=headers,
        json={
            "terms_version": settings.TERMS_VERSION,
            "privacy_version": settings.PRIVACY_VERSION,
        },
    )
    assert accept.status_code == 200, accept.text
    accepted = accept.json()
    assert accepted["requires_acceptance"] is False
    assert accepted["terms"]["accepted_version"] == settings.TERMS_VERSION
    assert accepted["privacy"]["accepted_version"] == settings.PRIVACY_VERSION
