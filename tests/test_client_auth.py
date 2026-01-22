"""Tests for client portal authentication endpoints."""

import secrets
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch, MagicMock

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import create_access_token, get_password_hash
from app.models.invitation import Invitation
from app.models.user import User
from app.schemas.user import UserResponseExtended


# ============================================================================
# FIXTURES
# ============================================================================


@pytest.fixture
def client_registration_data():
    """Sample client registration data for testing."""
    return {
        "email": "client@example.com",
        "password": "SecurePass123!",
        "first_name": "John",
        "last_name": "Doe",
        "company_name": "Acme Corp",
        "phone": "+1234567890",
    }


@pytest.fixture
def admin_user_data():
    """Sample admin user data for testing."""
    return {
        "email": "admin@energyexe.com",
        "username": "adminuser",
        "hashed_password": get_password_hash("AdminPass123!"),
        "first_name": "Admin",
        "last_name": "User",
        "is_active": True,
        "is_superuser": True,
        "role": "admin",
        "is_approved": True,
        "email_verified": True,
    }


@pytest.fixture
async def admin_user(test_session: AsyncSession, admin_user_data):
    """Create an admin user for testing."""
    user = User(**admin_user_data)
    test_session.add(user)
    await test_session.commit()
    await test_session.refresh(user)
    return user


@pytest.fixture
def admin_auth_header(admin_user):
    """Create authorization header for admin user."""
    token = create_access_token(subject=admin_user.username)
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
async def pending_client_user(test_session: AsyncSession):
    """Create a pending client user (email verified, not approved)."""
    user = User(
        email="pending@example.com",
        username="pendinguser",
        hashed_password=get_password_hash("TestPass123!"),
        first_name="Pending",
        last_name="Client",
        company_name="Pending Corp",
        role="client",
        is_active=True,
        is_approved=False,
        email_verified=True,
    )
    test_session.add(user)
    await test_session.commit()
    await test_session.refresh(user)
    return user


@pytest.fixture
async def unverified_client_user(test_session: AsyncSession):
    """Create a client user with unverified email."""
    verification_token = secrets.token_urlsafe(32)
    user = User(
        email="unverified@example.com",
        username="unverifieduser",
        hashed_password=get_password_hash("TestPass123!"),
        first_name="Unverified",
        last_name="Client",
        company_name="Unverified Corp",
        role="client",
        is_active=True,
        is_approved=False,
        email_verified=False,
        email_verification_token=verification_token,
        email_verification_sent_at=datetime.utcnow(),
    )
    test_session.add(user)
    await test_session.commit()
    await test_session.refresh(user)
    return user, verification_token


@pytest.fixture
async def user_with_reset_token(test_session: AsyncSession):
    """Create a user with a password reset token."""
    reset_token = secrets.token_urlsafe(32)
    user = User(
        email="reset@example.com",
        username="resetuser",
        hashed_password=get_password_hash("OldPass123!"),
        first_name="Reset",
        last_name="User",
        role="client",
        is_active=True,
        is_approved=True,
        email_verified=True,
        password_reset_token=reset_token,
        password_reset_sent_at=datetime.utcnow(),
    )
    test_session.add(user)
    await test_session.commit()
    await test_session.refresh(user)
    return user, reset_token


@pytest.fixture
async def valid_invitation(test_session: AsyncSession, admin_user):
    """Create a valid invitation."""
    token = secrets.token_urlsafe(32)
    invitation = Invitation(
        email="invited@example.com",
        token=token,
        invited_by_id=admin_user.id,
        expires_at=datetime.utcnow() + timedelta(days=7),
    )
    test_session.add(invitation)
    await test_session.commit()
    await test_session.refresh(invitation)
    return invitation, token


# ============================================================================
# CLIENT REGISTRATION TESTS
# ============================================================================


class TestClientRegistration:
    """Tests for client registration endpoint."""

    @patch("app.api.v1.endpoints.auth.email_service.send_verification_email", new_callable=AsyncMock)
    def test_register_client_success(
        self, mock_send_email, client: TestClient, client_registration_data, test_session
    ):
        """Test successful client registration.

        Note: Due to SQLAlchemy async/sync context issues with TestClient and
        lazy-loaded relationships (features), we test using the service directly.
        """
        import asyncio
        from app.services.user import UserService
        from app.schemas.user import ClientRegister

        async def register_and_verify():
            user_service = UserService(test_session)
            client_data = ClientRegister(**client_registration_data)
            user, verification_token = await user_service.register_client(client_data)

            # Verify user attributes
            assert user.email == client_registration_data["email"]
            assert user.first_name == client_registration_data["first_name"]
            assert user.last_name == client_registration_data["last_name"]
            assert user.company_name == client_registration_data["company_name"]
            assert user.role == "client"
            assert user.is_approved is False
            assert user.email_verified is False
            assert verification_token is not None
            return user

        loop = asyncio.get_event_loop()
        loop.run_until_complete(register_and_verify())

    @patch("app.api.v1.endpoints.auth.email_service.send_verification_email", new_callable=AsyncMock)
    def test_register_client_duplicate_email(
        self, mock_send_email, client: TestClient, client_registration_data, test_session
    ):
        """Test registration fails with duplicate email."""
        import asyncio

        # Create first user directly in db to avoid endpoint serialization issues
        async def create_first_user():
            user = User(
                email=client_registration_data["email"],
                username="existinguser",
                hashed_password=get_password_hash(client_registration_data["password"]),
                first_name=client_registration_data["first_name"],
                last_name=client_registration_data["last_name"],
                company_name=client_registration_data["company_name"],
                role="client",
                is_active=True,
                is_approved=False,
                email_verified=False,
            )
            test_session.add(user)
            await test_session.commit()
            return user

        loop = asyncio.get_event_loop()
        loop.run_until_complete(create_first_user())

        # Try to register with same email
        response = client.post("/api/v1/auth/client/register", json=client_registration_data)

        assert response.status_code == 400
        assert "email" in response.json()["error"]["message"].lower()

    def test_register_client_invalid_email_format(
        self, client: TestClient, client_registration_data
    ):
        """Test registration fails with invalid email format."""
        client_registration_data["email"] = "not-an-email"
        response = client.post("/api/v1/auth/client/register", json=client_registration_data)

        assert response.status_code == 422  # Validation error
        data = response.json()
        assert "detail" in data

    def test_register_client_password_too_short(
        self, client: TestClient, client_registration_data
    ):
        """Test registration fails with password too short."""
        client_registration_data["password"] = "short"
        response = client.post("/api/v1/auth/client/register", json=client_registration_data)

        assert response.status_code == 422  # Validation error
        data = response.json()
        assert "detail" in data

    def test_register_client_missing_required_fields(self, client: TestClient):
        """Test registration fails with missing required fields."""
        incomplete_data = {
            "email": "incomplete@example.com",
            "password": "SecurePass123!",
        }
        response = client.post("/api/v1/auth/client/register", json=incomplete_data)

        assert response.status_code == 422  # Validation error

    def test_register_client_generates_unique_username(
        self, client_registration_data, test_session
    ):
        """Test that registration generates unique usernames from email."""
        import asyncio
        from app.services.user import UserService
        from app.schemas.user import ClientRegister

        async def register_multiple_clients():
            user_service = UserService(test_session)

            # Register first client
            client_data1 = ClientRegister(**client_registration_data)
            user1, _ = await user_service.register_client(client_data1)

            # Register second client with similar email pattern
            second_client = client_registration_data.copy()
            second_client["email"] = "client2@example.com"
            client_data2 = ClientRegister(**second_client)
            user2, _ = await user_service.register_client(client_data2)

            # Usernames should be unique (derived from email prefix)
            assert user1.username != user2.username
            assert user1.username == "client"  # from client@example.com
            assert user2.username == "client2"  # from client2@example.com

        loop = asyncio.get_event_loop()
        loop.run_until_complete(register_multiple_clients())


# ============================================================================
# EMAIL VERIFICATION TESTS
# ============================================================================


class TestEmailVerification:
    """Tests for email verification endpoint."""

    def test_verify_email_success(self, client: TestClient, unverified_client_user):
        """Test successful email verification."""
        user, verification_token = unverified_client_user

        response = client.post(
            "/api/v1/auth/verify-email", json={"token": verification_token}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "Email verified successfully"
        assert data["email"] == user.email
        assert data["is_approved"] is False  # Still needs admin approval

    def test_verify_email_invalid_token(self, client: TestClient):
        """Test email verification with invalid token."""
        response = client.post(
            "/api/v1/auth/verify-email", json={"token": "invalid-token-12345"}
        )

        assert response.status_code == 400
        assert "invalid" in response.json()["error"]["message"].lower()

    def test_verify_email_expired_token(self, client: TestClient, test_session):
        """Test email verification with expired token."""
        # Create user with expired verification token
        import asyncio

        async def create_expired_user():
            verification_token = secrets.token_urlsafe(32)
            user = User(
                email="expired@example.com",
                username="expireduser",
                hashed_password=get_password_hash("TestPass123!"),
                first_name="Expired",
                last_name="User",
                company_name="Expired Corp",
                role="client",
                is_active=True,
                is_approved=False,
                email_verified=False,
                email_verification_token=verification_token,
                # Token sent 25 hours ago (expired if limit is 24 hours)
                email_verification_sent_at=datetime.utcnow() - timedelta(hours=25),
            )
            test_session.add(user)
            await test_session.commit()
            return verification_token

        loop = asyncio.get_event_loop()
        token = loop.run_until_complete(create_expired_user())

        response = client.post("/api/v1/auth/verify-email", json={"token": token})

        assert response.status_code == 400
        assert "expired" in response.json()["error"]["message"].lower()


# ============================================================================
# PASSWORD RESET FLOW TESTS
# ============================================================================


class TestPasswordResetFlow:
    """Tests for password reset flow."""

    @patch("app.api.v1.endpoints.auth.email_service.send_password_reset_email", new_callable=AsyncMock)
    def test_forgot_password_success(
        self, mock_send_email, client: TestClient, user_with_reset_token
    ):
        """Test forgot password request for existing user."""
        user, _ = user_with_reset_token

        response = client.post(
            "/api/v1/auth/forgot-password", json={"email": user.email}
        )

        assert response.status_code == 200
        data = response.json()
        # Should always return success message (security)
        assert "message" in data
        mock_send_email.assert_called_once()

    def test_forgot_password_nonexistent_email(self, client: TestClient):
        """Test forgot password for non-existent email (should not reveal)."""
        response = client.post(
            "/api/v1/auth/forgot-password", json={"email": "nonexistent@example.com"}
        )

        # Should return 200 with same message (security - don't reveal if user exists)
        assert response.status_code == 200
        data = response.json()
        assert "message" in data

    @patch("app.api.v1.endpoints.auth.email_service.send_password_changed_email", new_callable=AsyncMock)
    def test_reset_password_success(
        self, mock_send_email, client: TestClient, user_with_reset_token
    ):
        """Test successful password reset."""
        user, reset_token = user_with_reset_token
        new_password = "NewSecurePass456!"

        response = client.post(
            "/api/v1/auth/reset-password",
            json={"token": reset_token, "new_password": new_password},
        )

        assert response.status_code == 200
        assert response.json()["message"] == "Password reset successfully"
        mock_send_email.assert_called_once()

    def test_reset_password_invalid_token(self, client: TestClient):
        """Test password reset with invalid token."""
        response = client.post(
            "/api/v1/auth/reset-password",
            json={"token": "invalid-token", "new_password": "NewPass123!"},
        )

        assert response.status_code == 400
        assert "invalid" in response.json()["error"]["message"].lower()

    def test_reset_password_expired_token(self, client: TestClient, test_session):
        """Test password reset with expired token."""
        import asyncio

        async def create_user_with_expired_reset():
            reset_token = secrets.token_urlsafe(32)
            user = User(
                email="expiredreset@example.com",
                username="expiredresetuser",
                hashed_password=get_password_hash("OldPass123!"),
                first_name="Expired",
                last_name="Reset",
                role="client",
                is_active=True,
                is_approved=True,
                email_verified=True,
                password_reset_token=reset_token,
                # Token sent 2 hours ago (expired if limit is 1 hour)
                password_reset_sent_at=datetime.utcnow() - timedelta(hours=2),
            )
            test_session.add(user)
            await test_session.commit()
            return reset_token

        loop = asyncio.get_event_loop()
        token = loop.run_until_complete(create_user_with_expired_reset())

        response = client.post(
            "/api/v1/auth/reset-password",
            json={"token": token, "new_password": "NewPass123!"},
        )

        assert response.status_code == 400
        assert "expired" in response.json()["error"]["message"].lower()

    def test_reset_password_too_short(self, client: TestClient, user_with_reset_token):
        """Test password reset with password too short."""
        _, reset_token = user_with_reset_token

        response = client.post(
            "/api/v1/auth/reset-password",
            json={"token": reset_token, "new_password": "short"},
        )

        assert response.status_code == 422  # Validation error


# ============================================================================
# INVITATION FLOW TESTS
# ============================================================================


class TestInvitationFlow:
    """Tests for invitation validation and acceptance."""

    def test_validate_invitation_valid(self, client: TestClient, valid_invitation):
        """Test validation of a valid invitation token."""
        invitation, token = valid_invitation

        response = client.get(f"/api/v1/auth/invitation/{token}")

        assert response.status_code == 200
        data = response.json()
        assert data["valid"] is True
        assert data["email"] == invitation.email
        assert data["message"] is None

    def test_validate_invitation_invalid_token(self, client: TestClient):
        """Test validation of an invalid invitation token."""
        response = client.get("/api/v1/auth/invitation/invalid-token-12345")

        assert response.status_code == 200  # Returns validation result, not error
        data = response.json()
        assert data["valid"] is False
        assert "invalid" in data["message"].lower()

    def test_validate_invitation_expired(self, client: TestClient, test_session, admin_user):
        """Test validation of an expired invitation token."""
        import asyncio

        async def create_expired_invitation():
            token = secrets.token_urlsafe(32)
            invitation = Invitation(
                email="expired-invite@example.com",
                token=token,
                invited_by_id=admin_user.id,
                expires_at=datetime.utcnow() - timedelta(days=1),  # Expired
            )
            test_session.add(invitation)
            await test_session.commit()
            return token

        loop = asyncio.get_event_loop()
        expired_token = loop.run_until_complete(create_expired_invitation())

        response = client.get(f"/api/v1/auth/invitation/{expired_token}")

        assert response.status_code == 200
        data = response.json()
        assert data["valid"] is False
        assert "expired" in data["message"].lower()

    def test_accept_invitation_success(self, valid_invitation, test_session):
        """Test successful invitation acceptance."""
        import asyncio
        from app.services.invitation import InvitationService
        from app.schemas.invitation import InvitationAccept

        invitation, token = valid_invitation
        accept_data = {
            "first_name": "Invited",
            "last_name": "User",
            "password": "SecurePass123!",
            "company_name": "Invited Corp",
            "phone": "+9876543210",
        }

        async def accept_and_verify():
            invitation_service = InvitationService(test_session)
            data = InvitationAccept(**accept_data)
            user = await invitation_service.accept(token, data)

            assert user is not None
            assert user.email == invitation.email
            assert user.first_name == accept_data["first_name"]
            assert user.last_name == accept_data["last_name"]
            assert user.company_name == accept_data["company_name"]
            assert user.is_approved is True  # Pre-approved via invitation
            assert user.email_verified is True  # Pre-verified via invitation
            assert user.role == "client"

        loop = asyncio.get_event_loop()
        loop.run_until_complete(accept_and_verify())

    def test_accept_invitation_invalid_token(self, client: TestClient):
        """Test accepting invitation with invalid token."""
        accept_data = {
            "first_name": "Test",
            "last_name": "User",
            "password": "SecurePass123!",
            "company_name": "Test Corp",
        }

        response = client.post(
            "/api/v1/auth/invitation/invalid-token/accept", json=accept_data
        )

        assert response.status_code == 400
        assert "invalid" in response.json()["error"]["message"].lower()

    def test_accept_invitation_already_used(
        self, client: TestClient, test_session, admin_user
    ):
        """Test accepting an already used invitation."""
        import asyncio

        async def create_used_invitation():
            token = secrets.token_urlsafe(32)
            invitation = Invitation(
                email="used-invite@example.com",
                token=token,
                invited_by_id=admin_user.id,
                expires_at=datetime.utcnow() + timedelta(days=7),
                used_at=datetime.utcnow(),  # Already used
            )
            test_session.add(invitation)
            await test_session.commit()
            return token

        loop = asyncio.get_event_loop()
        used_token = loop.run_until_complete(create_used_invitation())

        accept_data = {
            "first_name": "Test",
            "last_name": "User",
            "password": "SecurePass123!",
            "company_name": "Test Corp",
        }

        response = client.post(
            f"/api/v1/auth/invitation/{used_token}/accept", json=accept_data
        )

        assert response.status_code == 400
        assert "used" in response.json()["error"]["message"].lower()

    def test_accept_invitation_password_too_short(
        self, client: TestClient, valid_invitation
    ):
        """Test accepting invitation with password too short."""
        _, token = valid_invitation
        accept_data = {
            "first_name": "Test",
            "last_name": "User",
            "password": "short",  # Too short
            "company_name": "Test Corp",
        }

        response = client.post(
            f"/api/v1/auth/invitation/{token}/accept", json=accept_data
        )

        assert response.status_code == 422  # Validation error


# ============================================================================
# ADMIN ENDPOINTS TESTS
# ============================================================================


class TestAdminEndpoints:
    """Tests for admin user management endpoints."""

    def test_get_pending_users_success(
        self, client: TestClient, admin_auth_header, pending_client_user
    ):
        """Test getting pending users as admin."""
        response = client.get("/api/v1/admin/users/pending", headers=admin_auth_header)

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) >= 1
        # Check that pending user is in the list
        emails = [user["email"] for user in data]
        assert pending_client_user.email in emails

    def test_get_pending_users_unauthorized(self, client: TestClient):
        """Test getting pending users without authentication."""
        response = client.get("/api/v1/admin/users/pending")

        assert response.status_code == 403  # Forbidden (no auth header)

    def test_get_pending_users_non_admin(
        self, client: TestClient, pending_client_user
    ):
        """Test getting pending users as non-admin user fails."""
        # Create a token for a non-admin user
        token = create_access_token(subject=pending_client_user.username)
        headers = {"Authorization": f"Bearer {token}"}

        response = client.get("/api/v1/admin/users/pending", headers=headers)

        # Should fail because user is not approved
        assert response.status_code == 401

    def test_approve_user_success(
        self, pending_client_user, admin_user, test_session
    ):
        """Test approving a user as admin."""
        import asyncio
        from app.services.user import UserService

        async def approve_and_verify():
            user_service = UserService(test_session)
            approved_user = await user_service.approve_user(
                pending_client_user.id, admin_user
            )

            assert approved_user.is_approved is True
            assert approved_user.approved_at is not None
            assert approved_user.approved_by_id == admin_user.id

        loop = asyncio.get_event_loop()
        loop.run_until_complete(approve_and_verify())

    def test_approve_user_not_found(self, client: TestClient, admin_auth_header):
        """Test approving non-existent user."""
        response = client.post(
            "/api/v1/admin/users/99999/approve", headers=admin_auth_header
        )

        assert response.status_code == 404

    def test_approve_user_already_approved(
        self, client: TestClient, admin_auth_header, admin_user
    ):
        """Test approving an already approved user."""
        response = client.post(
            f"/api/v1/admin/users/{admin_user.id}/approve",
            headers=admin_auth_header,
        )

        assert response.status_code == 400
        assert "already" in response.json()["error"]["message"].lower()

    @patch("app.api.v1.endpoints.admin.email_service.send_invitation_email", new_callable=AsyncMock)
    def test_create_invitation_success(
        self, mock_send_email, client: TestClient, admin_auth_header
    ):
        """Test creating an invitation as admin."""
        invitation_data = {"email": "newinvite@example.com"}

        response = client.post(
            "/api/v1/admin/invitations",
            json=invitation_data,
            headers=admin_auth_header,
        )

        assert response.status_code == 201
        data = response.json()
        assert data["email"] == invitation_data["email"]
        assert data["is_valid"] is True
        assert data["is_used"] is False
        assert data["is_expired"] is False
        mock_send_email.assert_called_once()

    def test_create_invitation_duplicate_email(
        self, client: TestClient, admin_auth_header, valid_invitation
    ):
        """Test creating invitation for email that already has a valid invitation."""
        invitation, _ = valid_invitation
        invitation_data = {"email": invitation.email}

        response = client.post(
            "/api/v1/admin/invitations",
            json=invitation_data,
            headers=admin_auth_header,
        )

        assert response.status_code == 400
        assert "invitation" in response.json()["error"]["message"].lower()

    def test_create_invitation_existing_user(
        self, client: TestClient, admin_auth_header, client_registration_data, test_session
    ):
        """Test creating invitation for email that already has an account."""
        import asyncio

        # Create user directly in db
        async def create_user():
            user = User(
                email=client_registration_data["email"],
                username="existingclient",
                hashed_password=get_password_hash(client_registration_data["password"]),
                first_name=client_registration_data["first_name"],
                last_name=client_registration_data["last_name"],
                company_name=client_registration_data["company_name"],
                role="client",
                is_active=True,
            )
            test_session.add(user)
            await test_session.commit()

        loop = asyncio.get_event_loop()
        loop.run_until_complete(create_user())

        # Try to create invitation for same email
        invitation_data = {"email": client_registration_data["email"]}
        response = client.post(
            "/api/v1/admin/invitations",
            json=invitation_data,
            headers=admin_auth_header,
        )

        assert response.status_code == 400
        assert "exists" in response.json()["error"]["message"].lower()

    def test_create_invitation_unauthorized(self, client: TestClient):
        """Test creating invitation without authentication."""
        invitation_data = {"email": "test@example.com"}

        response = client.post("/api/v1/admin/invitations", json=invitation_data)

        assert response.status_code == 403

    def test_get_invitations_success(
        self, client: TestClient, admin_auth_header, valid_invitation
    ):
        """Test getting all invitations as admin."""
        response = client.get("/api/v1/admin/invitations", headers=admin_auth_header)

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) >= 1


# ============================================================================
# RESEND VERIFICATION TESTS
# ============================================================================


class TestResendVerification:
    """Tests for resending verification email."""

    @patch("app.api.v1.endpoints.auth.email_service.send_verification_email", new_callable=AsyncMock)
    def test_resend_verification_success(
        self, mock_send_email, client: TestClient, unverified_client_user
    ):
        """Test resending verification email for unverified user."""
        user, _ = unverified_client_user

        response = client.post(
            "/api/v1/auth/resend-verification", json={"email": user.email}
        )

        assert response.status_code == 200
        assert "verification email sent" in response.json()["message"].lower()
        mock_send_email.assert_called_once()

    def test_resend_verification_nonexistent_email(self, client: TestClient):
        """Test resending verification for non-existent email."""
        response = client.post(
            "/api/v1/auth/resend-verification",
            json={"email": "nonexistent@example.com"},
        )

        # Should return success message (security - don't reveal if user exists)
        assert response.status_code == 200
        assert "message" in response.json()

    def test_resend_verification_already_verified(
        self, client: TestClient, pending_client_user
    ):
        """Test resending verification for already verified user."""
        # pending_client_user has email_verified=True
        response = client.post(
            "/api/v1/auth/resend-verification",
            json={"email": pending_client_user.email},
        )

        assert response.status_code == 400
        assert "already verified" in response.json()["error"]["message"].lower()


# ============================================================================
# USER DEACTIVATION/REACTIVATION TESTS
# ============================================================================


class TestUserDeactivation:
    """Tests for user deactivation and reactivation."""

    def test_deactivate_user_success(
        self, pending_client_user, test_session
    ):
        """Test deactivating a user."""
        import asyncio
        from app.services.user import UserService

        async def deactivate_and_verify():
            user_service = UserService(test_session)
            deactivated_user = await user_service.deactivate_user(pending_client_user.id)
            assert deactivated_user.is_active is False

        loop = asyncio.get_event_loop()
        loop.run_until_complete(deactivate_and_verify())

    def test_deactivate_self_fails(self, client: TestClient, admin_auth_header, admin_user):
        """Test that admin cannot deactivate their own account via API."""
        response = client.post(
            f"/api/v1/admin/users/{admin_user.id}/deactivate",
            headers=admin_auth_header,
        )

        assert response.status_code == 400
        assert "own account" in response.json()["error"]["message"].lower()

    def test_reactivate_user_success(self, test_session):
        """Test reactivating a deactivated user."""
        import asyncio
        from app.services.user import UserService

        async def create_deactivated_and_reactivate():
            # Create deactivated user
            user = User(
                email="deactivated@example.com",
                username="deactivateduser",
                hashed_password=get_password_hash("TestPass123!"),
                first_name="Deactivated",
                last_name="User",
                role="client",
                is_active=False,  # Deactivated
                is_approved=True,
                email_verified=True,
            )
            test_session.add(user)
            await test_session.commit()
            await test_session.refresh(user)

            # Reactivate
            user_service = UserService(test_session)
            reactivated_user = await user_service.reactivate_user(user.id)
            assert reactivated_user.is_active is True

        loop = asyncio.get_event_loop()
        loop.run_until_complete(create_deactivated_and_reactivate())


# ============================================================================
# USER REJECTION TESTS
# ============================================================================


class TestUserRejection:
    """Tests for rejecting user applications."""

    def test_reject_user_success(self, pending_client_user, test_session):
        """Test rejecting a pending user."""
        import asyncio
        from sqlalchemy import select
        from app.services.user import UserService

        async def reject_and_verify():
            user_service = UserService(test_session)
            user_id = pending_client_user.id

            # Reject the user (this deletes them)
            await user_service.reject_user(user_id, "Incomplete application")

            # Verify user is deleted
            result = await test_session.execute(
                select(User).where(User.id == user_id)
            )
            user = result.scalar_one_or_none()
            assert user is None

        loop = asyncio.get_event_loop()
        loop.run_until_complete(reject_and_verify())

    def test_reject_approved_user_fails(self, admin_user, test_session):
        """Test that rejecting an approved user fails."""
        import asyncio
        import pytest
        from app.services.user import UserService
        from app.core.exceptions import ValidationException

        async def try_reject_approved():
            user_service = UserService(test_session)
            with pytest.raises(ValidationException):
                await user_service.reject_user(admin_user.id, "Test reason")

        loop = asyncio.get_event_loop()
        loop.run_until_complete(try_reject_approved())


# ============================================================================
# USER FEATURES TESTS
# ============================================================================


class TestUserFeatures:
    """Tests for user feature management."""

    def test_get_user_features(
        self, pending_client_user, admin_user, test_session
    ):
        """Test getting user features after approval."""
        import asyncio
        from app.services.user import UserService

        async def approve_and_get_features():
            user_service = UserService(test_session)
            # First approve the user to create default features
            await user_service.approve_user(pending_client_user.id, admin_user)

            # Get features
            features = await user_service.get_user_features(pending_client_user.id)

            assert isinstance(features, dict)
            # Check default features exist
            assert "can_view_dashboard" in features
            assert "can_view_reports" in features
            assert "can_export_data" in features
            assert "can_view_analytics" in features

        loop = asyncio.get_event_loop()
        loop.run_until_complete(approve_and_get_features())

    def test_get_user_features_not_found(self, test_session):
        """Test getting features for non-existent user."""
        import asyncio
        import pytest
        from app.services.user import UserService
        from app.core.exceptions import NotFoundException

        async def get_features_not_found():
            user_service = UserService(test_session)
            with pytest.raises(NotFoundException):
                await user_service.get_user_features(99999)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(get_features_not_found())

    def test_update_user_features(
        self, pending_client_user, admin_user, test_session
    ):
        """Test updating user features."""
        import asyncio
        from app.services.user import UserService

        async def approve_and_update_features():
            user_service = UserService(test_session)
            # First approve the user
            await user_service.approve_user(pending_client_user.id, admin_user)

            # Update features
            updated_features = await user_service.update_user_features(
                pending_client_user.id, {"can_export_data": False}
            )

            assert updated_features["can_export_data"] is False

        loop = asyncio.get_event_loop()
        loop.run_until_complete(approve_and_update_features())
