"""Tests for audit logging functionality."""

import asyncio
from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.audit import AuditContext, audit_action, serialize_for_audit
from app.models.audit_log import AuditAction, AuditLog
from app.models.user import User
from app.services.audit_log import AuditLogService


@pytest.fixture
def mock_request():
    """Create a mock request object."""
    request = Mock()
    request.client.host = "192.168.1.100"
    request.headers = {"User-Agent": "TestAgent/1.0"}
    request.url.path = "/api/v1/test"
    request.method = "POST"
    return request


@pytest.fixture
def test_user_data():
    """Sample user data for audit testing."""
    return {
        "id": 1,
        "email": "audit@example.com",
        "username": "audituser",
        "first_name": "Audit",
        "last_name": "User",
        "is_active": True,
        "is_superuser": False,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }


class TestAuditLogService:
    """Test the AuditLogService class."""

    @pytest.mark.asyncio
    async def test_create_audit_log(self, test_session: AsyncSession):
        """Test creating an audit log entry."""
        audit_data = {
            "action": AuditAction.CREATE,
            "resource_type": "user",
            "user_id": 1,
            "user_email": "test@example.com",
            "resource_id": "123",
            "resource_name": "Test User",
            "description": "Created new user",
            "ip_address": "192.168.1.100",
        }

        result = await AuditLogService.log_action(test_session, **audit_data)

        assert result.id is not None
        assert result.action == AuditAction.CREATE
        assert result.resource_type == "user"
        assert result.user_id == 1
        assert result.user_email == "test@example.com"
        assert result.resource_id == "123"
        assert result.resource_name == "Test User"
        assert result.description == "Created new user"
        assert result.ip_address == "192.168.1.100"
        assert result.created_at is not None

    @pytest.mark.asyncio
    async def test_get_audit_logs(self, test_session: AsyncSession):
        """Test retrieving audit logs."""
        # Create test audit logs
        await AuditLogService.log_action(
            test_session,
            action=AuditAction.CREATE,
            resource_type="user",
            user_id=1,
            description="First log",
        )
        await AuditLogService.log_action(
            test_session,
            action=AuditAction.UPDATE,
            resource_type="user",
            user_id=1,
            description="Second log",
        )

        # Get all logs
        logs = await AuditLogService.get_audit_logs(test_session)
        assert len(logs) == 2

        # Test with limit
        logs_limited = await AuditLogService.get_audit_logs(test_session, limit=1)
        assert len(logs_limited) == 1

    @pytest.mark.asyncio
    async def test_get_audit_logs_with_filters(self, test_session: AsyncSession):
        """Test retrieving audit logs with filters."""
        # Create test audit logs with different actions and users
        await AuditLogService.log_action(
            test_session,
            action=AuditAction.CREATE,
            resource_type="user",
            user_id=1,
            user_email="user1@example.com",
        )
        await AuditLogService.log_action(
            test_session,
            action=AuditAction.DELETE,
            resource_type="owner",
            user_id=2,
            user_email="user2@example.com",
        )

        # Filter by action
        from app.schemas.audit_log import AuditLogFilter

        filters = AuditLogFilter(action=AuditAction.CREATE)
        logs = await AuditLogService.get_audit_logs(test_session, filters=filters)
        assert len(logs) == 1
        assert logs[0].action == AuditAction.CREATE

        # Filter by resource type
        filters = AuditLogFilter(resource_type="owner")
        logs = await AuditLogService.get_audit_logs(test_session, filters=filters)
        assert len(logs) == 1
        assert logs[0].resource_type == "owner"

        # Filter by user ID
        filters = AuditLogFilter(user_id=2)
        logs = await AuditLogService.get_audit_logs(test_session, filters=filters)
        assert len(logs) == 1
        assert logs[0].user_id == 2

    @pytest.mark.asyncio
    async def test_count_audit_logs(self, test_session: AsyncSession):
        """Test counting audit logs."""
        # Create test audit logs
        await AuditLogService.log_action(
            test_session,
            action=AuditAction.CREATE,
            resource_type="user",
            user_id=1,
        )
        await AuditLogService.log_action(
            test_session,
            action=AuditAction.UPDATE,
            resource_type="user",
            user_id=1,
        )

        count = await AuditLogService.count_audit_logs(test_session)
        assert count == 2

    @pytest.mark.asyncio
    async def test_get_resource_audit_history(self, test_session: AsyncSession):
        """Test getting audit history for a specific resource."""
        resource_id = "123"
        resource_type = "user"

        # Create logs for different resources
        await AuditLogService.log_action(
            test_session,
            action=AuditAction.CREATE,
            resource_type=resource_type,
            resource_id=resource_id,
        )
        await AuditLogService.log_action(
            test_session,
            action=AuditAction.UPDATE,
            resource_type=resource_type,
            resource_id=resource_id,
        )
        await AuditLogService.log_action(
            test_session,
            action=AuditAction.CREATE,
            resource_type="owner",
            resource_id="456",
        )

        logs = await AuditLogService.get_resource_audit_history(
            test_session, resource_type, resource_id
        )
        assert len(logs) == 2
        assert all(log.resource_type == resource_type for log in logs)
        assert all(log.resource_id == resource_id for log in logs)

    @pytest.mark.asyncio
    async def test_get_user_audit_history(self, test_session: AsyncSession):
        """Test getting audit history for a specific user."""
        user_id = 1

        # Create logs for different users
        await AuditLogService.log_action(
            test_session,
            action=AuditAction.CREATE,
            resource_type="user",
            user_id=user_id,
        )
        await AuditLogService.log_action(
            test_session,
            action=AuditAction.LOGIN,
            resource_type="user",
            user_id=user_id,
        )
        await AuditLogService.log_action(
            test_session,
            action=AuditAction.CREATE,
            resource_type="owner",
            user_id=2,
        )

        logs = await AuditLogService.get_user_audit_history(test_session, user_id)
        assert len(logs) == 2
        assert all(log.user_id == user_id for log in logs)


class TestAuditDecorator:
    """Test the audit_action decorator."""

    @pytest.mark.asyncio
    async def test_audit_decorator_basic(self, test_session: AsyncSession, mock_request):
        """Test basic audit decorator functionality."""

        @audit_action(AuditAction.CREATE, "test_resource", description="Test action")
        async def test_function(db: AsyncSession, request, current_user=None):
            return {"id": 123, "name": "Test Resource"}

        # Create a mock user
        mock_user = Mock()
        mock_user.id = 1
        mock_user.email = "test@example.com"

        result = await test_function(db=test_session, request=mock_request, current_user=mock_user)

        # Check function result
        assert result["id"] == 123
        assert result["name"] == "Test Resource"

        # Check audit log was created
        logs = await AuditLogService.get_audit_logs(test_session)
        assert len(logs) == 1

        log = logs[0]
        assert log.action == AuditAction.CREATE
        assert log.resource_type == "test_resource"
        assert log.user_id == 1
        assert log.user_email == "test@example.com"
        assert log.description == "Test action"
        assert log.ip_address == "192.168.1.100"
        assert log.user_agent == "TestAgent/1.0"
        assert log.endpoint == "/api/v1/test"
        assert log.method == "POST"

    @pytest.mark.asyncio
    async def test_audit_decorator_without_user(self, test_session: AsyncSession, mock_request):
        """Test audit decorator when no user is provided."""

        @audit_action(AuditAction.ACCESS, "test_resource", description="Anonymous access")
        async def test_function(db: AsyncSession, request):
            return {"data": "test"}

        result = await test_function(db=test_session, request=mock_request)

        # Check audit log was created without user info
        logs = await AuditLogService.get_audit_logs(test_session)
        assert len(logs) == 1

        log = logs[0]
        assert log.action == AuditAction.ACCESS
        assert log.resource_type == "test_resource"
        assert log.user_id is None
        assert log.user_email is None
        assert log.description == "Anonymous access"

    @pytest.mark.asyncio
    async def test_audit_decorator_error_handling(self, test_session: AsyncSession):
        """Test audit decorator handles database errors gracefully."""

        @audit_action(AuditAction.CREATE, "test_resource")
        async def test_function(db: AsyncSession):
            return {"id": 123}

        # Mock the AuditLogService.log_action to raise an exception
        with patch("app.core.audit.AuditLogService.log_action", side_effect=Exception("DB Error")):
            # Function should still work even if audit logging fails
            result = await test_function(db=test_session)
            assert result["id"] == 123


class TestAuditContext:
    """Test the AuditContext context manager."""

    @pytest.mark.asyncio
    async def test_audit_context_success(self, test_session: AsyncSession):
        """Test AuditContext creates log on successful completion."""
        async with AuditContext(
            test_session,
            AuditAction.UPDATE,
            "test_resource",
            user_id=1,
            user_email="test@example.com",
            resource_id="123",
            description="Test context",
        ) as ctx:
            ctx.set_new_values({"name": "Updated Name"})
            ctx.set_old_values({"name": "Old Name"})

        # Check audit log was created
        logs = await AuditLogService.get_audit_logs(test_session)
        assert len(logs) == 1

        log = logs[0]
        assert log.action == AuditAction.UPDATE
        assert log.resource_type == "test_resource"
        assert log.user_id == 1
        assert log.resource_id == "123"
        assert log.new_values == {"name": "Updated Name"}
        assert log.old_values == {"name": "Old Name"}

    @pytest.mark.asyncio
    async def test_audit_context_exception(self, test_session: AsyncSession):
        """Test AuditContext doesn't create log on exception."""
        try:
            async with AuditContext(
                test_session,
                AuditAction.DELETE,
                "test_resource",
                user_id=1,
                description="Test exception",
            ):
                raise ValueError("Test error")
        except ValueError:
            pass

        # Check no audit log was created due to exception
        logs = await AuditLogService.get_audit_logs(test_session)
        assert len(logs) == 0


class TestSerializeForAudit:
    """Test the serialize_for_audit function."""

    def test_serialize_basic_types(self):
        """Test serializing basic Python types."""
        assert serialize_for_audit("string") == "string"
        assert serialize_for_audit(123) == 123
        assert serialize_for_audit(12.34) == 12.34
        assert serialize_for_audit(True) is True
        assert serialize_for_audit(None) is None

    def test_serialize_datetime(self):
        """Test serializing datetime objects."""
        dt = datetime(2023, 1, 1, 12, 0, 0)
        result = serialize_for_audit(dt)
        assert result == dt.isoformat()

    def test_serialize_dict(self):
        """Test serializing dictionaries."""
        data = {
            "string": "value",
            "number": 123,
            "datetime": datetime(2023, 1, 1, 12, 0, 0),
            "nested": {"key": "value"},
        }
        result = serialize_for_audit(data)

        assert result["string"] == "value"
        assert result["number"] == 123
        assert result["datetime"] == datetime(2023, 1, 1, 12, 0, 0).isoformat()
        assert result["nested"]["key"] == "value"

    def test_serialize_list(self):
        """Test serializing lists."""
        data = [1, "string", datetime(2023, 1, 1)]
        result = serialize_for_audit(data)

        assert len(result) == 3
        assert result[0] == 1
        assert result[1] == "string"
        assert result[2] == datetime(2023, 1, 1).isoformat()

    def test_serialize_object_with_dict(self):
        """Test serializing objects with __dict__."""

        class TestObj:
            def __init__(self):
                self.name = "test"
                self.value = 123
                self.created_at = datetime(2023, 1, 1)
                self._private = "hidden"  # Should be ignored

        obj = TestObj()
        result = serialize_for_audit(obj)

        assert result["name"] == "test"
        assert result["value"] == 123
        assert result["created_at"] == datetime(2023, 1, 1).isoformat()
        assert "_private" not in result


class TestAuditLogsAPI:
    """Test the audit logs API endpoints."""

    def test_get_audit_logs_unauthorized(self, client: TestClient):
        """Test that audit logs require authentication."""
        response = client.get("/api/v1/audit-logs")
        assert response.status_code == 401

    def test_get_audit_logs_non_superuser(self, client: TestClient, user_data):
        """Test that audit logs require superuser access."""
        # Register and login as regular user
        register_response = client.post("/api/v1/auth/register", json=user_data)
        assert register_response.status_code == 201

        login_response = client.post(
            "/api/v1/auth/login",
            json={"username": user_data["username"], "password": user_data["password"]},
        )
        assert login_response.status_code == 200
        token = login_response.json()["access_token"]

        # Try to access audit logs
        response = client.get("/api/v1/audit-logs", headers={"Authorization": f"Bearer {token}"})
        assert response.status_code == 403

    def test_get_audit_logs_superuser(self, client: TestClient, user_data):
        """Test that superusers can access audit logs."""
        # Make user a superuser
        user_data["is_superuser"] = True

        # Register and login as superuser
        register_response = client.post("/api/v1/auth/register", json=user_data)
        assert register_response.status_code == 201

        login_response = client.post(
            "/api/v1/auth/login",
            json={"username": user_data["username"], "password": user_data["password"]},
        )
        assert login_response.status_code == 200
        token = login_response.json()["access_token"]

        # Access audit logs should work
        response = client.get("/api/v1/audit-logs", headers={"Authorization": f"Bearer {token}"})
        assert response.status_code == 200

        # Should return list of audit logs (including registration and login)
        data = response.json()
        assert isinstance(data, list)
