from functools import wraps
from typing import Any, Callable, Dict, Optional

import structlog
from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.request_utils import get_client_ip, get_user_agent  # noqa: F401 - re-exported
from app.models.audit_log import AuditAction
from app.services.audit_log import AuditLogService

logger = structlog.get_logger(__name__)


def serialize_for_audit(obj: Any) -> Dict[str, Any]:
    """Serialize object for audit logging, handling special types."""
    if hasattr(obj, "__dict__"):
        # SQLAlchemy model or similar object
        result = {}
        for key, value in obj.__dict__.items():
            if key.startswith("_"):
                continue

            if hasattr(value, "isoformat"):  # datetime objects
                result[key] = value.isoformat()
            elif isinstance(value, (str, int, float, bool, type(None))):
                result[key] = value
            elif isinstance(value, (list, tuple)):
                result[key] = [serialize_for_audit(item) for item in value]
            elif isinstance(value, dict):
                result[key] = {k: serialize_for_audit(v) for k, v in value.items()}
            else:
                result[key] = str(value)

        return result

    elif isinstance(obj, (list, tuple)):
        return [serialize_for_audit(item) for item in obj]

    elif isinstance(obj, dict):
        return {k: serialize_for_audit(v) for k, v in obj.items()}

    elif hasattr(obj, "isoformat"):  # datetime objects
        return obj.isoformat()

    elif isinstance(obj, (str, int, float, bool, type(None))):
        return obj

    else:
        return str(obj)


def audit_action(
    action: AuditAction,
    resource_type: str,
    get_resource_id: Optional[Callable] = None,
    get_resource_name: Optional[Callable] = None,
    description: Optional[str] = None,
    track_changes: bool = True,
):
    """
    Decorator to automatically audit API endpoint actions.

    Args:
        action: The type of action being performed
        resource_type: The type of resource being acted upon
        get_resource_id: Function to extract resource ID from response/args
        get_resource_name: Function to extract resource name from response/args
        description: Optional description of the action
        track_changes: Whether to track before/after changes for UPDATE actions
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Extract common dependencies from function signature: by name first
            # (FastAPI always passes dependencies as keyword arguments), then by type.
            db: Optional[AsyncSession] = kwargs.get("db")
            request: Optional[Request] = kwargs.get("request")
            current_user = kwargs.get("current_user")

            for key, value in kwargs.items():
                if key in ("db", "request", "current_user"):
                    continue
                if db is None and isinstance(value, AsyncSession):
                    db = value
                elif request is None and isinstance(value, Request):
                    request = value
                elif (
                    current_user is None
                    and hasattr(value, "email")
                    and hasattr(value, "id")
                    and not isinstance(value, Request)
                ):
                    current_user = value

            # If db not in kwargs, check args (for positional arguments)
            if not db:
                for arg in args:
                    if isinstance(arg, AsyncSession):
                        db = arg
                        break

            if not db:
                # If we can't find db session, execute without audit logging
                return await func(*args, **kwargs)

            # Get old values for UPDATE actions
            old_values = None
            if action == AuditAction.UPDATE and track_changes:
                try:
                    # For update operations, try to get the current state
                    resource_id = None
                    if get_resource_id:
                        resource_id = get_resource_id(*args, **kwargs)
                    else:
                        # Try to find ID in args/kwargs
                        for arg in args:
                            if isinstance(arg, int):
                                resource_id = str(arg)
                                break

                        for key, value in kwargs.items():
                            if key.endswith("_id") and isinstance(value, int):
                                resource_id = str(value)
                                break

                    if resource_id:
                        # This would need to be customized per resource type
                        # For now, we'll skip old values extraction
                        pass
                except Exception:
                    pass

            # Execute the original function
            result = await func(*args, **kwargs)

            # Extract audit information
            resource_id = None
            resource_name = None
            new_values = None

            if get_resource_id:
                try:
                    resource_id = get_resource_id(result, *args, **kwargs)
                except Exception:
                    pass
            elif hasattr(result, "id"):
                resource_id = str(result.id)

            if get_resource_name:
                try:
                    resource_name = get_resource_name(result, *args, **kwargs)
                except Exception:
                    pass
            elif hasattr(result, "name"):
                resource_name = result.name
            elif hasattr(result, "code"):
                resource_name = result.code
            elif hasattr(result, "email"):
                resource_name = result.email

            if action in [AuditAction.CREATE, AuditAction.UPDATE] and result:
                try:
                    new_values = serialize_for_audit(result)
                except Exception:
                    pass

            # Get request context
            ip_address = None
            user_agent = None
            endpoint = None
            method = None

            if request:
                ip_address = get_client_ip(request)
                user_agent = get_user_agent(request)
                endpoint = str(request.url.path)
                method = request.method

            # Get user information
            user_id = None
            user_email = None
            if current_user:
                user_id = getattr(current_user, "id", None)
                user_email = getattr(current_user, "email", None)

            # Persist in a dedicated session: the request session never commits on
            # teardown, so rows added here used to be discarded (EPR-124).
            try:
                await AuditLogService.log_action_detached(
                    action=action,
                    resource_type=resource_type,
                    user_id=user_id,
                    user_email=user_email,
                    resource_id=resource_id,
                    resource_name=resource_name,
                    old_values=old_values,
                    new_values=new_values,
                    ip_address=ip_address,
                    user_agent=user_agent,
                    endpoint=endpoint,
                    method=method,
                    description=description,
                )
            except Exception as exc:  # noqa: BLE001 - never fail the main operation
                logger.warning("audit_log_write_failed", action=str(action), error=str(exc))

            return result

        return wrapper

    return decorator


class AuditContext:
    """Context manager for manual audit logging.

    ``db`` is accepted for backwards compatibility only; the row is written through
    ``AuditLogService.log_action_detached`` so it is committed regardless of what the
    caller does with its own session.
    """

    def __init__(
        self,
        db: Optional[AsyncSession],
        action: AuditAction,
        resource_type: str,
        user_id: Optional[int] = None,
        user_email: Optional[str] = None,
        resource_id: Optional[str] = None,
        resource_name: Optional[str] = None,
        description: Optional[str] = None,
        request: Optional[Request] = None,
    ):
        self.db = db
        self.action = action
        self.resource_type = resource_type
        self.user_id = user_id
        self.user_email = user_email
        self.resource_id = resource_id
        self.resource_name = resource_name
        self.description = description
        self.request = request
        self.old_values = None
        self.new_values = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:  # Only log if no exception occurred
            ip_address = None
            user_agent = None
            endpoint = None
            method = None

            if self.request:
                ip_address = get_client_ip(self.request)
                user_agent = get_user_agent(self.request)
                endpoint = str(self.request.url.path)
                method = self.request.method

            try:
                await AuditLogService.log_action_detached(
                    action=self.action,
                    resource_type=self.resource_type,
                    user_id=self.user_id,
                    user_email=self.user_email,
                    resource_id=self.resource_id,
                    resource_name=self.resource_name,
                    old_values=self.old_values,
                    new_values=self.new_values,
                    ip_address=ip_address,
                    user_agent=user_agent,
                    endpoint=endpoint,
                    method=method,
                    description=self.description,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("audit_log_write_failed", action=str(self.action), error=str(exc))

    def set_old_values(self, values: Any):
        """Set the old values for update operations."""
        self.old_values = serialize_for_audit(values)

    def set_new_values(self, values: Any):
        """Set the new values for create/update operations."""
        self.new_values = serialize_for_audit(values)

    def set_resource_info(self, resource_id: str, resource_name: Optional[str] = None):
        """Set resource identification information."""
        self.resource_id = resource_id
        if resource_name:
            self.resource_name = resource_name
