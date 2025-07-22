import json
from functools import wraps
from typing import Any, Callable, Dict, Optional

from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.audit_log import AuditAction
from app.services.audit_log import AuditLogService


def get_client_ip(request: Request) -> Optional[str]:
    """Extract client IP address from request."""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    
    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        return real_ip
    
    return request.client.host if request.client else None


def get_user_agent(request: Request) -> Optional[str]:
    """Extract user agent from request."""
    return request.headers.get("User-Agent")


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
    
    elif isinstance(value, (list, tuple)):
        return [serialize_for_audit(item) for item in obj]
    
    elif isinstance(value, dict):
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
            # Extract common dependencies from function signature
            db: Optional[AsyncSession] = None
            request: Optional[Request] = None
            current_user = None
            
            # Find dependencies in kwargs
            for key, value in kwargs.items():
                if isinstance(value, AsyncSession):
                    db = value
                elif isinstance(value, Request):
                    request = value
                elif hasattr(value, 'email') and hasattr(value, 'id'):
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
                            if key.endswith('_id') and isinstance(value, int):
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
            elif hasattr(result, 'id'):
                resource_id = str(result.id)
            
            if get_resource_name:
                try:
                    resource_name = get_resource_name(result, *args, **kwargs)
                except Exception:
                    pass
            elif hasattr(result, 'name'):
                resource_name = result.name
            elif hasattr(result, 'code'):
                resource_name = result.code
            elif hasattr(result, 'email'):
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
                user_id = getattr(current_user, 'id', None)
                user_email = getattr(current_user, 'email', None)
            
            # Create audit log
            try:
                await AuditLogService.log_action(
                    db=db,
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
                    commit=False,  # Don't commit separately, let the main transaction handle it
                )
            except Exception as e:
                # Log the error but don't fail the main operation
                print(f"Failed to create audit log: {e}")
            
            return result
        
        return wrapper
    return decorator


class AuditContext:
    """Context manager for manual audit logging."""
    
    def __init__(
        self,
        db: AsyncSession,
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
                await AuditLogService.log_action(
                    db=self.db,
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
                    commit=False,
                )
            except Exception as e:
                print(f"Failed to create audit log: {e}")
    
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