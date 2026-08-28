# Audit System Guide

This guide provides comprehensive documentation for the audit logging system implemented in EnergyExe.

## Overview

The audit system records user actions in the `audit_logs` table for compliance, security and
"who is using the platform" questions. It captures the actor (user id/email), the action, the
resource, the originating client IP (proxy-aware — see below), user agent, endpoint/method and
optional before/after values.

### What is audited today (2026-08-28, EPR-124)

| Event | Where | Rows |
|---|---|---|
| Login success / failure (`/auth/login`, `/auth/token`) | `app/api/v1/endpoints/auth.py` — direct `AuditLogService.log_action` calls | `LOGIN` (failed attempts have `extra_metadata.success = false`; `user_email` is the *typed* username) |
| Registration (`/auth/register`) | `@audit_action(AuditAction.CREATE, "user")` | `CREATE` |
| Owners list / search / get / get-by-code | `@audit_action(AuditAction.ACCESS, "owner")` in `owners.py` | `ACCESS` |
| Owners create / update / delete | `@audit_action` in `owners.py` | `CREATE` / `UPDATE` / `DELETE` |

Everything else (users CRUD, wind farms, reports, portfolios, the brain agent, …) is **not**
audited — 3 of the ~50 endpoint modules carry the decorator. Reading the audit log itself is
deliberately not audited (it used to be: every admin page view would add "viewed audit log"
rows and inflate the counts being viewed). There is no `/auth/logout` endpoint, so `LOGOUT`
is never emitted, and there is no `users.last_login_at` column — "active clients" is currently
inferred from `LOGIN` rows only. Extending coverage is one decorator per endpoint (see Usage).

### How rows are persisted (read this before touching the decorator)

`get_db()` **never commits on teardown** — it only rolls back on exception and closes. Until
EPR-124 the decorator added its row to the request session with `commit=False`, "so the main
transaction commits it"; nothing ever did, so every decorated endpoint's row was discarded and
production held nothing but `LOGIN` rows (the four direct calls in `auth.py` commit
themselves). Audit rows are now written through `AuditLogService.log_action_detached(...)`,
which opens its own short-lived session and commits — independent of the request transaction,
so it survives endpoint rollbacks and never commits unrelated pending work. Tests point that
writer at the test engine via `set_audit_session_factory(...)` (see `tests/conftest.py`).

### Client IP behind the ALB

`request.client.host` is always the load balancer's private address (`100.64.x` / `172.31.x`).
Use `app.core.request_utils.get_client_ip(request)` — the **last** hop of `X-Forwarded-For`
(the one our ALB appended; earlier hops are client-supplied and spoofable), then `X-Real-IP`,
then the socket peer. Only the ALB can reach the task, so that last hop is trustworthy.

## Architecture

### Components

1. **Models** (`app/models/audit_log.py`)
   - `AuditLog`: Main model storing audit entries
   - `AuditAction`: Enum defining action types

2. **Services** (`app/services/audit_log.py`)
   - `AuditLogService`: Business logic for audit operations

3. **Core** (`app/core/audit.py`)
   - `@audit_action` decorator: Automatic audit tracking
   - `AuditContext`: Manual audit logging context manager
   - Helper functions for data serialization

4. **API Endpoints** (`app/api/v1/endpoints/audit_logs.py`)
   - RESTful endpoints for querying audit logs

5. **Frontend Components** (`src/components/audit-logs/`)
   - React components for viewing audit logs
   - Filtering and search capabilities

## Database Schema

```sql
CREATE TABLE audit_logs (
    id INTEGER PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
    user_email VARCHAR(255),
    action VARCHAR(50) NOT NULL,
    resource_type VARCHAR(100) NOT NULL,
    resource_id VARCHAR(100),
    resource_name VARCHAR(255),
    old_values JSON,
    new_values JSON,
    ip_address VARCHAR(45),
    user_agent VARCHAR(500),
    endpoint VARCHAR(255),
    method VARCHAR(10),
    description TEXT,
    extra_metadata JSON,
    created_at TIMESTAMP DEFAULT NOW()
);
```

### Indexed Fields

- `id`, `user_id`, `user_email`, `action`, `resource_type`, `resource_id`, `ip_address`, `created_at`

## Usage

### Automatic Audit Logging

Use the `@audit_action` decorator on API endpoints:

```python
from app.core.audit import audit_action
from app.models.audit_log import AuditAction

@router.post("/", response_model=Owner, status_code=201)
@audit_action(AuditAction.CREATE, "owner", description="Created owner")
async def create_owner(
    owner: OwnerCreate,
    db: AsyncSession = Depends(get_db),
    request: Request = None,
    current_user: User = Depends(get_current_user),
):
    return await OwnerService.create_owner(db, owner)
```

### Manual Audit Logging

For complex operations, use the `AuditContext` context manager:

```python
from app.core.audit import AuditContext
from app.models.audit_log import AuditAction

async def complex_operation(db: AsyncSession, user: User):
    async with AuditContext(
        db,
        AuditAction.UPDATE,
        "complex_operation",
        user_id=user.id,
        user_email=user.email,
        description="Performed complex business operation",
    ) as ctx:
        # Get old state
        old_data = await get_current_state()
        ctx.set_old_values(old_data)
        
        # Perform operation
        result = await perform_operation()
        
        # Set new state
        ctx.set_new_values(result)
        ctx.set_resource_info(str(result.id), result.name)
        
        return result
```

### Direct Service Usage

For full control, use the service directly. `log_action(db, ...)` commits the session you pass
(the login endpoints do this); `log_action_detached(...)` writes through its own session and is
what the decorator and `AuditContext` use:

```python
from app.services.audit_log import AuditLogService

await AuditLogService.log_action(
    db=db,
    action=AuditAction.LOGIN,
    resource_type="user",
    user_id=user.id,
    user_email=user.email,
    ip_address="192.168.1.100",
    description="Successful login",
    extra_metadata={"login_method": "password", "remember_me": True}
)
```

## Action Types

| Action | Description | Use Case |
|--------|-------------|----------|
| `CREATE` | Resource creation | New users, owners, projects |
| `UPDATE` | Resource modification | Profile updates, data changes |
| `DELETE` | Resource deletion | Record removal |
| `LOGIN` | Authentication success/failure | User login attempts |
| `LOGOUT` | Session termination | Defined but never emitted — there is no logout endpoint |
| `ACCESS` | Resource access | Viewing lists, individual records |

## API Endpoints

### Authentication Required
All audit log endpoints require authentication. Most require **superuser privileges**.

### Available Endpoints

```http
GET    /api/v1/audit-logs                    # List audit logs with filtering
GET    /api/v1/audit-logs/count              # Count audit logs
GET    /api/v1/audit-logs/summary            # Get summary statistics
GET    /api/v1/audit-logs/{id}               # Get specific audit log
GET    /api/v1/audit-logs/resource/{type}/{id} # Get resource history
GET    /api/v1/audit-logs/user/{id}/history  # Get user's audit history
GET    /api/v1/audit-logs/my/history         # Get current user's history
```

### Query Parameters

**Filtering:**
- `user_id`: Filter by user ID
- `user_email`: Filter by user email (partial match)
- `action`: Filter by action type
- `resource_type`: Filter by resource type
- `resource_id`: Filter by resource ID
- `ip_address`: Filter by IP address
- `date_from`: Filter from date (ISO format)
- `date_to`: Filter to date (ISO format)
- `search`: Search in names, emails, descriptions

**Pagination:**
- `skip`: Number of records to skip (default: 0)
- `limit`: Maximum records to return (default: 100, max: 1000)

### Example Requests

```bash
# Get recent audit logs
curl -H "Authorization: Bearer $TOKEN" \
  "/api/v1/audit-logs?limit=50"

# Get failed login attempts
curl -H "Authorization: Bearer $TOKEN" \
  "/api/v1/audit-logs?action=LOGIN&search=failed"

# Get user's activity in date range
curl -H "Authorization: Bearer $TOKEN" \
  "/api/v1/audit-logs?user_id=123&date_from=2024-01-01T00:00:00Z&date_to=2024-01-31T23:59:59Z"

# Get resource history
curl -H "Authorization: Bearer $TOKEN" \
  "/api/v1/audit-logs/resource/owner/456"
```

## Frontend Integration

### Access Control
- Audit logs menu item only appears for superusers
- Route protection enforces superuser requirement

### Components
- `AuditLogsPage`: Main page with filters and table
- `AuditLogsDataTable`: Sortable table with pagination
- `AuditLogDetailModal`: Detailed view of individual logs
- `AuditLogsFilters`: Advanced filtering interface

### Features
- **Real-time Updates**: Auto-refresh capabilities
- **Advanced Filtering**: Multiple criteria with date ranges
- **Detailed Views**: Full audit log details with JSON formatting
- **Export Options**: Copy/download audit data
- **Responsive Design**: Works on mobile and desktop

## Security Considerations

### Access Control
- **Superuser Only**: Audit log access restricted to superusers
- **User History Exception**: Users can view their own history
- **API Protection**: All endpoints require authentication

### Data Privacy
- **PII Handling**: Sensitive data properly masked
- **Retention**: Consider implementing data retention policies
- **Encryption**: Database encryption recommended for production

### Audit Trail Integrity
- **Immutable Logs**: Audit logs should never be modified
- **Backup Strategy**: Regular backups of audit data
- **Chain of Custody**: Proper access logging to audit logs themselves

## Performance Considerations

### Database Optimization
- **Indexes**: Key fields are indexed for fast queries
- **Partitioning**: Consider date-based partitioning for large datasets
- **Archival**: Implement archival strategy for old logs

### Query Optimization
- **Pagination**: Always use limit/offset for large result sets
- **Date Filtering**: Use date ranges to limit query scope
- **Selective Fields**: Only query needed fields

### Monitoring
- **Log Volume**: Monitor audit log growth
- **Query Performance**: Track slow queries
- **Storage Usage**: Monitor disk space usage

## Troubleshooting

### Common Issues

**1. Audit logs not being created**
- Check if decorator is applied correctly (`db` must be a keyword argument of the endpoint —
  FastAPI passes dependencies by name; without a session the decorator runs the endpoint un-audited)
- Look for `audit_log_write_failed` warnings in the logs — the detached writer never raises
- Do NOT reintroduce `commit=False` on the request session: `get_db()` never commits (EPR-124)
- `GET /audit-logs/summary` reporting a number that is roughly `count²` means the summary
  query cross-joined `audit_logs` again (fixed in EPR-124 — each aggregate filters directly)

**2. Missing context information**
- Ensure `Request` object is passed to decorated functions
- Check user authentication
- Verify current_user dependency

**3. Frontend access denied**
- Confirm user has `is_superuser = True`
- Check authentication token validity
- Verify route protection

### Debug Mode

Enable debug logging in development:

```python
import logging
logging.getLogger("app.core.audit").setLevel(logging.DEBUG)
```

## Migration

### Database Migration

Run the migration to create the audit_logs table:

```bash
cd energyexe-core-backend
poetry run alembic upgrade head
```

### Existing Data

The audit system starts logging from the point of deployment. Historical data is not automatically audited.

## Compliance

### Regulations
- **GDPR**: User consent for activity tracking
- **SOX**: Financial data access logging
- **HIPAA**: Healthcare data access requirements

### Reports
- **Access Reports**: Who accessed what and when
- **Change Reports**: What was modified and by whom
- **Security Reports**: Failed login attempts, suspicious activity

## Best Practices

### Implementation
1. **Consistent Usage**: Apply audit decorators to all CRUD operations
2. **Meaningful Descriptions**: Provide clear, business-relevant descriptions
3. **Metadata Usage**: Include relevant context in extra_metadata field
4. **Error Handling**: Ensure audit failures don't break business operations

### Monitoring
1. **Regular Reviews**: Periodic audit log reviews
2. **Anomaly Detection**: Alert on unusual patterns
3. **Retention Policies**: Define and implement data retention
4. **Access Monitoring**: Monitor who accesses audit logs

### Security
1. **Principle of Least Privilege**: Restrict audit log access
2. **Separate Storage**: Consider separate database for audit logs
3. **Backup Strategy**: Regular, secure backups
4. **Incident Response**: Include audit logs in incident procedures

## Support

For questions or issues with the audit system:

1. Check the logs for error messages
2. Review the test suite for usage examples
3. Consult the API documentation
4. Contact the development team

---

*This guide covers version 1.0 of the audit system. Keep this document updated as the system evolves.*