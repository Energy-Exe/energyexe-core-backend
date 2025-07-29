from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, desc, func, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.models.audit_log import AuditAction, AuditLog
from app.models.user import User
from app.schemas.audit_log import AuditLogCreate, AuditLogFilter, AuditLogSummary


class AuditLogService:
    """Service for managing audit logs."""

    @staticmethod
    async def create_audit_log(
        db: AsyncSession, audit_data: AuditLogCreate, commit: bool = True
    ) -> AuditLog:
        """Create a new audit log entry."""
        db_audit = AuditLog(**audit_data.model_dump())
        db.add(db_audit)
        if commit:
            await db.commit()
            await db.refresh(db_audit)
        return db_audit

    @staticmethod
    async def log_action(
        db: AsyncSession,
        action: AuditAction,
        resource_type: str,
        user_id: Optional[int] = None,
        user_email: Optional[str] = None,
        resource_id: Optional[str] = None,
        resource_name: Optional[str] = None,
        old_values: Optional[Dict[str, Any]] = None,
        new_values: Optional[Dict[str, Any]] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        endpoint: Optional[str] = None,
        method: Optional[str] = None,
        description: Optional[str] = None,
        extra_metadata: Optional[Dict[str, Any]] = None,
        commit: bool = True,
    ) -> AuditLog:
        """Convenience method to log an action."""
        audit_data = AuditLogCreate(
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
            extra_metadata=extra_metadata,
        )
        return await AuditLogService.create_audit_log(db, audit_data, commit=commit)

    @staticmethod
    async def get_audit_logs(
        db: AsyncSession,
        filters: Optional[AuditLogFilter] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> List[AuditLog]:
        """Get audit logs with optional filtering."""
        query = select(AuditLog).order_by(desc(AuditLog.created_at))

        if filters:
            conditions = []

            if filters.user_id:
                conditions.append(AuditLog.user_id == filters.user_id)

            if filters.user_email:
                conditions.append(AuditLog.user_email.ilike(f"%{filters.user_email}%"))

            if filters.action:
                conditions.append(AuditLog.action == filters.action)

            if filters.resource_type:
                conditions.append(AuditLog.resource_type == filters.resource_type)

            if filters.resource_id:
                conditions.append(AuditLog.resource_id == filters.resource_id)

            if filters.ip_address:
                conditions.append(AuditLog.ip_address == filters.ip_address)

            if filters.date_from:
                conditions.append(AuditLog.created_at >= filters.date_from)

            if filters.date_to:
                conditions.append(AuditLog.created_at <= filters.date_to)

            if filters.search:
                search_pattern = f"%{filters.search}%"
                conditions.append(
                    or_(
                        AuditLog.resource_name.ilike(search_pattern),
                        AuditLog.description.ilike(search_pattern),
                        AuditLog.user_email.ilike(search_pattern),
                    )
                )

            if conditions:
                query = query.where(and_(*conditions))

        query = query.offset(skip).limit(limit)
        result = await db.execute(query)
        return result.scalars().all()

    @staticmethod
    async def get_audit_log(db: AsyncSession, log_id: int) -> Optional[AuditLog]:
        """Get a specific audit log by ID."""
        result = await db.execute(select(AuditLog).where(AuditLog.id == log_id))
        return result.scalar_one_or_none()

    @staticmethod
    async def count_audit_logs(db: AsyncSession, filters: Optional[AuditLogFilter] = None) -> int:
        """Count audit logs with optional filtering."""
        query = select(func.count(AuditLog.id))

        if filters:
            conditions = []

            if filters.user_id:
                conditions.append(AuditLog.user_id == filters.user_id)

            if filters.user_email:
                conditions.append(AuditLog.user_email.ilike(f"%{filters.user_email}%"))

            if filters.action:
                conditions.append(AuditLog.action == filters.action)

            if filters.resource_type:
                conditions.append(AuditLog.resource_type == filters.resource_type)

            if filters.resource_id:
                conditions.append(AuditLog.resource_id == filters.resource_id)

            if filters.ip_address:
                conditions.append(AuditLog.ip_address == filters.ip_address)

            if filters.date_from:
                conditions.append(AuditLog.created_at >= filters.date_from)

            if filters.date_to:
                conditions.append(AuditLog.created_at <= filters.date_to)

            if filters.search:
                search_pattern = f"%{filters.search}%"
                conditions.append(
                    or_(
                        AuditLog.resource_name.ilike(search_pattern),
                        AuditLog.description.ilike(search_pattern),
                        AuditLog.user_email.ilike(search_pattern),
                    )
                )

            if conditions:
                query = query.where(and_(*conditions))

        result = await db.execute(query)
        return result.scalar()

    @staticmethod
    async def get_audit_summary(
        db: AsyncSession,
        filters: Optional[AuditLogFilter] = None,
    ) -> AuditLogSummary:
        """Get summary statistics for audit logs."""
        base_query = select(AuditLog)

        if filters:
            conditions = []

            if filters.date_from:
                conditions.append(AuditLog.created_at >= filters.date_from)

            if filters.date_to:
                conditions.append(AuditLog.created_at <= filters.date_to)

            if conditions:
                base_query = base_query.where(and_(*conditions))

        # Total actions
        total_result = await db.execute(
            select(func.count(AuditLog.id)).select_from(base_query.subquery())
        )
        total_actions = total_result.scalar() or 0

        # Actions by type
        actions_by_type_result = await db.execute(
            select(AuditLog.action, func.count(AuditLog.id))
            .select_from(base_query.subquery())
            .group_by(AuditLog.action)
        )
        actions_by_type = dict(actions_by_type_result.all())

        # Actions by user (top 10)
        actions_by_user_result = await db.execute(
            select(
                AuditLog.user_email, AuditLog.user_id, func.count(AuditLog.id).label("action_count")
            )
            .select_from(base_query.subquery())
            .where(AuditLog.user_email.isnot(None))
            .group_by(AuditLog.user_email, AuditLog.user_id)
            .order_by(desc(func.count(AuditLog.id)))
            .limit(10)
        )
        actions_by_user = [
            {
                "user_email": row.user_email,
                "user_id": row.user_id,
                "action_count": row.action_count,
            }
            for row in actions_by_user_result.all()
        ]

        # Actions by resource type
        actions_by_resource_result = await db.execute(
            select(AuditLog.resource_type, func.count(AuditLog.id))
            .select_from(base_query.subquery())
            .group_by(AuditLog.resource_type)
        )
        actions_by_resource = dict(actions_by_resource_result.all())

        # Date range
        date_range_result = await db.execute(
            select(
                func.min(AuditLog.created_at).label("earliest"),
                func.max(AuditLog.created_at).label("latest"),
            ).select_from(base_query.subquery())
        )
        date_range_row = date_range_result.first()
        date_range = {
            "earliest": date_range_row.earliest if date_range_row else None,
            "latest": date_range_row.latest if date_range_row else None,
        }

        return AuditLogSummary(
            total_actions=total_actions,
            actions_by_type=actions_by_type,
            actions_by_user=actions_by_user,
            actions_by_resource=actions_by_resource,
            date_range=date_range,
        )

    @staticmethod
    async def get_resource_audit_history(
        db: AsyncSession,
        resource_type: str,
        resource_id: str,
        skip: int = 0,
        limit: int = 100,
    ) -> List[AuditLog]:
        """Get audit history for a specific resource."""
        result = await db.execute(
            select(AuditLog)
            .where(
                and_(
                    AuditLog.resource_type == resource_type,
                    AuditLog.resource_id == resource_id,
                )
            )
            .order_by(desc(AuditLog.created_at))
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()

    @staticmethod
    async def get_user_audit_history(
        db: AsyncSession,
        user_id: int,
        skip: int = 0,
        limit: int = 100,
    ) -> List[AuditLog]:
        """Get audit history for a specific user."""
        result = await db.execute(
            select(AuditLog)
            .where(AuditLog.user_id == user_id)
            .order_by(desc(AuditLog.created_at))
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()
