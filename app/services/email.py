"""Email service for sending transactional emails using Resend."""

import logging
from pathlib import Path
from typing import Optional

import resend
from jinja2 import Environment, FileSystemLoader

from app.core.config import get_settings
from app.models.user import User

logger = logging.getLogger(__name__)

# Get the templates directory
TEMPLATES_DIR = Path(__file__).parent.parent / "templates" / "email"


class EmailService:
    """Service for sending transactional emails via Resend."""

    def __init__(self):
        """Initialize the email service."""
        self.settings = get_settings()
        self.jinja_env = Environment(
            loader=FileSystemLoader(str(TEMPLATES_DIR)),
            autoescape=True,
        )
        # Configure Resend API key
        if self.settings.RESEND_API_KEY:
            resend.api_key = self.settings.RESEND_API_KEY

    @property
    def is_configured(self) -> bool:
        """Check if email service is properly configured."""
        return bool(self.settings.RESEND_API_KEY and self.settings.EMAILS_FROM_EMAIL)

    @property
    def from_email(self) -> str:
        """Get the from email address.

        Uses Resend's test email in development if domain is not verified.
        """
        # Use Resend's onboarding email for testing when domain not verified
        if self.settings.DEBUG or not self.settings.RESEND_API_KEY:
            return "onboarding@resend.dev"
        return self.settings.EMAILS_FROM_EMAIL

    async def _send_email(
        self,
        to_email: str,
        subject: str,
        html_content: str,
    ) -> bool:
        """Send an email using Resend.

        Args:
            to_email: Recipient email address
            subject: Email subject
            html_content: HTML content of the email

        Returns:
            True if email was sent successfully, False otherwise
        """
        if not self.is_configured:
            logger.warning("Email service not configured. Skipping email send.")
            # In development, log the email content for debugging
            logger.info(f"Would send email to {to_email}: {subject}")
            logger.debug(f"Email content: {html_content[:500]}...")
            return True  # Return True in dev mode to not block flows

        try:
            # Use test email (onboarding@resend.dev) for development
            # Production should use verified domain
            from_email = f"{self.settings.EMAILS_FROM_NAME} <{self.from_email}>"

            params: resend.Emails.SendParams = {
                "from": from_email,
                "to": [to_email],
                "subject": subject,
                "html": html_content,
            }

            email_response = resend.Emails.send(params)

            email_id = email_response.get('id', 'unknown') if isinstance(email_response, dict) else getattr(email_response, 'id', 'unknown')
            logger.info(f"Email sent successfully to {to_email}: {subject}, id={email_id}")
            return True

        except Exception as e:
            # Log the error but don't fail the operation
            logger.warning(f"Failed to send email to {to_email}: {e}")
            return False

    def _render_template(self, template_name: str, **context) -> str:
        """Render an email template.

        Args:
            template_name: Name of the template file
            **context: Template context variables

        Returns:
            Rendered HTML string
        """
        # Add common context
        context.update({
            "support_email": self.settings.SUPPORT_EMAIL,
            "company_name": "EnergyExe",
            "client_portal_url": self.settings.CLIENT_PORTAL_URL,
            "admin_portal_url": self.settings.ADMIN_PORTAL_URL,
        })

        template = self.jinja_env.get_template(template_name)
        return template.render(**context)

    async def send_verification_email(self, user: User, token: str) -> bool:
        """Send email verification email.

        Args:
            user: User object
            token: Email verification token

        Returns:
            True if email was sent successfully
        """
        # Extract user info before any async operations to avoid DetachedInstanceError
        user_email = user.email
        user_name = user.first_name or user.username

        verification_url = (
            f"{self.settings.CLIENT_PORTAL_URL}/verify-email?token={token}"
        )

        html_content = self._render_template(
            "verification.html",
            user_name=user_name,
            verification_url=verification_url,
            expire_hours=self.settings.EMAIL_VERIFICATION_EXPIRE_HOURS,
        )

        return await self._send_email(
            to_email=user_email,
            subject="Verify Your EnergyExe Account",
            html_content=html_content,
        )

    async def send_approval_email(self, user: User) -> bool:
        """Send account approval notification email.

        Args:
            user: User object that was approved

        Returns:
            True if email was sent successfully
        """
        # Extract user info before any async operations to avoid DetachedInstanceError
        user_email = user.email
        user_name = user.first_name or user.username

        login_url = f"{self.settings.CLIENT_PORTAL_URL}/login"

        html_content = self._render_template(
            "approved.html",
            user_name=user_name,
            login_url=login_url,
        )

        return await self._send_email(
            to_email=user_email,
            subject="Your EnergyExe Account Has Been Approved!",
            html_content=html_content,
        )

    async def send_rejection_email(self, user: User, reason: Optional[str] = None) -> bool:
        """Send account rejection notification email.

        Args:
            user: User object that was rejected
            reason: Optional rejection reason

        Returns:
            True if email was sent successfully
        """
        # Extract user info before any async operations to avoid DetachedInstanceError
        user_email = user.email
        user_name = user.first_name or user.username

        html_content = self._render_template(
            "rejected.html",
            user_name=user_name,
            reason=reason,
        )

        return await self._send_email(
            to_email=user_email,
            subject="Update on Your EnergyExe Account Application",
            html_content=html_content,
        )

    async def send_invitation_email(
        self,
        email: str,
        token: str,
        invited_by_name: str,
    ) -> bool:
        """Send invitation email to a new user.

        Args:
            email: Invitee email address
            token: Invitation token
            invited_by_name: Name of the person who sent the invitation

        Returns:
            True if email was sent successfully
        """
        invitation_url = f"{self.settings.CLIENT_PORTAL_URL}/invitation/{token}"

        html_content = self._render_template(
            "invitation.html",
            invitation_url=invitation_url,
            invited_by_name=invited_by_name,
            expire_days=self.settings.INVITATION_EXPIRE_DAYS,
        )

        return await self._send_email(
            to_email=email,
            subject=f"You've Been Invited to Join EnergyExe by {invited_by_name}",
            html_content=html_content,
        )

    async def send_password_reset_email(self, user: User, token: str) -> bool:
        """Send password reset email.

        Args:
            user: User object
            token: Password reset token

        Returns:
            True if email was sent successfully
        """
        # Extract user info before any async operations to avoid DetachedInstanceError
        user_email = user.email
        user_name = user.first_name or user.username

        reset_url = f"{self.settings.CLIENT_PORTAL_URL}/reset-password?token={token}"

        html_content = self._render_template(
            "password_reset.html",
            user_name=user_name,
            reset_url=reset_url,
            expire_hours=self.settings.PASSWORD_RESET_EXPIRE_HOURS,
        )

        return await self._send_email(
            to_email=user_email,
            subject="Reset Your EnergyExe Password",
            html_content=html_content,
        )

    async def send_password_changed_email(self, user: User) -> bool:
        """Send password changed confirmation email.

        Args:
            user: User object

        Returns:
            True if email was sent successfully
        """
        # Extract user info before any async operations to avoid DetachedInstanceError
        user_email = user.email
        user_name = user.first_name or user.username

        html_content = self._render_template(
            "password_changed.html",
            user_name=user_name,
        )

        return await self._send_email(
            to_email=user_email,
            subject="Your EnergyExe Password Has Been Changed",
            html_content=html_content,
        )


# Singleton instance
email_service = EmailService()
