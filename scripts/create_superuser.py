#!/usr/bin/env python3
"""
Script to create superuser or promote existing user to superuser.
Usage:
    python scripts/create_superuser.py --email user@example.com
    python scripts/create_superuser.py --username myuser
    python scripts/create_superuser.py --create --email admin@example.com --username admin --password secretpass
"""

import argparse
import asyncio
import sys
from typing import Optional

# Add the parent directory to sys.path to import app modules
sys.path.append(".")

from app.core.database import async_session_factory
from app.core.security import get_password_hash
from app.schemas.user import UserCreate
from app.services.user import UserService


async def promote_user_to_superuser(email: Optional[str] = None, username: Optional[str] = None):
    """Promote an existing user to superuser status."""
    if not email and not username:
        print("Error: Either email or username must be provided")
        return False

    async with async_session_factory() as db:
        user_service = UserService(db)

        # Find the user
        if email:
            user = await user_service.get_by_email(email)
            search_field = f"email '{email}'"
        else:
            user = await user_service.get_by_username(username)
            search_field = f"username '{username}'"

        if not user:
            print(f"Error: No user found with {search_field}")
            return False

        if user.is_superuser:
            print(f"User {user.email} ({user.username}) is already a superuser")
            return True

        # Promote to superuser
        user.is_superuser = True
        await db.commit()
        await db.refresh(user)

        print(f"✅ Successfully promoted user {user.email} ({user.username}) to superuser")
        return True


async def create_superuser(
    email: str,
    username: str,
    password: str,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
):
    """Create a new superuser account."""
    async with async_session_factory() as db:
        user_service = UserService(db)

        # Check if user already exists
        existing_email = await user_service.get_by_email(email)
        if existing_email:
            print(f"Error: User with email '{email}' already exists")
            return False

        existing_username = await user_service.get_by_username(username)
        if existing_username:
            print(f"Error: User with username '{username}' already exists")
            return False

        # Create the superuser
        user_data = UserCreate(
            email=email,
            username=username,
            password=password,
            first_name=first_name,
            last_name=last_name,
            is_active=True,
        )

        try:
            user = await user_service.create(user_data)
            # Promote to superuser
            user.is_superuser = True
            await db.commit()
            await db.refresh(user)

            print(f"✅ Successfully created superuser {user.email} ({user.username})")
            return True
        except Exception as e:
            print(f"Error creating superuser: {e}")
            return False


async def list_users():
    """List all users and their superuser status."""
    async with async_session_factory() as db:
        user_service = UserService(db)
        users = await user_service.get_all(skip=0, limit=1000)

        if not users:
            print("No users found in the database")
            return

        print("\n📋 Current Users:")
        print("-" * 80)
        print(f"{'ID':<4} {'Email':<30} {'Username':<20} {'Superuser':<10} {'Active':<8}")
        print("-" * 80)

        for user in users:
            print(
                f"{user.id:<4} {user.email:<30} {user.username:<20} "
                f"{'Yes' if user.is_superuser else 'No':<10} "
                f"{'Yes' if user.is_active else 'No':<8}"
            )


async def main():
    parser = argparse.ArgumentParser(description="Manage superuser accounts")
    parser.add_argument("--email", help="User email address")
    parser.add_argument("--username", help="Username")
    parser.add_argument("--password", help="Password (for creating new user)")
    parser.add_argument("--first-name", help="First name (for creating new user)")
    parser.add_argument("--last-name", help="Last name (for creating new user)")
    parser.add_argument(
        "--create", action="store_true", help="Create new superuser instead of promoting existing"
    )
    parser.add_argument("--list", action="store_true", help="List all users")

    args = parser.parse_args()

    if args.list:
        await list_users()
        return

    if args.create:
        if not all([args.email, args.username, args.password]):
            print("Error: --create requires --email, --username, and --password")
            sys.exit(1)

        success = await create_superuser(
            email=args.email,
            username=args.username,
            password=args.password,
            first_name=args.first_name,
            last_name=args.last_name,
        )
    else:
        if not args.email and not args.username:
            print("Error: Either --email or --username must be provided")
            print("Use --list to see all users")
            sys.exit(1)

        success = await promote_user_to_superuser(email=args.email, username=args.username)

    if success:
        print(
            "\n🔄 Please restart your frontend session (logout and login again) for changes to take effect"
        )
    else:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
