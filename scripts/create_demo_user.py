"""Create or update a farm-limited SCADA demo login (a plain client account, never a superuser).

The account sees all of Perform like any client, and the SCADA + Predict portals for the granted
farms only, read-only (see app/core/scada_access.py and docs/operations/scada-demo-access.md).

Run (dry-run by default; DATABASE_URL decides the target, as for every script here):
    DEMO_PASSWORD='...' poetry run python scripts/create_demo_user.py \\
        --email demo-user@energyexe.com --username demo-user --farm hill_of_towie
    ... --apply        # write

The password is read from DEMO_PASSWORD only, so it never lands in shell history as an argument or
in the repo. Re-running updates the same account (by email): it resets the password, keeps it a
plain approved client, and replaces its SCADA farm grant with exactly the --farm values given.
"""

import argparse
import asyncio
import os
import sys

from sqlalchemy import delete, select

from app.core.database import get_session_factory
from app.core.scada_access import SCADA_FARM_FEATURE_PREFIX, SCADA_PORTAL_FEATURE
from app.core.security import get_password_hash
from app.models.user import User
from app.models.user_feature import DEFAULT_FEATURES, UserFeature
from app.services.user import UserService


async def main(email: str, username: str, farms: list[str], company: str, apply: bool) -> None:
    password = os.environ.get("DEMO_PASSWORD", "")
    if len(password) < 8:
        sys.exit("Set DEMO_PASSWORD (8+ characters) in the environment.")

    async with get_session_factory()() as db:
        service = UserService(db)
        user = await service.get_by_email(email)
        clash = await service.get_by_username(username)
        if clash is not None and (user is None or clash.id != user.id):
            sys.exit(f"Username {username!r} already belongs to another account (id {clash.id}).")
        if user is not None and (user.is_superuser or user.is_internal):
            sys.exit(
                f"{email} is a superuser or internal account; refusing to turn it into a demo login."
            )

        action = "update" if user is not None else "create"
        if user is None:
            user = User(email=email.lower(), username=username, hashed_password="")
            db.add(user)
        user.username = username
        user.hashed_password = get_password_hash(password)
        user.first_name, user.last_name = "Demo", "User"
        user.company_name = company
        user.role = "client"
        user.is_active = True
        user.is_superuser = False
        user.is_internal = False
        user.is_approved = True
        user.email_verified = True
        await db.flush()

        # Replace the SCADA grant; keep (or add) the default client features.
        await db.execute(
            delete(UserFeature).where(
                UserFeature.user_id == user.id,
                (UserFeature.feature_key == SCADA_PORTAL_FEATURE)
                | UserFeature.feature_key.startswith(SCADA_FARM_FEATURE_PREFIX),
            )
        )
        have = set(
            (
                await db.execute(
                    select(UserFeature.feature_key).where(UserFeature.user_id == user.id)
                )
            ).scalars()
        )
        keys = [SCADA_PORTAL_FEATURE] + [f"{SCADA_FARM_FEATURE_PREFIX}{f}" for f in farms]
        keys += [k for k in DEFAULT_FEATURES if k not in have]
        for key in keys:
            db.add(UserFeature(user_id=user.id, feature_key=key, enabled=True))

        print(f"{'APPLY' if apply else 'DRY  '} {action} user id={user.id} {email} ({username})")
        print(
            f"      role=client approved verified, not superuser, not internal; company={company!r}"
        )
        print(f"      SCADA grant: {SCADA_PORTAL_FEATURE} + farms {sorted(farms)}")
        if apply:
            await db.commit()
            print("Done.")
        else:
            await db.rollback()
            print("Dry-run: nothing written; re-run with --apply.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--email", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument(
        "--farm", action="append", required=True, help="SCADA farm slug; repeatable"
    )
    parser.add_argument("--company", default="EnergyExe demo")
    parser.add_argument("--apply", action="store_true", help="write (default is a dry-run)")
    args = parser.parse_args()
    asyncio.run(main(args.email, args.username, args.farm, args.company, args.apply))
