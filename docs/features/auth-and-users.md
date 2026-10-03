# Auth, users and access control

JWT bearer auth with bcrypt passwords, two user populations (internal staff vs. client-portal
users), email flows via Resend, invitations, legal-consent tracking, and a small set of
dependency functions that every endpoint uses to gate access. Code:
[`app/core/security.py`](../../app/core/security.py), [`app/core/deps.py`](../../app/core/deps.py),
[`app/core/agent_access.py`](../../app/core/agent_access.py),
[`app/api/v1/endpoints/{auth,users,admin,consents}.py`](../../app/api/v1/endpoints/),
[`app/services/{user,invitation,consent,email}.py`](../../app/services/).

## Tokens

`create_access_token(subject, expires_delta)` / `verify_token(token)` in `security.py` —
HS256 JWTs signed with `SECRET_KEY`, lifetime `ACCESS_TOKEN_EXPIRE_MINUTES`. The subject is the
username. Passwords: `get_password_hash` / `verify_password` (bcrypt). The frontends keep the
token in `localStorage` (`auth_token`) and send it as `Authorization: Bearer …`.

## The `users` row (`app/models/user.py`)

| Column | Meaning |
|---|---|
| `role` (`client` default / `admin`) and `is_superuser` | admin-ui access is `is_superuser OR role == 'admin'` (`get_current_admin_user`) |
| `is_internal` | **EnergyExe staff** (EPR-143). Granted per environment by SQL against a vetted id list — never derived from the email domain, which a user can change through `PUT /users/me`. Runbook: [`docs/operations/EPR-143-internal-users-grant.md`](../operations/EPR-143-internal-users-grant.md). |
| `is_active` | deactivated accounts cannot authenticate |
| `is_approved`, `approved_at`, `approved_by_id` | client registrations wait for admin approval |
| `email_verified`, `email_verification_token/_sent_at` | client registrations must verify email before login (403 otherwise) |
| `password_reset_token/_sent_at` | forgot/reset flow |
| `company_name`, `phone` | client profile |

## Dependencies (`app/core/deps.py`) — pick the right one

| Dependency | Lets through |
|---|---|
| `get_current_user` | valid token, existing user, plus the client gates (active, verified, approved) |
| `get_current_user_basic` | valid token + active only — for endpoints that must work before approval |
| `get_current_user_optional` | the user when a valid token is supplied, otherwise `None` — never raises (public reads with client-style filtering) |
| `get_current_active_user` | `get_current_user` + `is_active` |
| `get_current_admin_user` | superuser **or** `role == 'admin'` |
| `get_current_superuser` | `is_superuser` |
| `get_current_internal_user` | superuser **and** `is_internal` — guards the SCADA PPA register (`/scada/ppas`) and the `/scada/ingestion-summary` read; a superuser without the flag gets 403 |
| `get_brain_agent_user` | `get_current_user` + `require_agent_access` (below) |

Helpers: `is_client_request(user)` — `True` for anonymous and for non-admin, non-superuser users;
drives visibility filtering (hidden sections, soft-deleted farms, report scoping). Note it is
`False` for superusers, so an admin browsing the client portal is **not** treated as a client
except for soft-deleted farms (`exclude_deleted`). `INTERNAL_ONLY_RESOURCE_TYPES = {"scada_ppa"}`
hides those audit rows from non-internal readers of `/audit-logs`.

## Brain-agent access policy (`app/core/agent_access.py`)

`require_agent_access(user, source)` denies when: inactive; a `client`-role user who is not
verified+approved; `BRAIN_AGENT_ACCESS_POLICY == "superusers"` and the user is not a superuser
(default policy is `"authenticated"`); or `source == "admin"` and the user is not
`is_superuser AND is_internal` (`is_internal_staff`). `require_fresh_agent_access` re-reads
those columns from the DB so a cached session cannot outlive a demotion. The two read-only
database roles the agent connects with are in
[`brain-agent/readonly-role.md`](brain-agent/readonly-role.md).

## Endpoints

**`/auth`** — `POST /register` (internal, audited `CREATE user`), `POST /client/register`
(client self-signup → verification email, then approval), `POST /login` (JSON) and `POST /token`
(OAuth2 form), `POST /verify-email`, `POST /resend-verification`, `POST /forgot-password`,
`POST /reset-password`, `GET /invitation/{token}` (validate), `POST /invitation/{token}/accept`
(creates the invited user). Login success/failure is written to the audit log
([`audit-system.md`](audit-system.md)). There is no logout endpoint; tokens simply expire.

**`/users`** — `GET/PUT /me`, admin `GET /`, `GET/PUT/DELETE /{user_id}`.

**`/admin`** (`get_current_admin_user`) — `GET /users/pending`, `POST /users/{id}/approve`,
`/reject`, `/deactivate`, `/reactivate`; per-user feature flags `GET/PUT /users/{id}/features`;
invitations `GET /invitations`, `POST /invitations`, `POST /invitations/bulk`,
`POST /invitations/{id}/resend`, `DELETE /invitations/{id}` (`app/services/invitation.py`,
`INVITATION_EXPIRE_DAYS`, default 7).

**`/consents`** — `GET /me`, `POST /me/accept` (`app/services/consent.py`,
`user_consents` table). Bumping `TERMS_VERSION` / `PRIVACY_VERSION` in `config.py` forces
re-acceptance at next login; the same constants live in client-ui
`src/lib/legal-versions.ts` and must move together.

## Email (`app/services/email.py`)

Resend (`RESEND_API_KEY`; empty = sends are no-ops in dev), Jinja2 templates in
`app/templates/email/` (`verification`, `approved`, `rejected`, `invitation`,
`password_reset`, `password_changed`, on `base.html`). Links are built from
`CLIENT_PORTAL_URL` / `ADMIN_PORTAL_URL`; expiries from `EMAIL_VERIFICATION_EXPIRE_HOURS` (24)
and `PASSWORD_RESET_EXPIRE_HOURS` (1). Diagnosing a missing reset mail: check the Resend
dashboard first, then the backend log line for the send.

## Local fixtures

The seeded `admin` / `adminenergyexe` account is a **local dev fixture**; see the workspace
`CLAUDE.md` for how it is used in tests. It must not exist with that password on a deployed
environment.
