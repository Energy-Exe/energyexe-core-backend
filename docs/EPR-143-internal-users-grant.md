# EPR-143 — granting `users.is_internal` (per environment, by hand)

The SCADA PPA register is shared per wind farm and visible only to **internal** users:
`is_superuser AND is_internal`. Migration `a7c1e4f9d2b8` adds the column with **no backfill**
(the email is user-editable, so it cannot bootstrap an entitlement). After the migration every
account is `is_internal = false` and `GET /api/v1/scada/ppas` answers 403 for everyone until the
flag is granted.

Grant it per environment against a **vetted id list**, never by email pattern:

```sql
-- 1. read
SELECT id, username, email, is_superuser FROM users WHERE is_superuser ORDER BY id;
-- 2. grant exactly the ids you checked (staging 2026-09-25: the six @energyexe.com superusers)
UPDATE users SET is_internal = true WHERE id IN (1, 2, 9, 10, 12, 18);
-- 3. verify
SELECT id, username, is_internal FROM users WHERE is_internal ORDER BY id;
```

Prod: the same three statements through `energyexe-scada-pipeline/scripts/prod_tunnel.sh`, with the
id list re-read on prod (ids differ between environments). The flag is not on any update schema and
cannot be set through the API; an internal account can only be edited or deleted by another internal
account (`PUT`/`DELETE /api/v1/users/{id}` → 403 otherwise).

## What else the flag gates (review round 2, 2026-09-26)

Two further surfaces honour `is_internal`, so both behave differently until the grant is applied:

- **Audit-log reads** (`/api/v1/audit-logs/*`): rows with `resource_type = 'scada_ppa'` carry the
  serialized contract row in `new_values`. A superuser without the flag no longer sees them (they
  are excluded from list / count / summary / histories, `GET /{id}` answers 404, and naming the
  resource type explicitly answers 403).
- **Brain agent, admin profile**: the admin profile runs on the shared read-only Postgres role,
  which has `SELECT` on every public table including `scada_ppa`. It is now internal-staff only.
  Until the grant is applied **every** admin-portal agent session runs on the client profile and the
  allowlisted client RO role (logged as `brain_agent_admin_profile_denied`); the `/map/interpret`
  stream follows the same rule.
