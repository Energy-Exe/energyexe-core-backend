# SCADA demo and pilot logins

A demo or pilot login is an ordinary client account that may also open the SCADA and Predict
portals, for chosen farms only. It is never an administrator: an administrator can manage users,
read audit logs and open every admin page.

## What the account sees

- **Perform**: everything, like any client account.
- **SCADA and Predict**: every page, but only for the farms it is granted. The farm picker lists
  only those farms. Asking the server for any other farm returns "not found", and the charts that
  normally cover every farm show only the granted ones.
- **Value in Play and Actions**: shown for the granted farms. Otherwise these pages are for
  EnergyExe staff only.
- **Hidden**: PPAs, which are EnergyExe staff only.

The account is **read-only**, because several people share it:

- it cannot change a finding's status, which every user sees;
- it cannot change its own email, username or password.

## How the grant is stored

Rows in the per-user features table:

| Feature | Meaning |
|---|---|
| `scada_portal` | may open SCADA and Predict |
| `scada_farm:<farm>` (for example `scada_farm:hill_of_towie`) | may read that farm; any such row makes the account farm-limited and read-only |

No migration is involved. Administrators are never farm-limited.

## Create or update a login

```bash
DEMO_PASSWORD='…' poetry run python scripts/create_demo_user.py \
  --email demo-user@energyexe.com --username demo-user --farm hill_of_towie          # dry-run
# … add --apply to write
```

`DATABASE_URL` decides the target. For production, open the database tunnel through the bastion
first, as for any hand-run production script. Re-running the script resets the password and
replaces the farm list with exactly the `--farm` values given. Keep the password in AWS Secrets
Manager (`energyexe/prod/demo-user`), never in the repo.

## Revoke

Either deactivate the account (`is_active = false`), or delete its `scada_portal` and
`scada_farm:*` feature rows. Deleting the rows makes it a plain client account again.
