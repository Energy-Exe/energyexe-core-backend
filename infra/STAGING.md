# EnergyExe Staging Environment — Build Guide & Decision Log

> A complete account of how the AWS **staging** environment was designed and built, the
> alternatives considered, why each choice was made, and how every concern (DNS, routing,
> TLS, database, secrets, scaling, CI/CD, cost, prod-safety) is handled.
>
> **Audience:** anyone operating or extending the staging setup.
> **Source of truth for the code:** `energyexe-core-backend/infra/staging/` (separate Terraform root).
> **Companion docs:** the `/cloud` skill (prod topology), `infra/README.md` (prod runbooks).
> **Built:** 2026-06-26 → 2026-06-28.

---

## 1. Why staging exists (goal)

Before this work there was only **production**: merges to `master` auto-deployed straight to the
live Fargate backend, and the frontends deployed to Vercel. There was **no safe place to validate
a change before customers saw it** — and a routine `terraform apply` had already caused an
unintended prod redeploy.

The goal: a **staging-first workflow**. Every change lands in staging, is verified there, then is
promoted to prod. Staging must be:

- **Isolated** from prod in its data plane (own database, own app, own secrets).
- **Cheap** (it's a test bed, not production traffic).
- **Incapable of harming prod** — both structurally and by procedure.

---

## 2. Architecture at a glance

```
                         Internet  (DNS at hyp.net — manual CNAMEs, NOT Route53)

  staging-api.energyexe.com ─┐                         ┌─ host=staging-api.* → staging TG  (prio 110)
  api.energyexe.com ─────────┤   PROD ALB (shared) ────┼─ host=errors.*      → glitchtip TG (prio 100)
  errors.energyexe.com ──────┘   :80→301→:443 (SNI)    └─ default            → prod backend TG
                                        │                         │
                              ECS service (prod)        ECS service energyexe-core-backend-staging (NEW)
                              2 vCPU / 8 GB             0.5 vCPU / 1 GB, on-demand, pipeline OFF, no Valkey
                              cluster "energyexe" ◄──────── same SHARED cluster
                                        │                         │
                              RDS energyexedb (prod)     RDS energyexedb-staging (NEW)
                              db.t4g.large, public       db.t4g.micro, PRIVATE, snapshot-restored

  staging-dashboard.energyexe.com → CloudFront → S3 (admin-ui)  ┐  AWS-hosted staging frontends,
  staging-app.energyexe.com       → CloudFront → S3 (client-ui) ┘  pointed at staging-api.energyexe.com
```

**Shared with prod (to save cost):** the ALB, the ECS cluster, the default VPC/subnets, the
GlitchTip error tracker, and the external-API-key secrets.

**Separate (for isolation):** the RDS instance, the ECS service + task, the target group, the
service & DB security groups, the staging-specific secrets, the ECR repo, the OIDC deploy roles
(backend + one per frontend), the CloudWatch log group, and the two frontend stacks (private S3
bucket + CloudFront distribution + us-east-1 ACM cert each).

Everything runs in **AWS account `580639800175`, region `eu-north-1`**, default VPC
`vpc-04f8e9553ad77d458`, using AWS CLI profile **`energyexe`**.

---

## 3. The single most important design choice: a separate Terraform root

Prod's infrastructure is a flat Terraform root at `infra/` with **local state** and every resource
keyed off `local.name = "energyexe-core-backend"`. The prod stack is live and fragile.

**Decision:** put staging in its **own Terraform root** at `infra/staging/`, with its **own local
state**, that consumes shared prod infra **read-only via data sources** and creates *only* staging
resources.

### Options considered

| Option | How it works | Why rejected / chosen |
|---|---|---|
| **Terraform workspaces** on the prod root | `terraform workspace new staging`, gate every name/size on `terraform.workspace` | ✗ The shared ALB/cluster are *created* by the prod root; a staging workspace (isolated state) would try to create a *second* ALB unless every shared resource is conditionally gated — smearing risky conditionals across live prod files. |
| **Refactor into a module** + `envs/prod` + `envs/staging` | One reusable module, two thin roots | ✗ Cleanest long-term, but migrating prod's existing local state into a module (via `moved`/`state mv`) is risky on a live, just-incident-ed prod. Deferred as a future cleanup. |
| **Separate root + data sources** ✅ | New `infra/staging/` root, own state, `data` blocks for the shared ALB/listener/cluster/VPC | ✓ **Chosen.** Prod state is never read or written. A staging `apply` *structurally cannot* create/modify/destroy a prod-managed resource. Shared infra is referenced, never owned. |

### Why this guarantees prod-safety

1. **Separate state file** → an `apply` in `infra/staging/` can only touch resources in *its* state.
   We never run `apply` in the prod root for staging work.
2. **The shared ALB is only touched additively** — staging adds a *new* `aws_lb_listener_rule`
   (host = `staging-api.energyexe.com`, priority 110) and a *new* SNI `aws_lb_listener_certificate`.
   It never edits the listener's default action or prod's existing rules. Host rules match only
   their exact hostname, so they can't intercept `api.*` / `errors.*` traffic.
3. **The shared cluster is data-sourced, not managed** — adding a service to a cluster can't affect
   prod's service (Fargate capacity is per-task).
4. **Procedural gate:** before any `apply`, `terraform plan` must show **only creates** and **zero
   changes/destroys** to existing resources. The initial build plan was exactly
   **`40 to add, 0 to change, 0 to destroy`**, and prod health + task-def revision were re-checked
   unchanged after every apply.

> **Lesson that drove this:** earlier, a "targeted" `terraform apply -target=<alarm>` in the *prod*
> root unexpectedly redeployed the backend, because the targeted alarm *referenced* the ECS service,
> and `-target` pulls a resource's dependencies (which had a pending change) into the apply. A
> separate root removes that whole class of footgun.

---

## 4. Step-by-step: how the environment was built

1. **Wrote the staging Terraform root** (`infra/staging/`, 13 files): `main.tf` (two providers — the
   default `eu-north-1` plus a `us_east_1` alias for CloudFront certs), `data.tf` (read-only shared
   infra + reused secrets), `network.tf`, `rds.tf`, `secrets.tf`, `iam.tf`, `ecs.tf`, `alb.tf`,
   `cicd.tf`, `outputs.tf`, `variables.tf`, plus `terraform.tfvars` (gitignored) and `.gitignore`.
2. **`terraform init` + `validate` + `plan`** → verified the prod-safety gate (40 add / 0 change /
   0 destroy; the only non-creates were read-only data sources).
3. **Snapshot of prod** — `aws rds create-db-snapshot --db-instance-identifier energyexedb
   --db-snapshot-identifier energyexedb-staging-seed` (online, no prod downtime).
4. **`terraform apply`** — restored the staging RDS from the snapshot and created the ECS service,
   target group, ALB host rule, ECR repo, secrets, IAM/OIDC roles, log group.
5. **Populated secrets** — derived `database-url` from prod's (host swapped to the staging
   endpoint), generated a fresh `secret-key`.
6. **Bootstrapped the first image** — copied the **live prod image** into the staging ECR repo
   (`docker pull` prod `:latest` → tag → push staging `:staging`) and redeployed, so staging came up
   immediately on a known-good artifact while CI was being wired.
7. **Attached the TLS cert (phase 2)** — once `staging-api`'s ACM cert validated, set
   `staging_api_certificate_arn` and applied to attach the SNI cert to the shared listener.
8. **Wired CI** — committed `infra/staging/**` + `.github/workflows/deploy-staging.yml` to a new
   `staging` branch; a push to `staging` now auto-builds and deploys the staging backend (verified by
   merging EPR-48 and watching it deploy healthy).

---

## 5. How each concern is handled

### DNS — manual at hyp.net (not Route53)

`energyexe.com` is hosted at **hyp.net**, so all records are added by hand. Because validation can't
be automated, certs use a **two-phase pattern**: phase 1 creates the ACM cert and *outputs* the
validation CNAME; once the record is added and the cert ISSUES, phase 2 sets the cert ARN variable
and re-applies to wire it. Records added for staging:

- `staging-api.energyexe.com` **CNAME** → the prod ALB DNS name (`…elb.amazonaws.com`).
- One ACM validation CNAME for the `staging-api` cert.

> **Verification tip:** query hyp.net's authoritative nameservers directly
> (`dig +short CNAME <name> @ns1.hyp.net`) to distinguish a real typo from propagation lag — and
> note hyp.net replicates across `ns1/ns2/ns3` with a short internal lag.

### Routing — host-based rules on the shared ALB

Staging rides the **prod ALB** (the same mechanism GlitchTip already uses). A new
`aws_lb_listener_rule` on the `:443` listener matches `host = staging-api.energyexe.com` at
**priority 110** (GlitchTip is 100; prod backend is the listener default). The cert is presented via
**SNI** using `aws_lb_listener_certificate` — the listener now holds three certs (api, glitchtip,
staging-api). This is purely additive; the prod default action and rules are untouched.

> **Gotcha discovered:** ECS will not attach a service to a target group that has **no load-balancer
> association**. The listener rule therefore had to be created in *phase 1* (it doesn't need the
> cert — only the SNI attachment does), not gated behind the cert like the rest of phase 2.

### TLS / certificates

- `staging-api` cert: **ACM in `eu-north-1`**, DNS-validated, attached to the shared listener.
- Frontend CloudFront certs (`staging-dashboard` / `staging-app`): **must be in `us-east-1`** (a hard
  CloudFront requirement) — hence the second `us_east_1` provider alias. DNS-validated, attached to
  each distribution as an SNI alias in phase 2.

### Database — separate instance, restored from a prod snapshot

**Decision:** a **separate RDS instance** restored from a **prod snapshot** (realistic data), kept
**private**.

| Option | Trade-off | Decision |
|---|---|---|
| Empty DB + migrations/seeds | Cheapest (~$15/mo, no 200 GB floor), no PII | Not chosen — wanted realistic data |
| **Snapshot restore** ✅ | Realistic data; inherits the snapshot's ~200 GB storage (can't shrink) and real PII | **Chosen** |
| Sanitized subset | Realistic-for-cheap, but ongoing extract tooling to maintain | Not chosen — too much upkeep |

Handling: `energyexedb-staging` is **`db.t4g.micro`**, gp3, single-AZ, **`publicly_accessible = false`**
(tighter than prod — only the in-VPC task reaches it), with its own security group allowing 5432
*only* from the staging service SG. **PII mitigations:** private networking + a **fresh
`secret-key`** (so prod and staging JWTs never cross). A post-restore scrub is a documented future
option. To **refresh** staging data, restore a new instance from a fresh snapshot rather than via
this resource (`snapshot_identifier` changes are ignored — see §6).

### Secrets

- **New, staging-specific:** `energyexe/core-backend-staging/{database-url, secret-key, sentry-dsn}`
  (containers in Terraform, values set out-of-band so they never enter state).
- **Reused from prod** (read-only IAM grant): the external-API-key + brain-agent secrets — the
  brain-agent DB roles exist in the restored snapshot, so the same passwords work.

### Scaling / sizing & compute model

Staging serves only the API — the **nightly pipeline is disabled** (`PIPELINE_DAILY_ENABLED=false`),
so there's no scheduler load. The task is **0.5 vCPU / 1 GB** (vs prod's 2 vCPU / 8 GB), single task
(`desired_count = 1`). **Valkey is skipped** entirely (no `REDIS_URL`) — the report cache and rate
limiter degrade gracefully (no-cache / fail-open).

**Compute model — on-demand, not Spot.** Spot would be ~70% cheaper, but it requires associating a
`FARGATE_SPOT` capacity provider with the **shared cluster** — a prod-owned modification. To keep
staging from touching shared/prod config at all, staging uses **on-demand Fargate** (`launch_type =
FARGATE`), ~$12/mo more. Consistent with "prioritize prod-safety over cost."

### CI/CD — staging-first deploy flow

**Decision:** push to a long-lived **`staging` branch → deploys staging**; merge to **`master` →
prod** (promotion).

**Phase 2 — `master → prod` promotion (built; PR #134, pending merge).** Prod's `deploy-aws.yml` no
longer rebuilds on `master`: it **promotes the exact staging-validated image** — pulls the staging
`:staging` image, retags it into the prod ECR repo as `:latest` + `:<sha>`, and `force-new-deployment`s
prod. So prod runs the identical bytes staging tested. Prod IAM is **unchanged**: the prod deploy
role's pull access to the staging repo is granted from the staging side (an ECR repository policy in
`infra/staging/promotion.tf`). `workflow_dispatch` is kept for deliberate/first promotions, and the
prod service's circuit breaker auto-rolls back a bad image. **Operating rule:** reach `master` by
*merging* `staging`, so `:staging` matches master's code (a direct push to master would promote the
older staging image).

- **OIDC, no stored keys:** a separate `…-staging-github-deploy` role, trust pinned to
  `repo:Energy-Exe/energyexe-core-backend:ref:refs/heads/staging`, scoped to the staging ECR repo +
  staging ECS service only. Prod's deploy role is untouched.
- **`deploy-staging.yml`** (on push to `staging`): build the image → push to the staging ECR
  (`:staging` + `:<sha>`) → `update-service --force-new-deployment` → wait for stable. **No
  `paths-ignore`** — staging deploys on every push (redeploys are cheap on a single task; this also
  let the bootstrap commit trigger the first deploy, since `workflow_dispatch` only works for
  workflows present on the default branch).
- **Image bootstrap:** the very first staging task ran the **prod image** copied into the staging
  ECR, so staging was live before CI existed.
- **Frontends:** each repo (`faisal-energyexe/energyexe-{admin,client}-ui`) has its own
  `deploy-staging.yml` (on push to `staging`): `pnpm build` with
  `VITE_API_URL=https://staging-api.energyexe.com/api/v1` → `aws s3 sync dist/ --delete` →
  CloudFront invalidation. Each assumes its **own** OIDC role (`energyexe-staging-{admin,client}-ui-github-deploy`),
  scoped to that bucket + distribution only, trust-pinned to its repo's `staging` branch. The
  bucket/distribution/role IDs are non-secret and live as `env:` in the workflow.

### Frontends — on AWS S3 + CloudFront (direct-child hostnames)

The staging frontends are hosted on **AWS S3 + CloudFront** (`frontend.tf`), one stack per UI:

- **Private S3 bucket** (all public access blocked) reached only by CloudFront via **Origin Access
  Control** (OAC) — a per-distribution bucket policy scopes `s3:GetObject` to that distribution's ARN.
- **CloudFront** with **SPA-fallback** custom error responses (`403/404 → /index.html`, `200`) so
  client-side deep links resolve (the AWS equivalent of Vercel's SPA rewrite); `PriceClass_100`
  (NA+EU edges); the AWS-managed *CachingOptimized* policy.
- **us-east-1 ACM cert** per host, DNS-validated, attached as the alias cert (phase 2).
- Hostnames are **direct children** of `energyexe.com` — `staging-dashboard.energyexe.com` (admin-ui)
  and `staging-app.energyexe.com` (client-ui). **This is the crux:** an earlier attempt used
  `staging.dashboard.*` / `staging.app.*`, which sit *under* the Vercel-CNAME'd `dashboard` / `app`
  labels and inherited Vercel's Amazon-excluding CAA → ACM refused (see §6). Direct children have no
  Vercel label in their path, so ACM issues cleanly (proven first by `staging-api`).

The backend's CORS (`cors_origins`) + `admin_portal_url` / `client_portal_url` are set to these two
hostnames. **Prod frontends stay on Vercel** (`dashboard.*` / `app.*`) — only staging moved to AWS,
as the pilot for eventually moving prod too.

### Cost

≈ **$55–65/mo**, dominated by the snapshot-restored RDS (the ~200 GB storage floor). On-demand
Fargate ≈ $18/mo. The two frontend stacks (S3 + CloudFront, low traffic) add ≈ **$2–5/mo**.
**Shared ALB + cluster + skipped Valkey = ~$0 added.** Levers if needed: stop the RDS when idle
(storage-only), or switch to an empty seeded DB (~$15/mo total).

---

## 6. Gotchas & lessons learned

- **`terraform -target` is not isolated** if the target *references* a resource with a pending
  change — it pulls dependencies in. (Caused an unintended prod redeploy in the prod root; the whole
  separate-root design avoids it for staging.)
- **EC2 security-group descriptions must be ASCII** — an em-dash (`—`) in a description failed the
  `CreateSecurityGroup` call.
- **ECS won't attach a service to a target group with no LB association** — create the listener rule
  before/with the service, not in a later phase.
- **CloudFront ACM certs must be in `us-east-1`** (needs a provider alias).
- **ELB/target-group names cap at 32 chars** — the staging TG is `energyexe-staging-tg`, not
  `${local.name}-tg`.
- **ACM `CAA_ERROR` from following a CNAME (and the fix):** `dashboard.energyexe.com` /
  `app.energyexe.com` are **CNAMEs to Vercel**, and Vercel's CAA authorizes only Let's Encrypt /
  Sectigo / Google / GlobalSign — **not Amazon**. ACM's CAA tree-walk for `staging.dashboard` /
  `staging.app` follows that CNAME and is refused. `staging-api` (a *direct* child of `energyexe.com`,
  no intermediate CNAME) issued fine. **Resolution:** the frontends use **direct-child hostnames**
  `staging-dashboard.energyexe.com` / `staging-app.energyexe.com` — no Vercel label in the path, so
  ACM issues normally. (An alternative would be a leaf CAA record `0 issue "amazon.com"`, but
  direct-child is cleaner and needs no extra DNS.) Lesson: when issuing an ACM cert for a name under
  a label that CNAMEs elsewhere, the *other* domain's CAA governs — pick a hostname whose path you
  control.
- **A killed `terraform apply` can drop resources from local state** — an interrupted run dropped
  `aws_db_instance.staging`. Fixed with `terraform import` + making the config match reality
  (`storage_encrypted = true`, `lifecycle { ignore_changes = [snapshot_identifier] }`) so it didn't
  try a destructive replace.
- **`workflow_dispatch` requires the workflow on the default branch**; and `paths-ignore` would skip
  an infra-only bootstrap commit — both reasons the staging workflow drops `paths-ignore`.
- **The shell is zsh** — unquoted `$VAR` does *not* word-split, so bundling CLI flags in a variable
  (`PROF="--profile … --region …"; aws $PROF`) passes them as one bad argument. Inline the flags.

---

## 7. Runbooks

**Deploy to staging:** push to the `staging` branch → `deploy-staging.yml` builds + deploys
automatically. Watch: `gh run list --repo Energy-Exe/energyexe-core-backend --workflow=deploy-staging.yml`.

**Apply infra changes (staging):**
```bash
cd energyexe-core-backend/infra/staging
terraform plan        # GATE: confirm 0 changes/0 destroys to any existing/prod resource
terraform apply
```

**Health checks:**
```bash
curl https://staging-api.energyexe.com/health         # public (once DNS propagates)
# pre-DNS / internal, through the shared ALB:
curl -k -H "Host: staging-api.energyexe.com" https://<prod-alb-dns>/health
```

**Refresh staging data from prod:** create a new snapshot, set `rds_snapshot_identifier`, and
restore a new instance (don't rely on in-place replace — `snapshot_identifier` is ignored).

**Tear down staging:** `terraform destroy` in `infra/staging/` (separate state ⇒ prod is untouched).

**Deploy a staging frontend:** push to the `staging` branch of `faisal-energyexe/energyexe-admin-ui`
or `…-client-ui` → its `deploy-staging.yml` builds + syncs to S3 + invalidates CloudFront. Watch:
`gh run list --repo faisal-energyexe/energyexe-admin-ui --branch staging`.

**Add a new frontend host on AWS:** add the cert ARN var, apply phase 1 (S3 + CloudFront + cert),
add the validation CNAME at hyp.net, set the cert ARN var + re-apply (phase 2 attaches the alias),
then add the host CNAME → CloudFront domain. Use a **direct-child** hostname (see §6 CAA).

**Set the backend CORS origin:** set `cors_origins` (JSON-array string) + optional `*_portal_url` in
`terraform.tfvars`, then `terraform apply` (replaces the task def → brief staging redeploy).

---

## 8. Current state & remaining work

**Live & verified (2026-06-28):**
- Staging backend healthy at `staging-api.energyexe.com` (valid TLS; runs the latest `staging` branch
  image). Prod unaffected throughout (health OK, ALB rules unchanged, task-def rev 8).
- **Staging frontends LIVE on AWS:** `https://staging-dashboard.energyexe.com` (admin-ui) +
  `https://staging-app.energyexe.com` (client-ui) — both serve over valid TLS, SPA deep-links fall
  back correctly, and the bundles call `staging-api.energyexe.com` (CORS verified). Deployed by each
  repo's `deploy-staging.yml` on push to `staging`.
- Staging-first CI committed and proven for all three repos (backend + both frontends).
- EPR-48 consolidated on the `staging` branches: client-ui **#192** (off `main`), backend **#132**
  (off `master`).

**Remaining:**
- **Phase 2 — built, pending activation:** the `master → prod` promotion workflow is implemented
  (PR #134) and the staging-side ECR pull grant is applied. Activate by merging PR #134, then run the
  first promotion via `workflow_dispatch` (deliberate + monitored) — it ships the current staging
  contents to prod.
- `infra/staging/**` + the backend `deploy-staging.yml` currently live only on the `staging` branch;
  they reach `master` when `staging → master` is first promoted.
- Optional: a post-restore PII scrub for the staging DB; extending the AWS-hosting pilot to the
  **prod** frontends (move them off Vercel).
- Future cleanup: extract a shared backend-service module so prod and staging stop duplicating.
