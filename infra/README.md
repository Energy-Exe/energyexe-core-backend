# AWS Fargate deployment (Terraform)

Deploys the FastAPI backend as a **single always-on Fargate task** behind an ALB,
in the same default VPC as the `energyexedb` RDS instance (`eu-north-1`).

```
Internet ──► ALB (:80/:443) ──► Fargate task (uvicorn :8001, 1 worker,
                                  APScheduler in-process, desired_count = 1)
                                        │ 5432                │ 6379 (TLS)
                                        ▼                     ▼
                                  RDS energyexedb       ElastiCache Serverless
                                  (existing, untouched  (Valkey, replaces the
                                  except one added      Railway instance)
                                  ingress rule)
```

Key design decisions (see inline comments for detail):

- **`desired_count = 1`, deploys stop-then-start** — now historical. It existed
  because the nightly pipeline ran on an in-process APScheduler and two tasks
  would have fired it twice. That scheduler is gone (see below), so this and the
  `--workers 1` override can be revisited; both are left in place deliberately
  rather than changing API concurrency in the same step as the pipeline move.
- **One uvicorn worker** (task def overrides the Dockerfile's `--workers 4`) —
  each worker runs the lifespan and would start its own scheduler.
- **`alembic upgrade head` runs in the container command** before uvicorn starts.
- **Public subnets + public IP, no NAT gateway** (saves ~$35/mo) — inbound is
  still ALB-only via security groups; the public IP is for ECR pulls and the
  external data APIs (ENTSOE/Elexon/EIA/...).
- **Secrets in Secrets Manager**, injected by ECS; values never enter TF state.

- **Valkey on ElastiCache Serverless** — used only for the windfarm report
  cache (1h TTL, graceful no-cache fallback). `REDIS_URL` is injected as a
  plain env var (`rediss://`, no password): the endpoint is VPC-only and
  SG-gated, so there's nothing secret in it.

Estimated cost: ~$45–50/mo for the task (1 vCPU / 4 GB) + ~$20/mo ALB
+ ~$6–7/mo Valkey ≈ **$75/mo total**.

## Scheduled jobs live here, not in GitHub Actions

`scheduled_imports.tf` owns every recurring data import: an EventBridge rule per
job → one Lambda → `POST /api/v1/import-jobs/trigger/{job_name}`, with retries
and an SQS DLQ that alarms. Adding a job means adding one entry to
`local.import_schedules`; rule, target and Lambda permission fan out from it.

This replaced GitHub Actions `schedule:` on 2026-08-17. **Do not add a
`schedule:` block to a workflow** — GitHub's scheduler is best-effort and was
dropping 15–60% of the hourly Taipower runs and delaying every daily import by
21–80 minutes. Rationale, measurements and the API-Destination footgun are in
the header comment of `scheduled_imports.tf` and in `.github/workflows/README.md`.

The nightly performance pipeline is here too, in `pipeline_daily.tf`, but as a
**one-off ECS task** rather than an HTTP trigger — it runs ~3 hours, far past any
request timeout. EventBridge → `ecs:RunTask` → `scripts/jobs/run_pipeline_daily.py`,
which exits 0/1/2 (ok / batch failed / detection failed). A non-zero exit *or* an
abnormal stop (OOM, image-pull failure, capacity error) alarms to SNS.

Create the task definition with `pipeline_schedule_enabled = false`, smoke-test it
by hand with `terraform output pipeline_run_task_command` plus
`--overrides` carrying `--windfarm-ids`, then flip the schedule on.

## Bootstrap (first deploy)

```bash
brew install terraform        # if not installed
cd infra
terraform init
terraform plan                # review everything before creating anything
terraform apply

# Set secret values (see secrets.tf header for exact commands).
# Reuse the Railway SECRET_KEY to keep existing JWTs valid.

# Build and push the first image (Apple Silicon needs --platform):
aws ecr get-login-password --profile energyexe --region eu-north-1 \
  | docker login --username AWS --password-stdin "$(terraform output -raw ecr_repository_url | cut -d/ -f1)"
docker build --target production --platform linux/amd64 \
  -t "$(terraform output -raw ecr_repository_url):latest" ..
docker push "$(terraform output -raw ecr_repository_url):latest"

# The service was crash-looping waiting for the image; kick it:
aws ecs update-service --cluster energyexe --service energyexe-core-backend \
  --force-new-deployment --profile energyexe

# Verify:
curl "$(terraform output -raw api_url)/health"
aws logs tail /ecs/energyexe-core-backend --follow --profile energyexe
```

## Cutover from Railway

1. Burn-in: leave Railway serving traffic, watch the Fargate task's nightly
   pipeline + import jobs in CloudWatch for a couple of days.
   ⚠️ While BOTH are up, the daily crons run twice (once per platform) — keep
   the burn-in short, or pause Railway's scheduler.
2. Add an ACM cert (DNS validation at your DNS host), set `certificate_arn`
   in terraform.tfvars, re-apply, CNAME your api domain to the ALB.
3. Point the frontends' API base URL at the new domain; set `cors_origins`.
4. Flip the nightly pipeline to AWS: set `pipeline_daily_enabled = true` in
   terraform.tfvars and apply (and disable `PIPELINE_DAILY_ENABLED` on Railway
   if shutting it down later rather than immediately). If the brain agent's
   frontend-repo access is used in prod, also provide `GITHUB_TOKEN` (add a
   secret + task-def entry like the existing two).
5. Shut down Railway — both the app service and the Valkey instance (the
   ECS task's `REDIS_URL` already points at ElastiCache, and losing the
   Railway cache contents is harmless: 1h TTL, repopulates on demand).
   Remember to delete the stale `REDIS_URL`/`VALKEY_*` vars wherever they
   linger (local `.env` keeps working — it points at docker-compose redis
   or the Railway proxy until then).
6. Optional hardening afterwards (ASK FIRST — laptop scripts depend on public
   RDS access): narrowing the RDS SG's 0.0.0.0/0 rule to specific IPs + the
   service SG keeps laptop access while closing the internet-wide hole.

## Monitoring & error tracking

Two layers, both defined here (see `glitchtip.tf` + `monitoring.tf`):

- **App errors → self-hosted GlitchTip** (`errors.energyexe.com`): a second
  Fargate service in the same cluster, behind the same ALB via host-routing.
  The backend reports exceptions to it with the Sentry SDK
  (`app/core/observability.py`); set `SENTRY_DSN` to enable. GlitchTip gives
  grouped issues, full stack traces, and its own email alerts.
- **Infra signals → CloudWatch alarms → SNS email**: service-down (no healthy
  targets), backend 5xx, ALB 5xx, and a GlitchTip-down alarm. These catch what
  an in-process SDK can't (the task being dead / OOM-killed).

Added cost ≈ **$20–40/mo** (one small Fargate task; ALB rule/cert + SNS are
effectively free; Postgres rides the existing RDS).

### Enable CloudWatch email alarms

```bash
# In terraform.tfvars:
#   alert_email = "you@energyexe.com"
terraform apply        # creates the SNS topic + subscription + alarms
# AWS emails a confirmation link — CLICK IT, or no alarms are delivered.
```

The `*-no-healthy-hosts` alarm requires 5 consecutive minutes with zero healthy
targets before firing, so a normal ~1–2 min deploy won't false-trip it. Validate
any alarm without real downtime via the console ("Set alarm state" → ALARM).

### Stand up GlitchTip (two-phase, like the API cert)

**Phase 1 — cert + prep** (`glitchtip_domain` set, `glitchtip_certificate_arn` empty):

```bash
# terraform.tfvars:  glitchtip_domain = "errors.energyexe.com"
terraform apply
terraform output glitchtip_acm_validation_records   # add this CNAME at hyp.net
# wait for the cert to reach ISSUED (aws acm describe-certificate ...)
```

While the cert validates, create the GlitchTip database on the existing RDS and
populate its secrets:

```sql
-- psql to energyexedb as the admin user:
CREATE DATABASE glitchtip;
CREATE ROLE glitchtip LOGIN PASSWORD '<choose-a-strong-password>';
GRANT ALL PRIVILEGES ON DATABASE glitchtip TO glitchtip;
```

```bash
aws secretsmanager put-secret-value --profile energyexe \
  --secret-id energyexe/glitchtip/secret-key --secret-string "$(openssl rand -hex 32)"
# NOTE the postgres:// scheme (GlitchTip is Django/psycopg, not asyncpg):
aws secretsmanager put-secret-value --profile energyexe \
  --secret-id energyexe/glitchtip/database-url \
  --secret-string 'postgres://glitchtip:<password>@energyexedb.<id>.eu-north-1.rds.amazonaws.com:5432/glitchtip'
# Reuse Resend over SMTP for GlitchTip's own alert emails:
aws secretsmanager put-secret-value --profile energyexe \
  --secret-id energyexe/glitchtip/email-url \
  --secret-string 'smtp://resend:<RESEND_API_KEY>@smtp.resend.com:587'
```

**Images — no Docker Hub account needed.** `redis` is pulled from the ECR Public
Gallery (`public.ecr.aws/docker/library/redis`, unlimited/no-auth to any AWS
region). GlitchTip is Docker-Hub-only, so phase 1 creates a private ECR repo and
you **mirror the image into it once** (and again on each upgrade) — one anonymous
Docker Hub pull is well under any rate limit:

```bash
TAG=6.1.8   # pin a real release: https://gitlab.com/glitchtip/glitchtip/-/releases
REPO=$(terraform output -raw glitchtip_ecr_repository_url)
docker pull glitchtip/glitchtip:$TAG
docker tag  glitchtip/glitchtip:$TAG "$REPO:$TAG"
aws ecr get-login-password --profile energyexe --region eu-north-1 \
  | docker login --username AWS --password-stdin "${REPO%%/*}"
docker push "$REPO:$TAG"
```

**Phase 2 — serve** (set `glitchtip_certificate_arn` to the issued ARN; set
`glitchtip_image_tag` to the tag you mirrored):

```bash
# terraform.tfvars:
#   glitchtip_certificate_arn = "<glitchtip_acm_certificate_arn output>"
#   glitchtip_image_tag       = "6.1.8"
terraform apply        # creates the target group, listener rule, task def, service
terraform output glitchtip_cname_target   # CNAME errors.energyexe.com here at hyp.net
```

Then create the first user/org/project in the GlitchTip UI (open registration is
off — use the brief first-run window or the GlitchTip shell), and copy the
**DSN**.

### Point the backend at GlitchTip

```bash
aws secretsmanager put-secret-value --profile energyexe \
  --secret-id energyexe/core-backend/sentry-dsn \
  --secret-string 'https://<public_key>@errors.energyexe.com/<project_id>'
# terraform.tfvars:  backend_sentry_dsn_enabled = true
terraform apply        # redeploys the backend with SENTRY_DSN wired in
```

Keep `backend_sentry_dsn_enabled = false` until the secret holds a real DSN —
an empty secret would stop the (live) backend from starting. To verify: hit an
endpoint that raises and confirm the issue appears in GlitchTip with a
`request_id` tag.

## Day-2 operations

- **Deploy**: automatic on push to `master` via `.github/workflows/deploy-aws.yml`
  (builds, pushes to ECR, forces an ECS redeploy). It authenticates with GitHub
  OIDC assuming the `energyexe-core-backend-github-deploy` role (infra/cicd.tf) —
  no stored AWS keys. Skips docs-only / `infra/**` changes. You can also trigger
  it manually (Actions → Run workflow) or run the build/push/redeploy by hand.
- **Logs**: `aws logs tail /ecs/energyexe-core-backend --follow --profile energyexe`
- **Shell into the task** (debugging): enable ECS Exec later if needed, or run a
  one-off task with the same task def.
- **Resize**: bump `task_cpu` / `task_memory` in terraform.tfvars, `terraform apply`.

## RDS network access (changed 2026-07-15)

**Prod `energyexedb` is PRIVATE; staging `energyexedb-staging` is PUBLIC**
(user decision — laptop/dev work targets staging directly; prod DB access
is a deliberate break-glass operation).

The prod instance and its SG (`sg-08ce9488ba4aa1fde`, the VPC default SG)
are **not Terraform-managed** — the flip was applied via CLI and is
recorded here:

```bash
# revoked the three public 5432 ingress rules (backup: infra/backups/sg-08ce…json)
aws ec2 revoke-security-group-ingress --group-id sg-08ce9488ba4aa1fde --protocol tcp --port 5432 --cidr 0.0.0.0/0
aws ec2 revoke-security-group-ingress --group-id sg-08ce9488ba4aa1fde --protocol tcp --port 5432 --cidr 103.218.26.197/32
aws ec2 revoke-security-group-ingress --group-id sg-08ce9488ba4aa1fde --protocol tcp --port 5432 --cidr 103.218.24.249/32
aws rds modify-db-instance --db-instance-identifier energyexedb --no-publicly-accessible --apply-immediately
```

Remaining prod 5432 ingress is SG-to-SG only: backend service, GlitchTip,
the scada-pipeline task, and the SSM bastion (`rds_from_bastion` in
network.tf, gated by `bastion_security_group_id` in tfvars). Both master
passwords were rotated the same day (secrets updated + services redeployed).

Break-glass prod access: `energyexe-scada-pipeline/scripts/prod_tunnel.sh`
(SSM port-forward via the stopped-by-default bastion; localhost:25432).
Emergency rollback to public: `aws rds modify-db-instance
--db-instance-identifier energyexedb --publicly-accessible
--apply-immediately` + re-authorize rules from the backup JSON.

Future hardening option (not done): `terraform import` the prod instance
so `publicly_accessible` becomes declarative.
