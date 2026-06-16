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

- **`desired_count = 1`, deploys stop-then-start** — APScheduler runs inside the
  app process; two tasks = every cron fires twice. Accepts ~1–2 min downtime per
  deploy. If horizontal scaling is ever needed, move scheduling to EventBridge →
  the existing `/import-jobs/trigger/...` endpoints first.
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
