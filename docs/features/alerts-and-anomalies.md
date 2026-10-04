# Alerts and data anomalies

Two unrelated features that are easy to confuse:

- **Alerts** (`/api/v1/alerts`) — user-defined alert *rules*, their *triggers*, in-app
  *notifications* and per-user notification *preferences*. Portal-facing.
- **Data anomalies** (`/api/v1/data-anomalies`) — detected data-quality problems in the
  generation data (gaps, spikes, impossible capacity factors). Admin tooling.

Neither is the pipeline's *performance* anomaly detection (Module 3, `performance_anomalies`,
[`docs/pipeline/module-3-anomaly-detection.md`](../pipeline/module-3-anomaly-detection.md)) nor
opportunity detection ([`opportunity-detection.md`](opportunity-detection.md)).

## Alerts

Code: [`app/api/v1/endpoints/alerts.py`](../../app/api/v1/endpoints/alerts.py),
[`app/services/alert_service.py`](../../app/services/alert_service.py),
[`app/models/alert.py`](../../app/models/alert.py). All routes use `get_current_user` and are
scoped to the caller (`user_id`).

| Table | Model | Routes |
|---|---|---|
| `alert_rules` | `AlertRule` — `metric` (`capacity_factor`, `generation`, `price`, `capture_rate`, `wind_speed`, `data_quality`), `condition` (`above`, `below`, `change_by_percent`, `outside_range`), `scope` (`specific_windfarm`, `portfolio`, `all_windfarms`), `severity`, channels (`in_app`, `email`, `email_digest`) | `GET/POST /rules`, `GET/PUT/DELETE /rules/{id}`, `POST /rules/{id}/toggle` |
| `alert_triggers` | `AlertTrigger` (`AlertTriggerStatus`) | `GET /triggers`, `PATCH /triggers/{id}/status`, `POST /triggers/{id}/acknowledge`, `/resolve` |
| `notifications` | `Notification` (`NotificationStatus`) | `GET /notifications`, `POST /notifications/mark-read`, `/mark-all-read`, `DELETE /notifications/{id}`, `GET /notifications/unread-count` |
| `notification_preferences` | `NotificationPreference` | `GET/PUT /preferences` |
| — | summaries | `GET /summary`, `GET /overview` |

**What is not there:** `AlertService.create_trigger` / `create_notification` exist, but nothing in
`app/` or `scripts/` calls them — there is **no background evaluator** that compares metrics
against rules and fires triggers, and no scheduled job for it
([`scheduled-jobs.md`](../operations/scheduled-jobs.md) lists everything that runs). Rules can be
created and listed; triggers and notifications only appear if something writes them. Treat the
feature as "rules UI shipped, evaluation engine not built" when planning work on it.

## Data anomalies

Code: [`app/api/v1/endpoints/data_anomalies.py`](../../app/api/v1/endpoints/data_anomalies.py),
[`app/services/data_anomaly_service.py`](../../app/services/data_anomaly_service.py),
[`app/models/data_anomaly.py`](../../app/models/data_anomaly.py) (`data_anomalies`). Routes use
`get_current_active_user`; the list route takes an optional `portfolio_id` filter (resolved through
`PortfolioService` for the caller).

`POST /data-anomalies/detect` runs `DataAnomalyService.detect_anomalies` on demand (it is **not**
scheduled): `_detect_capacity_factor_anomalies` (CF over the physical limit), `_detect_data_gap_anomalies`
(missing hours, severity by gap length — `_gap_severity`), `_detect_data_spike_anomalies`. Rows carry
`anomaly_type` (`capacity_factor_over_limit`, `negative_generation`, `missing_data`, `data_spike`,
`data_gap`, `invalid_capacity`, `gen_consumption_swapped`, plus the windfarm-level
`missing_generation_data` written by the DQ-01 opportunity detector), `status` (`pending`,
`investigating`, `resolved`, `ignored`, `false_positive`) and `severity`.

Other routes: `GET ""` (filters), `GET /{id}`, `PATCH /{id}` and `PATCH /{id}/status`,
`DELETE /{id}` (soft), and the repair path `POST /{id}/reaggregate` / `POST /reaggregate` →
`reaggregate_period`, which re-runs the existing daily aggregation processor for the given
sources / window (optionally one windfarm). Known
fix patterns are recorded in [`docs/history/ENERGISTYRELSEN_DOUBLE_COUNTING_FIX.md`](../history/ENERGISTYRELSEN_DOUBLE_COUNTING_FIX.md)
and [`docs/history/ELEXON_BST_FIX_LOG.md`](../history/ELEXON_BST_FIX_LOG.md).

Tests: `tests/test_anomalies_api.py` (needs a live server on :8001; skips/fails without one).
