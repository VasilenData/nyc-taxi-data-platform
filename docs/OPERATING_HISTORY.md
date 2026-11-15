# Dated operating evidence for a historical replay

**Status on 14 November 2026: implemented and exercised locally. The scheduled
workflows activate from the default branch for the window below.** No multiweek service
history, outbound email delivery, persistent cloud warehouse or live taxi feed is
claimed. AWS remains offline-only.

## First scheduled attempt: delayed launch and unavailable metadata

The [15 November scheduled run](https://github.com/VasilenData/nyc-taxi-data-platform/actions/runs/43706325459)
used commit `6235ffc7d09526be76c42b612d60ac5be762498d`. GitHub created and
started it at **10:54:28 UTC**, approximately **7 hours 37 minutes after the
03:17 UTC due time**, already beyond the three-hour freshness allowance.
Its eligibility step then failed when GitHub's own run-metadata response hit a
10-second read timeout. Ingestion, dbt, publication, independent checks and
artifact retention were all skipped. This records a failed scheduled attempt,
not pipeline execution, consumer-data corruption or a successful recovery.

The metadata helper now permits at most **three attempts**, uses request
timeouts of at most **eight seconds**, and applies a **30-second retry budget**
to requests and backoff. Timeout/temporary connection failures and selected
temporary HTTP responses can retry; permanent authentication, validation,
malformed JSON and oversized responses fail immediately. Retry-After and
rate-limit reset hints are honored in full. If the required wait cannot fit
the remaining budget, the helper fails rather than retrying early or waiting
indefinitely. A late response is rejected. Socket timeouts are not an OS-level
hard process deadline; the workflow timeout remains the outer execution bound.
The policy follows [GitHub's rate-limit guidance](https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api#handle-rate-limit-errors-appropriately).

Deterministic transport/clock tests cover recovery, exhaustion, permanent
errors, rate-limit hints and late responses; they are synthetic regressions,
not additional successful scheduled runs. The due-slot identity, three-hour
freshness rule and coverage checks remain unchanged. The failed/delayed attempt
above is retained even if a later retry succeeds.

## Supervisor correction from hosted CI

Hosted Linux CI exposed a timeout-reporting defect: `psutil.wait_procs` reaped
the direct child before `subprocess.Popen` could collect its exit status, allowing
a killed process to be reported with exit code zero. The supervisor now gives
`Popen.wait` sole ownership of the direct child's status and uses psutil to confirm
the descendants stopped. The existing timeout and descendant-writer regressions
cover this boundary. The dated local record below remains unchanged; hosted run
artifacts retain their own source fingerprints.

The operating exercise replays the bundled **30,000 January–February 2023 input
rows**, using the actual loader, dbt models/tests and atomic publisher. Freshness
means a complete replay published by its deadline; it does not mean recent taxi
events. The existing 31.6-million-row performance evidence is unchanged.

## What was actually observed

[Dated acceptance record](operations/2026-11-14/evidence.json) ·
[Complete recorded history, logs and notices](operations/2026-11-14/history.zip)

One local exercise completed two planned slots through three real attempts:

The recorded interval was **14 November 2026, 15:44:37–15:45:18 UTC**. The separate
checker completed its failed-publication inspection 1.261 seconds after the
failure record; the verified local inbox write followed at 15:45:04.675 UTC.
Recovery published at 15:45:17.846 UTC and its local notice was verified at
15:45:18.954 UTC. These are actual exercise observations, not a promised service
response time.

| Attempt | Observed outcome |
|---|---|
| Baseline | Loaded 30,000 rows and published 29,326 valid fact rows. |
| Controlled incident | Set the candidate hourly mart's trip counts to zero. Publication refused the fact/mart mismatch; all eight previous consumer tables and their version record were preserved. |
| Recovery | Reloaded the same inputs, rebuilt dbt models/tests and published a complete version. A separate process opened the database read-only and checked every consumer relation. |

All ten acceptance checks passed. The checker wrote one incident to the local
inbox, verified its bytes and retained a delivery receipt. Repeated detection
reused that receipt. Recovery produced one notice referencing the exact prior
alert ID; later healthy checks do not manufacture additional recoveries.

The JSON record contains the actual UTC attempt times and measured local
detection interval. The checker was deliberately invoked immediately after the
controlled failure: this interval is **not a measured scheduler or email SLA**.
The ZIP retains all 35 recorded files, including phase logs, source-input hashes,
attempt requests, completed/failed phase records, independent check reports,
inbox notices and delivery receipts. `evidence.json` hashes every other archive
member. Source hashes normalize CRLF to LF; they identify the actual files used,
even though the parent Git commit precedes these local changes.

## What the checker considers complete

Each planned slot exists independently of its attempts. An attempt records its
start before invoking workers, then appends phase and terminal records. Reusing
an attempt ID returns its immutable result; a changed request is refused. A
started attempt without a terminal record stays visible as overdue.

| Contract | Default budget / observation |
|---|---|
| Ingestion completion | 60 seconds from attempt start. |
| Complete publication | 360 seconds from attempt start. |
| Scheduled replay freshness | Three hours after the committed UTC due time. |
| Child execution bounds | Ingestion 60 s, dbt 240 s, publication 60 s; termination confirmation may take a further 10 s. |
| Retained console output | First 65,536 bytes per phase; excess output is drained and counted, not retained. |

The local report distinguishes **latest-attempt completion** from **ever having a
successful attempt**. A new failed attempt cannot hide behind an earlier success.
The latest successful warehouse snapshot remains the expected consumer version
while a candidate fails. Snapshot verification hashes complete sorted row
multisets, including duplicates and column types, across all nine relations; it
rejects relations above 50,000 rows before fetching them.

Failure states distinguish absent runs, ingestion/publication overruns, failed
ingestion, failed dbt builds, refused publication, worker timeouts, late results,
unavailable checks and changed consumer outputs. A failed post-timeout snapshot
is recorded as unknown preservation, not a successful recovery claim. Worker
termination freezes and kills the trusted process tree, including Windows
launchers; a regression observes a descendant writing before termination and
proves it cannot continue afterward. This is not a hostile-process containment
mechanism or a power-loss guarantee.

Immutable JSON uses complete temporary files and exclusive hard-link publication
on a local filesystem. This needs local hard-link support and the documented
single-writer workflow. Hashes and refusal to overwrite protect reviewability;
they are not an external signature or independent human review.

## The prepared schedule and its boundaries

[Replay workflow](../.github/workflows/operating-replay.yml) ·
[Separate schedule monitor](../.github/workflows/operating-monitor.yml) ·
[Committed operating window](../operations/replay-window.json)

The prepared window is **15 November–14 December 2026**, one replay daily at
03:17 UTC. A separate monitor runs at 06:47 UTC, with a final check on 15 December.
The runtime date guard prevents the annual cron expressions from silently
reactivating the replay in a later year. Manual dispatch is an explicit manual
exercise and does not count as a scheduled success.

Expected slots come from committed configuration, so a missing run is detectable
even if the pipeline never starts and writes no local plan. The monitor reads
GitHub's scheduled-run history and actual job completion timestamps. It requires
the replay, independent checker and artifact-retention steps to have completed
successfully; an otherwise green wrapper with skipped work does not count.
Truncated API history, absent job evidence and unavailable responses do not turn
unknown coverage into healthy results.

Both the replay plan and monitor attribute runs to the same UTC window using
the workflow's recorded creation time, not the worker's potentially later start
date. This is a **creation-time window audit**, not proof that GitHub delivered
every nominal cron event. Arbitrarily delayed event creation can remain
ambiguous. More than one run in a window is reported for review.

Every daily replay starts with a fresh local database. Its complete attempt
records are uploaded under a unique run/attempt artifact name; 45-day retention
is temporary storage, not a permanent archive. The monitor reconstructs the
window from configuration plus GitHub history and retains its own dated report
and local inbox. It checks workflow/step evidence, not downloaded archive
contents or a separately hosted warehouse.

The two workflows share GitHub's scheduler and control plane. GitHub documents
that scheduled events can be delayed or dropped under load, and schedules run
from the default branch. A separate workflow does not establish an independent
cloud monitoring service. [GitHub schedule behavior](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule),
[workflow-history API](https://docs.github.com/en/rest/actions/workflow-runs#list-workflow-runs-for-a-repository).

## Activation and reproduction

Before activation, publish and verify the code on the default branch. If the
planned start has already passed, choose a **new prospective window** and update
both `operations/replay-window.json` **and both workflows' cron day/month ranges**
together. JSON changes alone do not schedule newly added days. Do not fabricate
earlier run records or label never-enabled dates as observed service outages.
Then verify one manual dispatch and observe subsequent scheduled runs as they
actually occur. Archive the full history before temporary artifacts expire;
disable the scheduled workflows when the bounded window is complete.

To repeat the local exercise, use fresh output paths:

```bash
python -m pip install -r requirements-dev.txt
python scripts/operating_replay.py drill \
  --history /tmp/nyc-operating-exercise --database /tmp/nyc-operating-exercise.duckdb
python scripts/operating_replay.py check \
  --history /tmp/nyc-operating-exercise --database /tmp/nyc-operating-exercise.duckdb \
  --check-id another-independent-inspection
python -m pytest -q tests/test_operations.py tests/test_replay_schedule.py
```

Use Windows-local paths in PowerShell. The replay forces the bundled files and
fails rather than downloading substitute data. The ordinary CI job also executes
the drill and retains its actual records separately from scheduled history.

**The active alert channel is a local JSON inbox and audit receipt.** No sending
credentials are configured, no external message was sent, and no email adapter
is active. A user-configured notification integration can consume these events;
recipient delivery would need its own actual end-to-end test.

## Engineering prompt used

> Preserve the existing publication contract. Exercise a real, bounded historical
> replay; commit expected schedules independently of workers. Retain actual UTC
> timestamps, source identities, complete/failed attempts and bounded logs.
> Detect a refused publication in a separate read-only process, deliver a local
> incident with a verifiable receipt, and link its recovery without duplicate
> notices. Test a missing run, interrupted stages, descendant termination and
> skipped scheduled work. Record only elapsed operation that really occurred;
> distinguish configured scheduling from activation and shared infrastructure
> from independent monitoring.

Implementation, tests and the exercise are AI-assisted. Several weeks of actual
scheduled results, human operation and external notification delivery still
require future observation; this dated cycle does not establish those facts.
