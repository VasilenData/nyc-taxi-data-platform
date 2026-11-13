# Two-query AWS experiment and operating record

**Status: prepared and tested offline; not executed on AWS.** The account, authenticated operator profile, region and approved numeric total budget have not been supplied. Nothing here establishes deployed resources, effective IAM denials, cloud costs, uptime or human review. The [implementation](../taxi_platform/cloud_experiment.py) closes a specific tooling gap: configuration acceptance can now lead to a bounded, journaled result comparison instead of an unrecorded console query.

## Exact scope to review

| Boundary | Fixed experiment |
| --- | --- |
| Environment | One `dev` stack, one account and region, existing scoped publisher/query roles |
| Source | One trusted historical TLC serving bundle, at most 16 MiB including its manifest |
| Queries | Exactly two SQL statements: row count and sum of `trips` for each release-specific daily/hourly aggregate |
| Oracle | Local verified Parquet; both aggregate trip totals must equal the fact row count before a plan is created |
| Scan control | Enforced Athena cutoff no higher than 100 MiB per query; result reuse disabled |
| Permission exercise | Publisher confirms the fact object's size/SHA-256, then query role attempts `GetObject Range bytes=0-0` |
| Time | Plan has an explicit UTC expiry; each submitted query is polled for at most 120 seconds plus bounded SDK call time |
| Mutations | Athena writes private result objects; no source-data, IAM or infrastructure changes by this runner |
| Stop behavior | Preflight, denial, query or reconciliation failure stops further submission; best-effort cancellation for an active known query ID |
| Evidence | Plan hash, exact SQL, expected/actual totals, query IDs, terminal/provisional usage and cancellation status in a local journal |

**The declared budget is not a spending hard cap.** The query cutoff applies to each execution, not all experiments or the account. Cancelled queries may consume scan bytes; S3 requests/storage, Glue and retained versions are separate costs. A new plan identity can create two more chargeable queries. The harness does not read an invoice or automatically shut down the account. Use one operator/process and one reviewed plan; do not treat repeated plan creation as a budget reset.

The one-byte probe fails if a read unexpectedly succeeds and closes the response body without retaining its content. Only explicit `AccessDenied` with HTTP 403 counts as an observed denial. A 404 or another error is inconclusive. The preceding publisher HEAD prevents treating an unverified missing key as an isolation proof; concurrent deletion between those requests is still a limitation. This tests one read boundary, not every IAM permission.

## Prepare and inspect without AWS calls

1. Run the existing sample pipeline and export/verify its serving bundle using the commands in [Cloud controls](CLOUD_CONTROLS.md#prepared-deployment-sequence). Keep the original manifest in a trusted local location. The historical sample is sufficient for this experiment; do not upload the full 31.6-million-row warehouse.
2. Complete the actual account, region, operator and spending review. Deploy/upload only through the existing documented, explicitly authorized sequence. Obtain the two role ARNs, bucket names, database and workgroup from that stack. No deployment has occurred in this repository's evidence.
3. Save the following **incomplete template** as `.cloud-work/experiment-settings.json`, replacing every `null` with actual reviewed values. It contains no credentials and cannot execute as supplied. `budget_usd` must be a positive decimal string; a zero budget means remain offline.

```json
{
  "account_id": null,
  "region": null,
  "data_bucket": null,
  "results_bucket": null,
  "database": null,
  "workgroup": null,
  "publisher_role_arn": null,
  "query_role_arn": null,
  "budget_usd": null,
  "expires_at_utc": null,
  "experiment_id": null
}
```

4. Create a plan locally. This verifies the bundle, computes both answers independently from local Parquet and records the exact SQL/limits. **Plan creation makes zero AWS calls.** Review the output file and SHA-256 before execution; a checksum does not itself constitute permission.

```bash
python -m taxi_platform.cloud_experiment plan .cloud-work/release \
  --settings .cloud-work/experiment-settings.json --plan .cloud-work/experiment-plan.json
```

No AWS credentials belong in JSON, command arguments, Git, logs or portfolio evidence. Use existing named SDK profiles. The runner rejects a modified bundle/plan or expired authorization window before constructing SDK clients, then verifies actual assumed-role identities and release/workgroup configuration before starting any query.

## Execute only the reviewed plan

This command **can incur AWS charges**. It requires both existing scoped profiles and explicit execution opt-in. It does not provision their roles or change their permissions.

```bash
python -m taxi_platform.cloud_experiment run .cloud-work/release \
  --plan .cloud-work/experiment-plan.json --output .cloud-work/experiment-journal.json \
  --query-profile taxi-query --publisher-profile taxi-publisher --execute-reviewed-plan
```

The request token is persisted before submission and deterministically bound to the plan and table. Retrying the same plan reuses Athena's request tokens and prior execution IDs; it does not intentionally submit replacement queries. Preserve the plan and journal after a network failure. A failed restart retains prior known query records. If a query needs a genuine new execution, explicitly review a new experiment identity and its additional cost.

If cancellation is requested, the journal deliberately records `terminal_state_verified: false`. Follow up with the existing `cloud_release measure` command for every known query ID until terminal, including failed/cancelled queries. A timeout or successful cancellation request is not evidence that billing stopped. If submission lost its response before an ID was obtained, resume the same plan/token to recover that ID; do not create a new plan to hide it. Avoid concurrent operators using the same journal.

Success establishes that both aggregate row counts/trip totals agree with the local bundle in that point-in-time exercise. It does not prove full row equality, query-layout savings, a production SLA, raw-data recovery, an account-wide cost total or all forbidden operations. The earlier [serving restoration exercise](cloud-readiness-2026-11-12.json) remains the local recovery evidence; a remote restore must be recorded separately when it actually happens.

Additional denied writes/deletes, cross-environment access and catalog mutation tests remain unexecuted. Those need explicit disposable-resource scope: a wrongly permitted mutation could succeed. Do not add them to this read-focused experiment implicitly.

## Capture real operation after execution

Use a dated record with these fields; **leave unknown observations null**. Keep account identifiers and detailed journals private, and review/redact identifiers before sharing a factual summary. A proposed run is not an operating-history entry.

```json
{
  "observed_at_utc": null,
  "operator": null,
  "source_months": [],
  "source_kind": "historical TLC; not a live daily feed",
  "release_id": null,
  "code_commit": null,
  "plan_sha256": null,
  "journal_sha256": null,
  "query_ids": [],
  "reconciliation": null,
  "denial_observed": null,
  "incident_and_recovery": null,
  "maintenance_change": null,
  "billed_cost_usd": null,
  "billing_period_and_source": null,
  "retained_resources_and_teardown_status": null,
  "human_reviewer_and_feedback": null
}
```

TLC source data is published by month. A daily monitor run or repeated historical replay must be labelled accordingly; it is not daily arrival of new taxi observations. For a later corrected-source backfill, record affected months, source checksums, selective rerun scope, old/new release IDs and reconciled differences. A recovery drill should state its injected failure, observed boundary, actual restoration time and data comparisons; do not turn it into an outage/SLA claim.

At the agreed end time, stop submitting work, collect final query states and inspect actual charges when AWS reports them. The template retains both S3 buckets on stack deletion; data versions can continue costing money. Inventory remaining versions/results and obtain explicit scope before irreversible cleanup. The scoped publisher/query roles intentionally cannot delete backups.

## Offline proof and references

The [dated evidence record](cloud-experiment-readiness-2026-11-13.json) separates reproduced catalog regressions from mocked orchestration tests. It contains no AWS result. To reproduce:

```bash
pytest -q tests/test_cloud_controls.py tests/test_cloud_acceptance.py tests/test_cloud_experiment.py
ruff check .
```

AWS references: [idempotent query submission](https://docs.aws.amazon.com/athena/latest/APIReference/API_StartQueryExecution.html), [query result shape and S3 access](https://docs.aws.amazon.com/athena/latest/APIReference/API_GetQueryResults.html), [cancellation request semantics](https://docs.aws.amazon.com/athena/latest/APIReference/API_StopQueryExecution.html), [S3 missing-key and permission responses](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html).
