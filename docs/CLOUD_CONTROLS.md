# Bounded cloud analytics and tested serving recovery

**Status: locally tested; AWS deployment not yet observed.** This extension prepares a private AWS query sandbox and proves that the published serving dataset can be backed up and restored. It does not turn the local pipeline into a distributed warehouse or claim production cloud experience.

## What is implemented

- [CloudFormation](../cloud/aws/template.json): private, encrypted, versioned S3 buckets; distinct publisher and analyst roles trusted only by a named operator; a Glue database; an Athena workgroup with enforced encrypted results and a default **100 MiB per-query scan cutoff**. Deploy separate stacks for `dev` and `prod`; no production stack has been deployed.
- The publisher can upload only the selected release prefix and register its **two exact aggregate catalog tables**. The analyst can read those two aggregate Parquet prefixes **and that release's commit manifest**, and write query results, with no data-bucket write or delete permissions. Both Glue policies name the two tables plus required catalog/database ancestors, rather than a `release_*` table wildcard. Bucket policies reject unencrypted transport and require conditional create-only release writes. This is static policy intent; effective account permissions, SCPs and live denials still need verification.
- [Serving backups](../taxi_platform/release_bundle.py): one read transaction exports eight tables plus the publication record; SHA-256, sizes, row counts, schemas and release identity are checked before an immutable local bundle becomes visible. Restoration replaces all nine consumer relations in one transaction.
- [Cloud publisher](../taxi_platform/cloud_release.py): explicit byte budget; conditional S3 writes; remote checksum verification; commit manifest uploaded last; only then are release-specific aggregate catalog tables registered. Interrupted upload can be resumed. A catalog interruption may leave one of the two new tables registered; there is **no atomic cross-table Glue catalog switch** and no mutable `latest` alias. Consumers must select both tables from the same complete manifest/release ID.
- [Read-only acceptance](../taxi_platform/cloud_acceptance.py) checks the expected assumed query role, enabled/enforced workgroup, approved scan cutoff, owned/encrypted result destination, metrics and effective engine version. It verifies the remote manifest against a trusted local bundle, lists each aggregate prefix to reject extra files, checks remote SHA-256/size/encryption and matches catalog schema, empty partition keys, format and location. It reads a bounded manifest and object metadata; it does not download the fact dataset, start queries or change access.
- Read-only Athena measurement collection records **all supplied query states**, including failed and cancelled work. Its bounded ledger rejects duplicate IDs, preserves absent counters as `null`, flags provisional running-query data, and totals only known scan bytes. Successful DML with known non-reused results and scan counters is eligible for comparison; SQL equivalence still requires review. It never starts a query or computes an invented invoice. SQL text is hashed, not included in the report.
- The [two-query experiment](CLOUD_EXPERIMENT.md) binds a trusted sample, account/region, explicit budget declaration and expiry to exact SQL. Its opt-in runner repeats acceptance, verifies one scoped read denial, reconciles Athena totals against local Parquet and preserves query IDs/counters across failures. It is tested offline, not an observed AWS run. Resumed catalog registration now rejects altered release/type/format/SerDe definitions; acceptance also rejects nonempty SerDe parameters that could change interpretation.

## Observed evidence

[Dated local exercise](cloud-readiness-2026-11-12.json): 30,000 historical TLC sample input rows; 29,326 valid facts and 674 quarantined records. The nine-relation Parquet bundle was **3,756,411 bytes**, exported in **0.235 s** and restored in **0.141 s** on this Windows/Python/DuckDB environment. All nine restored schemas matched, all bidirectional `EXCEPT ALL` differences were zero, and valid/quarantine and aggregate/fact reconciliations passed. These are single-run local sample measurements, not a recovery SLA or full-data/cloud benchmark.

The tests also interrupt restoration after relation 1, 5 and 9 and before commit; kill a child process with `os._exit(37)` after copying facts; and verify that the previous complete dataset survives before a successful retry. Corrupt, missing, extra and path-tampered bundle files are rejected. An interrupted mocked AWS upload cannot create its commit marker or catalog tables. Mocked SDK tests establish sequencing, **not** AWS behavior.

The [13 November offline regression record](cloud-controls-2026-11-13.json) documents four assertions that failed before this upgrade and pass afterward: cancelled-query scan usage was rejected, analysts lacked commit-manifest access, and each role allowed all `release_*` catalog tables. The expanded tests use a real local Parquet bundle with synthetic three-row relations and mocked service responses. They reject 18 injected identity/configuration/object/catalog changes. Their synthetic five-query ledger totals 200 known scan bytes, including failed/cancelled work, while one running query remains unknown. Those bytes are **fixture values, not an AWS measurement**. An AI cross-review then identified a fifth counterexample: unexpected catalog partition keys passed acceptance despite changing query semantics. The new regression failed before the follow-up fix; catalog creation and acceptance now explicitly require an unpartitioned table. This is recorded as AI review, not an independent human endorsement. The [review prompt](CLOUD_REVIEW_PROMPT.md) makes deployment and evidence boundaries explicit for future changes.

```bash
pip install -r requirements-dev.txt -r requirements-cloud.txt
cfn-lint cloud/aws/template.json
pytest -q tests/test_release_bundle.py tests/test_cloud_controls.py tests/test_cloud_acceptance.py
python scripts/cloud_readiness_exercise.py --output .cloud-work/local-proof.json
```

The backup covers published serving relations only. It excludes `raw`, `build` and audit history, configuration, credentials and scheduling state. A restored copy can answer consumer queries; ingestion must be rebuilt from the retained sources before further publication. Manifests detect corruption, not malicious replacement by someone who can replace both data and hashes. Keep bundles and their manifest hashes in a trusted location.

## Prepared deployment sequence

**Required before deployment:** an authenticated AWS account/profile, region, an existing named operator IAM principal and an approved total experiment spending ceiling. None were available during this implementation. No AWS resources were provisioned and no billed cloud result is claimed. The optional tools use the SDK credential chain; credentials must not be committed or supplied as CLI arguments.

1. Produce a clean warehouse with the existing sample pipeline and export its published release. Keep backups outside the public repo.

```bash
python -m taxi_platform.release_bundle export .cloud-work/release --database data/warehouse.duckdb
python -m taxi_platform.release_bundle verify .cloud-work/release
```

2. Inspect the CloudFormation template and create a change set in the selected account. Use a separate stack for each environment. Supply the bundle's `release_id` and the exact existing operator ARN. The following AWS CLI command is a **deployment command**, to run only after account and spend approval:

```bash
aws cloudformation deploy --profile operator --region eu-west-1 \
  --stack-name taxi-dev --template-file cloud/aws/template.json \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides Environment=dev ReleaseId=RELEASE_UUID \
  OperatorPrincipalArn=EXISTING_OPERATOR_ARN PerQueryScanLimitBytes=104857600
```

An operator needs CloudFormation/S3/IAM/Glue/Athena provisioning permissions to create the stack; the scoped roles themselves do not grant these. Stack outputs provide role ARNs, bucket names, catalog database and workgroup. Configure named `taxi-publisher` and `taxi-query` AWS profiles to assume their respective output roles using the existing authenticated operator profile.

3. Upload only after checking the sample size. Substitute actual stack outputs and the account ID; the explicit **16 MiB** cap below is enough for the measured sample. Larger releases require a deliberate higher budget.

```bash
python -m taxi_platform.cloud_release --profile taxi-publisher --region eu-west-1 upload \
  .cloud-work/release --bucket DATA_BUCKET --database CATALOG_DATABASE \
  --account-id ACCOUNT_ID --max-bytes 16777216
```

4. First perform read-only acceptance under the scoped query role. Substitute the actual stack outputs and preserve the original verified local bundle; substituting an untrusted downloaded manifest would remove the trust anchor. This command uses only `GetCallerIdentity`, `GetWorkGroup`, `GetObject`, `ListObjectsV2`, `HeadObject` and `GetTable`. The list reads are bounded to two keys per aggregate prefix, enough to reject an unexpected second file; a truncated response also fails.

```bash
python -m taxi_platform.cloud_acceptance .cloud-work/release \
  --profile taxi-query --region eu-west-1 --environment dev \
  --data-bucket DATA_BUCKET --results-bucket RESULTS_BUCKET \
  --database CATALOG_DATABASE --workgroup WORKGROUP --account-id ACCOUNT_ID \
  --query-role-arn QUERY_ROLE_ARN --max-scan-bytes 104857600 \
  --output .cloud-work/aws-acceptance.json
```

Then verify the negative cases before publishing access: reading `fct_trips`, writing a data object, deleting an object, querying another workgroup, reading another release's catalog tables, and reading another environment's bucket must fail. Under the publisher role, overwrite without `If-None-Match`, writes to another release, and creating another release's catalog table must fail. Record actual errors and identities; successful read-only acceptance and static policy tests do **not** establish effective denied permissions. Do not run mutating denial probes unless their account/resource scope is explicitly authorized: unexpectedly broad permissions can allow them to succeed.

5. Use the [reviewed two-query plan](CLOUD_EXPERIMENT.md) for the first experiment. It checks both aggregates against a local oracle rather than claiming performance gains from queries with different answers. It also performs the bounded fact-read denial probe above; other denied operations remain a separately authorized exercise. Keep its plan and journal, then collect final counters for any query left running or cancelled:

```bash
python -m taxi_platform.cloud_release --profile taxi-query --region eu-west-1 measure \
  --workgroup WORKGROUP --query-id FIRST_QUERY_ID --query-id SECOND_QUERY_ID \
  --output .cloud-work/athena-measurements.json
```

Performance/layout comparisons are a separate later experiment with additional queries and budget review. Do not promise scan savings: this small aggregate can fit a single Parquet row group and may show little change. Compare equivalent answers across layouts on observed service counters, including cold and reused queries.

Include failed and cancelled query IDs when collecting usage. The ledger accepts 1–100 distinct IDs; it is not workgroup-wide accounting, and omitted executions are not discovered automatically. `known_scanned_bytes` is a subtotal, not a completeness claim: inspect `unknown_scan_query_count` and `all_terminal_scan_counters_present`. Running/queued executions need collection again after completion. `eligible_for_scan_comparison` does not mean the example queries answer the same question; compare equivalent SQL when evaluating a storage/layout change. No dollar estimate or invoiced charge is generated from scan counters.

## Costs, teardown and limits

The cutoff applies **per query**, not across an hour/month. AWS can charge for bytes scanned by a canceled query; S3 storage/requests, catalog operations and result retention are separate. The uploader's byte cap constrains one release, not repeated releases. A budget alert is not a hard spending ceiling. Choose a total experiment budget, a small fixed query count and an end time; inspect actual charges afterward. This template does not deploy running VMs, clusters, NAT gateways or scheduled jobs.

Results expire after seven days, including noncurrent versions. Data versions are deliberately retained for recovery. Both buckets have `DeletionPolicy: Retain`: deleting the stack leaves the storage and its costs. After verified recovery and approval to discard experiment data, an account operator must separately empty/delete retained bucket versions. Least-privilege publisher/query roles cannot delete backups. No automatic cleanup is scheduled.

References: [Glue unpartitioned-table requirements](https://docs.aws.amazon.com/glue/latest/webapi/API_Table.html), [Athena query limits and cancelled-query charges](https://docs.aws.amazon.com/athena/latest/ug/workgroups-setting-control-limits-cloudwatch.html), [execution statistics and optional counters](https://docs.aws.amazon.com/athena/latest/APIReference/API_QueryExecutionStatistics.html), [Glue table and ancestor permissions](https://docs.aws.amazon.com/athena/latest/ug/fine-grained-access-to-glue-resources.html), [workgroup configuration](https://docs.aws.amazon.com/athena/latest/APIReference/API_WorkGroupConfiguration.html), [S3 conditional-write enforcement](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes-enforce.html).
