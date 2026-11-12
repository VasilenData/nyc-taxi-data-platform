# Bounded cloud analytics and tested serving recovery

**Status: locally tested; AWS deployment not yet observed.** This extension prepares a private AWS query sandbox and proves that the published serving dataset can be backed up and restored. It does not turn the local pipeline into a distributed warehouse or claim production cloud experience.

## What is implemented

- [CloudFormation](../cloud/aws/template.json): private, encrypted, versioned S3 buckets; distinct publisher and analyst roles trusted only by a named operator; a Glue database; an Athena workgroup with enforced encrypted results and a default **100 MiB per-query scan cutoff**. Deploy separate stacks for `dev` and `prod`; no production stack has been deployed.
- The publisher can upload only the selected release prefix and register release tables. The analyst can read **only the two aggregate Parquet tables** and write query results, with no data-bucket write or delete permissions. Bucket policies reject unencrypted transport and require conditional create-only release writes. This is static policy intent; effective account permissions, SCPs and live denials still need verification.
- [Serving backups](../taxi_platform/release_bundle.py): one read transaction exports eight tables plus the publication record; SHA-256, sizes, row counts, schemas and release identity are checked before an immutable local bundle becomes visible. Restoration replaces all nine consumer relations in one transaction.
- [Cloud publisher](../taxi_platform/cloud_release.py): explicit byte budget; conditional S3 writes; remote checksum verification; commit manifest uploaded last; only then are release-specific aggregate catalog tables registered. Interrupted upload can be resumed. A catalog interruption may leave one of the two new tables registered; there is **no atomic cross-table Glue catalog switch** and no mutable `latest` alias. Consumers must select both tables from the same complete manifest/release ID.
- Read-only Athena measurement collection records AWS-reported scan bytes, engine/total duration, SQL hash and result reuse. It never starts a query, computes an invented invoice, or labels local measurements as cloud results.

## Observed evidence

[Dated local exercise](cloud-readiness-2026-11-12.json): 30,000 historical TLC sample input rows; 29,326 valid facts and 674 quarantined records. The nine-relation Parquet bundle was **3,756,411 bytes**, exported in **0.235 s** and restored in **0.141 s** on this Windows/Python/DuckDB environment. All nine restored schemas matched, all bidirectional `EXCEPT ALL` differences were zero, and valid/quarantine and aggregate/fact reconciliations passed. These are single-run local sample measurements, not a recovery SLA or full-data/cloud benchmark.

The tests also interrupt restoration after relation 1, 5 and 9 and before commit; kill a child process with `os._exit(37)` after copying facts; and verify that the previous complete dataset survives before a successful retry. Corrupt, missing, extra and path-tampered bundle files are rejected. An interrupted mocked AWS upload cannot create its commit marker or catalog tables. Mocked SDK tests establish sequencing, **not** AWS behavior.

```bash
pip install -r requirements-dev.txt -r requirements-cloud.txt
cfn-lint cloud/aws/template.json
pytest -q tests/test_release_bundle.py tests/test_cloud_controls.py
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

4. Under the query role, verify the negative cases before publishing access: reading `fct_trips`, writing a data object, deleting an object, querying another workgroup, and reading another environment's bucket must fail. Under the publisher role, overwrite without `If-None-Match` and writes to another release must fail. Record actual errors and identities; static policy tests are not a substitute.

5. Explicitly run a baseline and filtered/column-pruned query against the release-specific daily aggregate through the bounded workgroup. Disable result reuse for the scan comparison. Keep both query IDs and query texts with the evidence, then collect the service counters:

```sql
SELECT * FROM "CATALOG_DATABASE"."release_UUIDWITHOUTDASHES_mart_daily_borough";
SELECT pickup_date, sum(trips) AS trips
FROM "CATALOG_DATABASE"."release_UUIDWITHOUTDASHES_mart_daily_borough"
WHERE pickup_date >= DATE '2023-02-01'
GROUP BY pickup_date;
```

```bash
python -m taxi_platform.cloud_release --profile taxi-query --region eu-west-1 measure \
  --workgroup WORKGROUP --query-id BASELINE_QUERY_ID --query-id FILTERED_QUERY_ID \
  --output .cloud-work/athena-measurements.json
```

Do not promise scan savings: this small aggregate can fit a single Parquet row group and may show little change. A useful full-data experiment should compare partitioned layouts on observed service counters, including cold and reused queries.

## Costs, teardown and limits

The cutoff applies **per query**, not across an hour/month. AWS can charge for bytes scanned by a canceled query; S3 storage/requests, catalog operations and result retention are separate. The uploader's byte cap constrains one release, not repeated releases. A budget alert is not a hard spending ceiling. Choose a total experiment budget, a small fixed query count and an end time; inspect actual charges afterward. This template does not deploy running VMs, clusters, NAT gateways or scheduled jobs.

Results expire after seven days, including noncurrent versions. Data versions are deliberately retained for recovery. Both buckets have `DeletionPolicy: Retain`: deleting the stack leaves the storage and its costs. After verified recovery and approval to discard experiment data, an account operator must separately empty/delete retained bucket versions. Least-privilege publisher/query roles cannot delete backups. No automatic cleanup is scheduled.

References: [Athena workgroup controls](https://docs.aws.amazon.com/athena/latest/ug/workgroups-manage-queries-control-costs.html), [workgroup configuration](https://docs.aws.amazon.com/athena/latest/APIReference/API_WorkGroupConfiguration.html), [S3 conditional-write enforcement](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes-enforce.html).
