# AWS analytics sandbox

`template.json` defines private versioned storage, separate publisher/query roles,
a Glue database and an enforced Athena query scan cutoff. It creates no running
compute or scheduled jobs. Deployment has not yet been observed.

Read the [deployment, cost and recovery runbook](../../docs/CLOUD_CONTROLS.md)
before choosing an account, region, operator principal and spending ceiling.
