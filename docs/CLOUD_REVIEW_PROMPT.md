# Review prompt for bounded cloud evidence

This is a repository-specific engineering checklist adapted from the AWS primary references in [Cloud Controls](CLOUD_CONTROLS.md). It is not an externally validated "best prompt" or evidence of independent human review. Use it to ask for falsifiable changes and reproducible evidence:

> Review this repository as a data-platform maintainer. Read the current code, policy documents and dated evidence before proposing additions. Preserve the distinction between the local historical TLC benchmark, synthetic tests, mocked SDK responses and observed AWS behavior.
>
> 1. Trace a complete release from one verified local manifest through object upload, catalog registration and consumer selection. Identify where mixed versions, extra objects, corruption or interrupted registration could change query results. Reproduce one failure before fixing it.
> 2. Check the publisher and query identities separately. Scope objects and Glue tables to the exact selected release, including required ancestor resources and the commit manifest. Verify configuration syntax offline; do not label it effective IAM validation.
> 3. Collect failed, cancelled, running and successful query states. Reject duplicate IDs. Keep absent counters unknown and reused results separate from cold execution. Scope every usage subtotal to its supplied query IDs. Never call scan counters an invoice or a per-query cutoff a total spending ceiling.
> 4. Add read-only acceptance checks for a future authorized deployment. Check the actual assumed identity, workgroup settings, bounded object prefixes, checksums and catalog definitions. Do not start a query or mutate cloud resources as part of read-only acceptance. State what successful reads cannot prove.
> 5. Run the focused regressions, repository lint/tests and CloudFormation validation. Retain dated results with code hashes and exact commands. Update documentation only to the level the evidence supports, including failures and skipped checks.
> 6. No cloud deployment, credential change or spending is authorized by this review. If those actions become necessary, first deliver the tested code and concrete experiment scope. Do not invent operating history, customers, human reviewers or employer impact.

A subsequent human review should record the actual reviewer, date, findings, discussion and accepted/rejected changes. This prompt and an AI review do not supply that evidence.
