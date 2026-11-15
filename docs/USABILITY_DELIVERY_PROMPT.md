# Delivery prompt: usable, verifiable data products

This is an original task-specific synthesis, not a claim that a universal "best prompt" exists. It applies clear workflows, explicit success criteria, independent verification and restrained complexity to the three existing projects.

## Task

Act as a principal data engineer and product engineer. Turn the existing project into a tool that a new user can install or open, complete one useful task with, and verify without help from its author. Work in the established architecture and visual style. Preserve existing correctness, isolation, recovery, business definitions and immutable historical evidence.

1. Read the current code, consumers, contracts and first-run instructions. Identify the specific user, their starting input, their useful output, and the missing step. Do not recreate an existing feature.
2. Implement one complete, bounded journey. Include a sample that works without credentials, explicit supported input, understandable errors, a useful export, and a path to the code and evidence. Keep private inputs local and out of source control, analytics, logs and public artifacts.
3. Define the data contract before implementation: types, keys, units, currency, dates/time zones, duplicate policy, inclusion/exclusion rules and output grain. Reject ambiguity rather than guessing. Label generated examples as synthetic and historical data as historical. A new private-data mode must not weaken or relabel the existing synthetic fixture contract.
4. Preserve complete previous results when validation, generation or publication fails. Use bounded work, strict input validation, consistent snapshot identity and atomic delivery where appropriate. Provide actionable errors and rerun/recovery guidance.
5. Make the visible product usable: concise task-oriented copy, keyboard access, responsive layouts, empty/error/loading states, readable tables and clear downloads. Retain the established charcoal/mint direction. Avoid visual decoration that obscures the workflow.
6. Prove meaningful behavior using independent arithmetic or SQL, adversarial examples and actual execution. Test a new user's documented path, repeated runs, malformed data, missing prerequisites and preserved prior outputs. Browser checks must exercise the real UI. Do not claim a hosted backend, observed user adoption, unaided interview performance, or a timed user trial that did not occur.
7. Keep changes reviewable. Document the quickstart, supported environment, limitations and exact verification commands. Run the required lint, tests, builds and integration gates, preserving honest skips. Use current primary documentation when version behavior matters.
8. Publish only within the existing authorization: code and static/read-only demos, local computation, $0 cloud. Verify final Git blobs, final-head CI, deployment results and downloadable bytes. No paid resources, public writable backend, outreach or credential/account changes.

## Completion criteria

- A documented fresh-user workflow produces the stated useful result.
- Invalid input identifies the offending field/record and cannot replace a valid result.
- Displayed totals, exports and independently computed expectations agree.
- A rerun has defined behavior and does not silently duplicate data.
- Historical evidence is preserved against the source revision it actually measured.
- The published artifact matches the tested source and accurately states its limits.

The proposed ten-minute first-use target is a design goal, not an observed user-study result. Measure any future completion time with the environment and prerequisite assumptions stated.

## Primary references consulted

- [Anthropic: Building effective agents](https://www.anthropic.com/research/building-effective-agents): simple workflows, clear decomposition and feedback loops; used as process guidance, not as a software framework dependency.
- [Anthropic: Demystifying evals for AI agents](https://www.anthropic.com/research/demystifying-evals-for-ai-agents): evaluate resulting system state and explicit task outcomes.
- [Docker Compose startup order](https://docs.docker.com/compose/how-tos/startup-order/): distinguish running containers, healthy dependencies and completed bootstrap jobs.
- [DuckDB CSV configuration](https://duckdb.org/docs/stable/data/csv/overview): specify supported types and parsing behavior rather than relying on inference for business identifiers or amounts.
- [Python atomic replacement](https://docs.python.org/3/library/os.html#os.replace): use the actual replacement guarantees and document filesystem limits.

These references inform implementation; repository tests and observed executions establish the project's evidence.
