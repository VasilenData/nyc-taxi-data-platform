# A failed refresh must preserve the previous complete dataset

[Watch the 74-second captioned replay](https://vasilendata.github.io/demos/nyc-recovery/) · [captured transcript](demo/run-2026-11-13/transcript.txt) · [evidence JSON](demo/run-2026-11-13/evidence.json) · [exact repair diff](demo/run-2026-11-13/repair.diff)

## What was actually executed

On 13 November 2026, `scripts/record_publication_demo.py` copied the current publisher into a temporary teaching module and deliberately removed its three transaction statements. The real publisher was not edited. This is a constructed counterexample, not a claim to have discovered a new bug in the current project or reproduced a historical incident.

The fixture contains four published valid rows and a seven-row candidate in January 2023; it is generated data, not a subset of the full TLC benchmark. Its contract is `raw = valid + rejected`, with zero rejected rows, and both marts' trip counts must equal the fact count.

1. Run the broken teaching publisher in a separate process.
2. Immediately after DuckDB finishes replacing the fact table, call `os._exit(37)`. There is no guessed delay, normal shutdown, `finally` block or Python rollback.
3. Reopen the on-disk database and compare all eight consumer tables plus publication metadata with the previous version.
4. Actually rewrite the temporary module with the original publisher, restoring `BEGIN`, `COMMIT` and `ROLLBACK`; capture the resulting source diff.
5. Repeat the same abrupt exit in a fresh database, compare every consumer row again, and retry publication.
6. Run the existing publication regression suite against the unmodified project publisher.

| Observation | Deliberately broken copy | Repaired copy |
|---|---:|---:|
| Abrupt subprocess exit code | 37 | 37 |
| Fact rows after reopen | 7 | 4 |
| Hourly mart trip count after reopen | 4 | 4 |
| Consumer relations unchanged, including metadata | 5/9 | 9/9 |
| Subsequent valid publication | Not attempted after failing invariant | 7 fact rows, 7 mart trips |

The existing regression suite passed **10 tests**. These cover invalid candidates, exceptions during table copies and before commit, abrupt process exit, and replacement of legacy views. The earlier published 31.6M-row measurement remains separate; this walkthrough does not rerun it.

Runtime: Python 3.12.14, DuckDB 2.0.0, Windows. The report records the exact observed versions, base Git revision, original source hashes, and before/after row digests. The teaching-source checksums use UTF-8 text with normalized newlines; source-file checksums cover the actual bytes read. Publication timestamps and UUIDs vary between runs, so row digests establish equality within a run, not identical bytes across different runs.

## Reproduce

Install the project's development requirements in a local environment, then use a **new** output directory:

```sh
python scripts/record_publication_demo.py --output measurements/my-recovery-demo
```

This needs no cloud account, service deployment or full TLC download. It uses disposable temporary DuckDB files and never edits the real publisher. It intentionally expects a nonzero exit from the broken example, but fails the recorder if the expected boundary, evidence file or repaired recovery is missing. The output includes `evidence.json`, `repair.diff` and `transcript.txt`. Existing evidence is not overwritten.

CI executes the same recorder after the normal tests and retains the generated evidence as an artifact. The dated public replay remains a snapshot of the named local run; a new CI artifact does not silently replace it.

## Video and source

The MP4 is a **rendered replay of captured process output**, with captions and a genuine source diff. Its timing is edited for readability. It contains no human screen recording, human narration or synthetic imitation of the owner's voice. It does not assess the owner's independent ability to diagnose or implement the fix.

The optional renderer uses Pillow and imageio-ffmpeg:

```sh
pip install Pillow imageio-ffmpeg
python scripts/render_publication_demo.py docs/demo/run-2026-11-13/evidence.json --font-dir C:/Windows/Fonts
```

The renderer expects Arial/Arial Bold and Consolas font files in the supplied font directory. The repository does not redistribute those fonts. It renders a terminal-style presentation, checks line capacity instead of clipping text, and writes a media hash manifest. The standalone HTML replay works with the accompanying files without a backend; JavaScript is optional for the video and transcript.

## Design decision and limits

Validation does not make a sequence of autocommitted replacements atomic. Keeping the candidate separate and wrapping validation, all table replacements and version metadata in one transaction lets consumers retain the previous complete version if publication fails.

Copying tables adds storage and time. This is one local DuckDB database and one writer. A consumer performing multiple queries must use a read transaction to hold one consistent version. Process termination is not a test of power-loss durability; this exercise also does not establish multi-writer behavior, managed-cloud operation or real customer traffic.

## Optional narration practice

The owner can record a personal walkthrough after running and understanding the exercise:

- State the consumer invariant: the fact, marts and version metadata must describe one release.
- Show the broken result: seven fact rows versus four mart trips.
- Explain why the child exits at an observed write boundary and why the Python exception handler cannot run.
- Point to `BEGIN` and `COMMIT`; explain that database recovery, not the Python `except`, preserves the old committed snapshot after process death.
- Show nine unchanged relations and the successful seven-row retry.
- Explain the storage trade-off and the single-writer/read-transaction boundary.

This outline is preparation material, not evidence that a personal recording or an unaided assessment has happened.
