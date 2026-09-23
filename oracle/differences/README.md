# Checked expected diffs

Each `dN.diff` here is the literal, checked-in output of

```sh
diff -u --label reactor --label saga <reactor-normalized-lines> <saga-normalized-lines>
```

for scenario `dN`, where "reactor-normalized-lines" and "saga-normalized-lines"
are that scenario's `normalized.dN.*` lines captured from, respectively,
`oracle/reactor/scenarios/dN_*.exs` (via `mix run`) and
`test/oracle_test.gleam`'s `main()` (via `gleam run -m oracle_test`). See
`oracle/reactor/lib/oracle/trace.ex`'s `Oracle.Trace.print_normalized/2`
moduledoc for the exact, language-neutral text format both sides print.

`scripts/oracle.sh` recomputes this diff on every run and compares it
byte-for-byte against the checked-in file:

- A scenario with **no** `dN.diff` file here is claimed to **match**:
  the script requires the recomputed diff to be empty.
- A scenario **with** a `dN.diff` file here is a claimed **deliberate
  difference**: the script requires the recomputed diff to be
  byte-identical to the checked-in one.

Either check failing means Saga's behavior (or Reactor's, if the
`expected/d*.txt` check earlier in the script also fails) has moved since
this file was captured, and PROVENANCE.md's classification for that
scenario needs to be re-examined before anyone trusts it again — this is
what makes the "match" / "deliberate difference" verdicts in
PROVENANCE.md and CAPABILITIES.md checked claims rather than an
unchecked, hand-written summary.

Currently recorded:

- `d2.diff` — undo order: Reactor undoes forward (`e1, e2, e3`), Saga
  undoes in reverse completion order (`e3, e2, e1`). See PROVENANCE.md D2.
- `d3.diff` — sibling settlement: Reactor never undoes `slow` (an
  orphaned effect after early return), Saga always undoes it once it is
  known to have completed. See PROVENANCE.md D3.

To regenerate a `dN.diff` after an intentional, reviewed behavior change,
rerun the two capture commands above for that scenario and overwrite the
file — never hand-edit it.
