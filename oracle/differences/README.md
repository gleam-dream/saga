# Expected oracle differences

Each `dN.diff` is captured output from comparing the `normalized.dN.*` lines of
one scenario. Reactor runs through `mix run` in `oracle/reactor/scenarios`;
Saga runs through `gleam run -m oracle_test`. The
[normalization rules](../reactor/lib/oracle/trace.ex) define the shared format.

```sh
diff -u --label reactor --label saga <reactor-normalized-lines> <saga-normalized-lines>
```

[`scripts/oracle.sh`](../../scripts/oracle.sh) reruns the comparison:

- Without a `dN.diff`, the scenario claims a match and the computed diff must be empty.
- With a `dN.diff`, the scenario claims a deliberate difference and the computed diff must match the file byte for byte.

A failed comparison means the corresponding [provenance classification](../../PROVENANCE.md#differential-oracle-scenarios-oraclereactor)
needs review. The separate Reactor raw-term capture check can identify upstream
changes; the normalized comparison checks behavior between the two implementations.

| File               | Recorded difference                                                                                 |
| ------------------ | --------------------------------------------------------------------------------------------------- |
| [d2.diff](d2.diff) | Reactor undoes `e1, e2, e3`; Saga undoes in reverse completion order, `e3, e2, e1`.                 |
| [d3.diff](d3.diff) | Reactor leaves the late `slow` effect after returning; Saga undoes it once its completion is known. |

After an intentional, reviewed behavior change, rerun both scenario capture
commands and save the computed diff. Never edit an expected diff by hand.
