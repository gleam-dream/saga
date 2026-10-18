# Expected oracle differences

Each `dN.diff` is the captured difference between the normalized Reactor output
and the normalized Saga output of scenario `dN`. `scripts/oracle.sh` recomputes
the comparison: a scenario without a `dN.diff` must produce an empty difference,
and a scenario with one must match it byte for byte. Recompute a file only after
a reviewed behavior change, never by hand.

```sh
nix develop .#oracle --command scripts/oracle.sh
```

The scenarios, verdicts, pin and limits are in [docs/ORACLE.md](../../docs/ORACLE.md).

| File               | Recorded difference                                                                                 |
| ------------------ | --------------------------------------------------------------------------------------------------- |
| [d2.diff](d2.diff) | Reactor undoes `e1, e2, e3`; Saga undoes in reverse completion order, `e3, e2, e1`.                 |
| [d3.diff](d3.diff) | Reactor leaves the late `slow` effect after returning; Saga undoes it once its completion is known. |
