# Saga run-cost benchmark

A separate Gleam package that measures trivial workflow construction and
execution through Saga's public modules. It runs apart from `gleam test`.

```sh
nix develop
cd bench
gleam run > /tmp/saga-bench.md
```

Method, retained results and limits are in [docs/BENCHMARKS.md](../docs/BENCHMARKS.md).
