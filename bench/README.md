# Saga run-cost benchmark

[![CI](https://github.com/gleam-dream/saga/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/gleam-dream/saga/actions/workflows/ci.yml)
[![saga on Hex](https://img.shields.io/hexpm/v/saga?label=Hex%3A%20saga)](https://hex.pm/packages/saga)
[![Gleam](https://img.shields.io/badge/Gleam-FFAFF3?logo=gleam&logoColor=000000)](https://gleam.run/)

A separate Gleam package that measures trivial workflow construction and
execution through Saga's public modules. It runs apart from `gleam test`.

```sh
nix develop
cd bench
gleam run > /tmp/saga-bench.md
```

Method, retained results and limits are in [docs/BENCHMARKS.md](../docs/BENCHMARKS.md).
