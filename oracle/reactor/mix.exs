defmodule Oracle.MixProject do
  use Mix.Project

  # Differential oracle harness: runs the plan's side-by-side scenarios
  # against real Reactor 1.0.6 and prints a normalized trace that
  # `scripts/oracle.sh` diffs against `expected/d*.txt`. See
  # `/code/gleam-dream/saga/PROVENANCE.md` for what each scenario proves.
  def project do
    [
      app: :oracle,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:reactor, "== 1.0.6"}
    ]
  end
end
