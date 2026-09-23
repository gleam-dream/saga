# SPDX-FileCopyrightText: 2026 gleam-dream contributors
#
# SPDX-License-Identifier: Apache-2.0
#
# D3: failure with an active sibling. Three async siblings join into one
# step: `slow` (300ms), `fast_fail` (50ms, fails), `quick` (instant).
# Adapted from reactor/executor/async_test.exs:166-205 (unrecoverable
# error while other steps are still running).
#
# The relative START order of `slow`/`fast_fail`/`quick` is a genuine race
# (all three are scheduled concurrently) and is NOT part of the oracle
# contract — only the causal facts below are asserted:
#
# Expected (recorded from real Reactor 1.0.6, see expected/d3.txt):
# Reactor returns {:error, _} once fast_fail's compensation and quick's
# undo finish, WHILE slow is still sleeping — `slow` has not finished and
# has therefore not been undone at the moment of return. slow completes
# AFTER the run has already returned and is NEVER undone (an orphaned
# effect). Saga is a DELIBERATE DIFFERENCE here: it settles active
# siblings before returning, so `slow` is either undone after it finishes
# within `settle_timeout`, or killed and reported `interrupted`. See
# PROVENANCE.md D3.

defmodule Oracle.D3 do
  use Reactor

  input(:agent)

  defmodule S do
    @moduledoc false
    use Reactor.Step

    def run(%{agent: agent}, _context, opts) do
      Agent.update(agent, &[{:start, opts[:name]} | &1])
      if opts[:sleep], do: Process.sleep(opts[:sleep])

      if opts[:fail] do
        {:error, :boom}
      else
        Agent.update(agent, &[{:done, opts[:name]} | &1])
        {:ok, opts[:name]}
      end
    end

    def undo(_value, %{agent: agent}, _context, opts) do
      Agent.update(agent, &[{:undo, opts[:name]} | &1])
      :ok
    end

    def compensate(_error, %{agent: agent}, _context, opts) do
      Agent.update(agent, &[{:compensate, opts[:name]} | &1])
      :ok
    end
  end

  step :slow, {S, name: :slow, sleep: 300} do
    argument(:agent, input(:agent))
  end

  step :fast_fail, {S, name: :fast_fail, sleep: 50, fail: true} do
    argument(:agent, input(:agent))
  end

  step :quick, {S, name: :quick} do
    argument(:agent, input(:agent))
  end

  step :join do
    argument(:a, result(:slow))
    argument(:b, result(:fast_fail))
    argument(:c, result(:quick))
    run(fn _args, _context -> {:ok, :joined} end)
  end

  return(:join)
end

{:ok, agent} = Agent.start_link(fn -> [] end)
result = Reactor.run(Oracle.D3, %{agent: agent}, %{}, max_concurrency: 8)

events_at_return = Agent.get(agent, &Enum.reverse/1)

# Causal facts only — never the raw interleaving of concurrent `start`
# events, which races between `slow` and `fast_fail`.
Oracle.Trace.print("d3.result", elem(result, 0))

Oracle.Trace.print(
  "d3.fast_fail_compensated_at_return",
  {:compensate, :fast_fail} in events_at_return
)

Oracle.Trace.print("d3.quick_undone_at_return", {:undo, :quick} in events_at_return)
Oracle.Trace.print("d3.slow_done_at_return", {:done, :slow} in events_at_return)
Oracle.Trace.print("d3.slow_undone_at_return", {:undo, :slow} in events_at_return)

# slow sleeps 300ms; give it time to finish so we can observe whether an
# undo for it was ever recorded after the run already returned.
Process.sleep(500)
events_after_settle = Agent.get(agent, &Enum.reverse/1)
Oracle.Trace.print("d3.slow_done_after_settle", {:done, :slow} in events_after_settle)
Oracle.Trace.print("d3.slow_undone_after_settle", {:undo, :slow} in events_after_settle)
