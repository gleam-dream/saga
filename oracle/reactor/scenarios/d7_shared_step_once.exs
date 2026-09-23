# SPDX-FileCopyrightText: 2026 gleam-dream contributors
#
# SPDX-License-Identifier: Apache-2.0
#
# D7: a shared dependency executes once. A diamond: `producer` feeds both
# `left` and `right`, which join into `combine`. Reactor's DAG scheduler
# memoizes a step's result for every downstream consumer, so `producer`
# must run exactly once even though two steps depend on it. The upstream
# suite has no single test isolating this (memoization is implicit in the
# planner's DAG); this scenario is a native Saga probe compared
# differentially rather than a faithful adaptation.

defmodule Oracle.D7 do
  use Reactor

  input(:agent)

  defmodule Producer do
    @moduledoc false
    use Reactor.Step

    def run(%{agent: agent}, _context, _opts) do
      Agent.update(agent, &[:produce | &1])
      {:ok, :shared_value}
    end
  end

  step :producer, Producer do
    argument(:agent, input(:agent))
  end

  step :left do
    argument(:value, result(:producer))
    run(fn %{value: v}, _context -> {:ok, {:left, v}} end)
  end

  step :right do
    argument(:value, result(:producer))
    run(fn %{value: v}, _context -> {:ok, {:right, v}} end)
  end

  step :combine do
    argument(:left, result(:left))
    argument(:right, result(:right))
    run(fn %{left: l, right: r}, _context -> {:ok, {l, r}} end)
  end

  return(:combine)
end

{:ok, agent} = Agent.start_link(fn -> [] end)
result = Reactor.run(Oracle.D7, %{agent: agent}, %{}, max_concurrency: 8)

events = Agent.get(agent, &Enum.reverse/1)
produce_count = Enum.count(events, &(&1 == :produce))

Oracle.Trace.print("d7.trace", events)
Oracle.Trace.print("d7.produce_count", produce_count)
Oracle.Trace.print("d7.result", result)
