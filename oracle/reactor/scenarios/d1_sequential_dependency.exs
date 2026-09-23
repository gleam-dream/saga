# SPDX-FileCopyrightText: 2026 gleam-dream contributors
#
# SPDX-License-Identifier: Apache-2.0
#
# D1: dependency ordering. Three steps chained a -> b -> c, each recording
# its name into a shared Agent when it runs. Reactor must run them in
# dependency order. Adapted from reactor/executor_test.exs:13-48
# "it executes the steps".

defmodule Oracle.D1 do
  use Reactor

  input(:agent)

  defmodule Rec do
    @moduledoc false
    use Reactor.Step

    def run(%{agent: agent}, _context, opts) do
      Agent.update(agent, &[{:run, opts[:name]} | &1])
      {:ok, opts[:name]}
    end
  end

  step :a, {Rec, name: :a} do
    argument(:agent, input(:agent))
    async?(false)
  end

  step :b, {Rec, name: :b} do
    argument(:agent, input(:agent))
    wait_for(:a)
    async?(false)
  end

  step :c, {Rec, name: :c} do
    argument(:agent, input(:agent))
    wait_for(:b)
    async?(false)
  end

  return(:c)
end

{:ok, agent} = Agent.start_link(fn -> [] end)
result = Reactor.run(Oracle.D1, %{agent: agent}, %{}, async?: false)

Oracle.Trace.print("d1.trace", Agent.get(agent, &Enum.reverse/1))
Oracle.Trace.print("d1.result", elem(result, 0))

# Normalized lines: what scripts/oracle.sh actually diffs against the
# Gleam side's test/oracle_test.gleam:main/0 output (see
# oracle/differences/README.md for the format).
Oracle.Trace.print_normalized("normalized.d1.trace", Agent.get(agent, &Enum.reverse/1))
Oracle.Trace.print_normalized("normalized.d1.result", elem(result, 0))
