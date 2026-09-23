# SPDX-FileCopyrightText: 2026 gleam-dream contributors
#
# SPDX-License-Identifier: Apache-2.0
#
# D5: compensate vs undo, `{:continue, value}` path. A step fails once,
# its compensation resolves with a replacement value via `{:continue, v}`,
# and the run completes successfully with that value substituted for the
# step's result. Adapted from reactor/executor/step_runner_test.exs:170-250
# (compensate continue -> ok).

defmodule Oracle.D5 do
  use Reactor

  input(:agent)

  defmodule S do
    @moduledoc false
    use Reactor.Step

    def run(%{agent: agent}, _context, _opts) do
      Agent.update(agent, &[:run | &1])
      {:error, :boom}
    end

    def compensate(_error, %{agent: agent}, _context, _opts) do
      Agent.update(agent, &[:compensate | &1])
      {:continue, :replacement}
    end
  end

  step :flaky, S do
    argument(:agent, input(:agent))
    async?(false)
  end

  return(:flaky)
end

{:ok, agent} = Agent.start_link(fn -> [] end)
result = Reactor.run(Oracle.D5, %{agent: agent}, %{}, async?: false)

Oracle.Trace.print("d5.trace", Agent.get(agent, &Enum.reverse/1))
Oracle.Trace.print("d5.result", result)

Oracle.Trace.print_normalized("normalized.d5.trace", Agent.get(agent, &Enum.reverse/1))
Oracle.Trace.print_normalized("normalized.d5.result", result)
