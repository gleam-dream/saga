# SPDX-FileCopyrightText: 2026 gleam-dream contributors
#
# SPDX-License-Identifier: Apache-2.0
#
# D4: retry limits. A step fails twice then succeeds on the third attempt,
# with compensation choosing :retry up to a max of 2 retries (Reactor's
# `current_try` is 0-based; 3 total run attempts). Adapted from
# reactor/executor/async_test.exs:228-262 and sync_test.exs:44-89
# (retry, retry count, out-of-retries -> undo).

defmodule Oracle.D4 do
  use Reactor

  input(:agent)

  defmodule S do
    @moduledoc false
    use Reactor.Step

    def run(%{agent: agent}, _context, _opts) do
      Agent.update(agent, &[:attempt | &1])
      count = agent |> Agent.get(& &1) |> Enum.count(&(&1 == :attempt))

      if count < 3 do
        {:error, :boom}
      else
        {:ok, :done}
      end
    end

    def compensate(_error, %{agent: agent}, _context, _opts) do
      Agent.update(agent, &[:retry_decision | &1])
      :retry
    end
  end

  step :flaky, S do
    argument(:agent, input(:agent))
    max_retries(2)
    async?(false)
  end

  return(:flaky)
end

{:ok, agent} = Agent.start_link(fn -> [] end)
result = Reactor.run(Oracle.D4, %{agent: agent}, %{}, async?: false)

events = Agent.get(agent, &Enum.reverse/1)
attempts = Enum.count(events, &(&1 == :attempt))

Oracle.Trace.print("d4.trace", events)
Oracle.Trace.print("d4.attempts", attempts)
Oracle.Trace.print("d4.result", result)

Oracle.Trace.print_normalized("normalized.d4.attempts", attempts)
Oracle.Trace.print_normalized("normalized.d4.result", elem(result, 0))
