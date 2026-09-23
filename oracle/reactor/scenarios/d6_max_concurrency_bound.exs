# SPDX-FileCopyrightText: 2026 gleam-dream contributors
#
# SPDX-License-Identifier: Apache-2.0
#
# D6: max_concurrency bound. Ten independent steps, each holding briefly
# so overlapping attempts are observable, with `max_concurrency: 3`. The
# peak number of steps running at once must never exceed 3. Adapted from
# reactor/executor/concurrency_tracker_test.exs:34-80 and
# reactor/executor_test.exs:649-688.

defmodule Oracle.D6 do
  use Reactor

  input(:agent)

  defmodule S do
    @moduledoc false
    use Reactor.Step

    def run(%{agent: agent}, _context, opts) do
      running =
        Agent.get_and_update(agent, fn {cur, peak} -> {cur + 1, {cur + 1, max(cur + 1, peak)}} end)

      Process.sleep(30)
      Agent.update(agent, fn {cur, peak} -> {cur - 1, peak} end)
      {:ok, {opts[:name], running}}
    end
  end

  for i <- 1..10 do
    step :"s#{i}", {S, name: i} do
      argument(:agent, input(:agent))
    end
  end

  return(:s1)
end

{:ok, agent} = Agent.start_link(fn -> {0, 0} end)
result = Reactor.run(Oracle.D6, %{agent: agent}, %{}, max_concurrency: 3)

{_current, peak} = Agent.get(agent, & &1)

Oracle.Trace.print("d6.peak_concurrency", peak)
Oracle.Trace.print("d6.result", elem(result, 0))
