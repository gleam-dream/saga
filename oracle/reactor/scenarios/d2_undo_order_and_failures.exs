# SPDX-FileCopyrightText: 2026 gleam-dream contributors
#
# SPDX-License-Identifier: Apache-2.0
#
# D2: undo ordering and multiple undo failures. Sequential
# e1 -> e2 -> e3 -> e4(fail), where e2 and e3's undo return {:error, _}.
# Adapted from reactor/executor_test.exs:353-411 "successful steps can be
# undone", extended with failing undos the way the planner's P1 probe did.
#
# Expected (recorded from real Reactor 1.0.6, see expected/d2.txt):
# Reactor undoes in FORWARD completion order (e1, e2, e3) and retains
# every undo failure plus the triggering run failure as separate error
# classes. Saga is a DELIBERATE DIFFERENCE here: it undoes in REVERSE
# completion order (e3, e2, e1). See PROVENANCE.md D2 / oracle/reactor's
# scripts/oracle.sh, which asserts the difference rather than hiding it.

defmodule Oracle.D2 do
  use Reactor

  input(:agent)

  defmodule S do
    @moduledoc false
    use Reactor.Step

    def run(%{agent: agent}, _context, opts) do
      if opts[:fail] do
        {:error, :boom}
      else
        Agent.update(agent, &[{:run, opts[:name]} | &1])
        {:ok, opts[:name]}
      end
    end

    def undo(_value, %{agent: agent}, _context, opts) do
      Agent.update(agent, &[{:undo, opts[:name]} | &1])
      if opts[:undo_fail], do: {:error, {:undo_failed, opts[:name]}}, else: :ok
    end
  end

  step :e1, {S, name: :e1} do
    argument(:agent, input(:agent))
    async?(false)
  end

  step :e2, {S, name: :e2, undo_fail: true} do
    argument(:agent, input(:agent))
    wait_for(:e1)
    async?(false)
  end

  step :e3, {S, name: :e3, undo_fail: true} do
    argument(:agent, input(:agent))
    wait_for(:e2)
    async?(false)
  end

  step :e4, {S, name: :e4, fail: true} do
    argument(:agent, input(:agent))
    wait_for(:e3)
    async?(false)
  end

  return(:e4)
end

{:ok, agent} = Agent.start_link(fn -> [] end)
result = Reactor.run(Oracle.D2, %{agent: agent}, %{}, async?: false)

Oracle.Trace.print("d2.trace", Agent.get(agent, &Enum.reverse/1))

error_classes =
  case result do
    {:error, error} -> Enum.map(error.errors, & &1.__struct__)
    _ -> []
  end

Oracle.Trace.print("d2.error_classes", error_classes)
Oracle.Trace.print("d2.result", elem(result, 0))
