# SPDX-FileCopyrightText: 2026 gleam-dream contributors
#
# SPDX-License-Identifier: Apache-2.0

defmodule Oracle.Trace do
  @moduledoc """
  A tiny shared recorder used by every scenario script.

  Each scenario appends `{event, step}` (or richer) tuples to an `Agent` as
  the Reactor run progresses, then prints the reversed list as a single
  line of Erlang-term text via `inspect/2` with `charlists: :as_lists` and
  a stable width, so `scripts/oracle.sh` can diff it byte-for-byte against
  `expected/d*.txt`. Keep event shapes small and orderable (atoms, tuples,
  integers) — never a struct, timestamp, or pid — so the same scenario
  produces the same text on every run.
  """

  def start, do: Agent.start_link(fn -> [] end)

  def record(agent, event), do: Agent.update(agent, &[event | &1])

  def events(agent), do: Agent.get(agent, &Enum.reverse/1)

  @doc "Prints a labelled, single-line, diff-stable trace."
  def print(label, term) do
    IO.puts("#{label}: #{inspect(term, limit: :infinity, printable_limit: :infinity)}")
  end
end
