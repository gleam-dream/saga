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

  @doc """
  Prints a labelled fact using the SAME neutral, language-agnostic text
  format the Gleam side prints via `test/oracle_test.gleam`'s `main/0`
  (see `oracle/differences/README.md`), so `scripts/oracle.sh` performs a
  genuine byte-for-byte `diff` between the two languages' output rather
  than comparing Erlang-term syntax against a hand-written Gleam
  approximation of it. This is the format the mechanical comparison
  actually uses; `print/2` above remains for the pre-existing
  `expected/d*.txt` fixtures capturing raw Reactor evidence.

  Normalization rules (mirrored exactly on the Gleam side):
    - an event tuple `{kind, name}` becomes `kind:name`
    - a bare atom event (`:attempt`) becomes just its name, `attempt`
    - a list of events becomes `[e1, e2, e3]` (or `[]`)
    - booleans and integers print as their literal Gleam/Erlang spelling
    - `{:ok, x}` / `{:error, x}` become `ok:x` / `error:x`
  """
  def print_normalized(label, value) do
    IO.puts("#{label}: #{normalize(value)}")
  end

  defp normalize(list) when is_list(list) do
    "[" <> Enum.map_join(list, ", ", &normalize_event/1) <> "]"
  end

  defp normalize(value), do: normalize_scalar(value)

  defp normalize_event({kind, name}) when is_atom(kind) do
    "#{atom_text(kind)}:#{atom_text(name)}"
  end

  defp normalize_event(atom) when is_atom(atom), do: atom_text(atom)

  defp normalize_scalar(true), do: "true"
  defp normalize_scalar(false), do: "false"
  defp normalize_scalar(n) when is_integer(n), do: Integer.to_string(n)
  defp normalize_scalar({:ok, x}), do: "ok:#{atom_text(x)}"
  defp normalize_scalar({:error, x}), do: "error:#{atom_text(x)}"
  defp normalize_scalar(atom) when is_atom(atom), do: atom_text(atom)

  defp atom_text(""), do: ""
  defp atom_text(a) when is_atom(a), do: a |> Atom.to_string() |> String.trim_leading(":")
  defp atom_text(s) when is_binary(s), do: s
  defp atom_text(n) when is_integer(n), do: Integer.to_string(n)
end
