-module(bench_native).

-export([monotonic_time/0, new_counter/0, counter_increment/1, counter_read/1]).

%% Native monotonic clock, milliseconds. The bench harness's own timing
%% utility -- saga's internal ffi module is off limits (bench only imports
%% saga's public modules), and gleam_erlang exposes no public monotonic
%% clock, so this one function is the bench package's own tiny native
%% surface.
monotonic_time() ->
    erlang:monotonic_time(millisecond).

%% A counter safe for concurrent increments from many processes (the
%% workflow build function under benchmark runs inside each run's own
%% coordinator process, not the bench harness's process, so a single-process
%% mailbox trick cannot safely count it). Backed by an atomics array of one
%% counter, which is exactly what it exists for.
new_counter() ->
    atomics:new(1, [{signed, false}]).

counter_increment(Ref) ->
    atomics:add(Ref, 1, 1),
    nil.

counter_read(Ref) ->
    atomics:get(Ref, 1).
