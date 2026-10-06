-module(bench_native).

-export([monotonic_time/0, new_counter/0, counter_increment/1, counter_read/1]).

%% Microsecond resolution avoids rounding small cells to 0-1 ms. The
%% benchmark has its own clock because Saga's internal FFI is not public.
monotonic_time() ->
    erlang:monotonic_time(microsecond).

%% Atomic increments remain safe when compared revisions invoke builders
%% from separate coordinator processes.
new_counter() ->
    atomics:new(1, [{signed, false}]).

counter_increment(Ref) ->
    atomics:add(Ref, 1, 1),
    nil.

counter_read(Ref) ->
    atomics:get(Ref, 1).
