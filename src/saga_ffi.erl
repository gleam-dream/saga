-module(saga_ffi).

-export([
    rescue/5,
    schedulers_online/0,
    unique_integer/0,
    monotonic_time/0,
    system_time/0,
    identity/1
]).

%% Runs `Fun` and calls back into exactly one of the supplied continuations:
%% `OnOk(Value)` when `Fun` returns normally, `OnError(Reason)` /
%% `OnExit(Reason)` / `OnThrow(Reason)` when it raises that class. `Reason` is
%% always a formatted string. Routing the outcome through Gleam-supplied
%% closures (rather than returning a raw tagged tuple for the Gleam side to
%% decode) keeps this the only native boundary the rescue path crosses.
rescue(Fun, OnOk, OnError, OnExit, OnThrow) ->
    try
        OnOk(Fun())
    catch
        error:Reason -> OnError(format_reason(error, Reason));
        exit:Reason -> OnExit(format_reason(exit, Reason));
        throw:Reason -> OnThrow(format_reason(throw, Reason))
    end.

format_reason(Class, Reason) ->
    unicode:characters_to_binary(
        io_lib:format("~p", [{Class, Reason}])
    ).

schedulers_online() ->
    erlang:system_info(schedulers_online).

unique_integer() ->
    erlang:unique_integer([positive, monotonic]).

monotonic_time() ->
    erlang:monotonic_time(millisecond).

system_time() ->
    erlang:system_time(millisecond).

%% A native no-op, used only by `saga/internal/store`'s single unsafe coerce
%% to change a value's Gleam-tracked type without touching the value itself
%% (BEAM erases parametric types at runtime, so this never inspects or
%% rebuilds anything -- see that module's doc comment for the soundness
%% argument).
identity(X) ->
    X.
