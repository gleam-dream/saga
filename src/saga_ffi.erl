-module(saga_ffi).

-export([
    rescue/5,
    rescue_redacted/5,
    schedulers_online/0,
    unique_integer/0,
    monotonic_time/0,
    system_time/0,
    identity/1,
    aliased_sender/1,
    links/0
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

%% Like `rescue/5`, but the reason names only the exception's kind (an atom
%% reason, the atom tag of a tuple reason, or a Gleam error's kind) and the
%% innermost stack frames as Module:Function/Arity. No term the exception
%% was raised with, and no frame argument, reaches the text.
rescue_redacted(Fun, OnOk, OnError, OnExit, OnThrow) ->
    try
        OnOk(Fun())
    catch
        error:Reason:Stack -> OnError(redact(Reason, Stack));
        exit:Reason:Stack -> OnExit(redact(Reason, Stack));
        throw:Reason:Stack -> OnThrow(redact(Reason, Stack))
    end.

redact(Reason, Stack) ->
    Frames = [frame(F) || F <- lists:sublist(Stack, 3)],
    Where = lists:join(<<" < ">>, Frames),
    unicode:characters_to_binary([kind(Reason), <<" in ">>, Where]).

kind(Reason) when is_atom(Reason) -> atom_to_binary(Reason);
kind(#{gleam_error := Kind}) when is_atom(Kind) -> atom_to_binary(Kind);
kind(Reason) when is_tuple(Reason), tuple_size(Reason) > 0,
                  is_atom(element(1, Reason)) ->
    atom_to_binary(element(1, Reason));
kind(_) -> <<"exception">>.

frame({M, F, Arity, _}) when is_integer(Arity) -> mfa(M, F, Arity);
frame({M, F, Args, _}) when is_list(Args) -> mfa(M, F, length(Args));
frame(_) -> <<"?">>.

mfa(M, F, Arity) ->
    iolist_to_binary(io_lib:format("~ts:~ts/~b", [M, F, Arity])).

format_reason(Class, Reason) ->
    unicode:characters_to_binary(
        io_lib:format("~p", [{Class, Reason}])
    ).

links() ->
    {links, Links} = erlang:process_info(self(), links),
    [P || P <- Links, is_pid(P)].

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


%% Keep the normal Gleam subject for receiving, but send via a process alias.
%% Deactivating the alias drops replies that arrive after the startup deadline.
aliased_sender({subject, Owner, Tag}) when Owner =:= self() ->
    Alias = erlang:alias(),
    {fun(Value) -> Alias ! {Tag, Value}, nil end,
     fun() -> erlang:unalias(Alias), nil end}.
