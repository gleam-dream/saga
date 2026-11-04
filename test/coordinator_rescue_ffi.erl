-module(coordinator_rescue_ffi).

-export([control/1, coerce/1, cause_round_trips/0]).

%% The control subject of a local `saga/execution.Execution` record
%% (`{execution, Pid, RunId, Control, Delivery}`), so a test can deliver a
%% message the coordinator cannot handle.
control({execution, _Pid, _RunId, Control, _Delivery}) ->
    Control.

%% Changes a value's Gleam type without touching it, to build a message
%% whose field is not the type the coordinator expects.
coerce(Value) ->
    Value.

%% Whether a durable checkpoint holding `CoordinatorCrashed` as a settling
%% trigger, a sibling failure, and a finished outcome's cause encodes and
%% decodes unchanged.
cause_round_trips() ->
    Crash = {crash, error_class, <<"badarg in erlang:send_after/3">>},
    Cause = {coordinator_crashed, Crash},
    Settlement = {settlement, [], [], [], [], [], [], [Cause], []},
    Snapshot = {snapshot, [], [], {saved_settling, {trigger_failure, Cause}, Settlement},
                [], [], none},
    Envelope = {envelope, 2, <<"id">>, <<"stamp">>, <<"input">>, {some, Snapshot},
                {some, {failed, Cause, Settlement}}, none, none},
    Same = fun(Value) -> {ok, Value} end,
    {ok, Bytes} = saga_checkpoint:encode(Envelope, Same, Same, Same),
    {ok, Envelope} =:= saga_checkpoint:decode(Bytes, Same, Same, Same).
