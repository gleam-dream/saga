-module(reporting_probe).
-export([parent/0, monitors/1]).

%% The actual coordinator that spawned this workflow action; no Saga messages
%% or results are fabricated. Used to terminate the real coordinator in tests.
parent() ->
    [Parent | _] = erlang:get('$ancestors'),
    Parent.

%% Observe the calling task's monitor target to terminate the real receiver.
monitors(Pid) ->
    {monitors, Monitors} = erlang:process_info(Pid, monitors),
    [Target || {process, Target} <- Monitors, is_pid(Target)].
