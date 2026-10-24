-module(saga_postgres_test_ffi).
-export([getenv/1, read_file/1, unique/0, with_process/2]).

getenv(Name) ->
    case os:getenv(binary_to_list(Name)) of
        false -> {error, nil};
        Value -> {ok, unicode:characters_to_binary(Value)}
    end.

read_file(Path) ->
    case file:read_file(Path) of
        {ok, Bytes} -> {ok, Bytes};
        {error, _} -> {error, nil}
    end.

unique() ->
    erlang:unique_integer([positive]).

%% Runs Work, then stops Pid (a pool) whether Work returned or raised.
with_process(Pid, Work) ->
    try Work()
    after
        unlink(Pid),
        Ref = monitor(process, Pid),
        exit(Pid, shutdown),
        receive
            {'DOWN', Ref, process, Pid, _} -> ok
        after 5000 ->
            exit(Pid, kill),
            receive {'DOWN', Ref, process, Pid, _} -> ok end
        end
    end.
