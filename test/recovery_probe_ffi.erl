-module(recovery_probe_ffi).
-export([mode/0, path/0, write_ledger/2, ledger_has/2, remove/1]).

mode() -> env("SAGA_PROBE_MODE").
path() -> env("SAGA_PROBE_PATH").

env(Name) ->
    case os:getenv(Name) of
        false -> <<>>;
        Value -> unicode:characters_to_binary(Value)
    end.

write_ledger(Path, Key) ->
    ok = file:write_file(<<Path/binary, ".ledger">>, Key, [sync]),
    nil.

ledger_has(Path, Key) ->
    case file:read_file(<<Path/binary, ".ledger">>) of
        {ok, Key} -> true;
        _ -> false
    end.

remove(Path) -> file:delete(Path), nil.
