-module(saga_storage).
-export([file_create/3, file_load/2, file_claim/2, file_commit/3, file_release/2,
         file_cancel/2, file_unfinished/2]).

%% One file per execution: <Directory>/<hex of the id>.saga, holding
%% {record, Revision, Generation, Cancelled, Phase, Data, Owner}, where Owner
%% is `none` or {owner, Token, Incarnation, Pid}. A claim stays while its
%% claiming process lives in this VM incarnation; a file written by an
%% earlier VM is unowned. Every mutation holds a VM-local lock on the path
%% for at most 5 seconds, then fails with `busy`.

path(Dir, Id) -> filename:join(Dir, <<(binary:encode_hex(Id))/binary, ".saga">>).

file_create(Dir, Id, Data) ->
    mutate(path(Dir, Id), fun(Path) ->
        case read(Path) of
            {error, not_found} ->
                Record = {record, 0, 0, false, pending, Data, none},
                case write(Path, Record) of ok -> {ok, stored(Record)}; Error -> Error end;
            {ok, _} -> {error, already_exists};
            Error -> Error
        end
    end).

file_load(Dir, Id) ->
    case read(path(Dir, Id)) of
        {ok, Record} -> {ok, stored(Record)};
        Error -> Error
    end.

file_claim(Dir, Id) ->
    mutate(path(Dir, Id), fun(Path) ->
        case read(Path) of
            {ok, {record, V, G, C, P, D, Owner}} ->
                case alive(Owner) of
                    true -> {error, busy};
                    false ->
                        Token = integer_to_binary(erlang:unique_integer([positive])),
                        Next = {record, V, G + 1, C, P, D,
                                {owner, Token, incarnation(), list_to_binary(pid_to_list(self()))}},
                        case write(Path, Next) of
                            ok -> {ok, {{claim, Id, G + 1, Token}, stored(Next)}};
                            Error -> Error
                        end
                end;
            Error -> Error
        end
    end).

file_commit(Dir, {claim, Id, Generation, Token}, {commit, Expected, Observed, Phase, Data}) ->
    mutate(path(Dir, Id), fun(Path) ->
        case read(Path) of
            {ok, {record, V, G, C, _, _, Owner} = _} ->
                case owns(Owner, G, Generation, Token) of
                    false -> {error, stale_owner};
                    true when C =/= Observed -> {error, cancellation_changed};
                    true when V =/= Expected -> {error, conflict};
                    true ->
                        Next = {record, V + 1, G, C, Phase, Data, Owner},
                        case write(Path, Next) of ok -> {ok, stored(Next)}; Error -> Error end
                end;
            Error -> Error
        end
    end).

file_release(Dir, {claim, Id, Generation, Token}) ->
    mutate(path(Dir, Id), fun(Path) ->
        case read(Path) of
            {ok, {record, V, G, C, P, D, Owner}} ->
                case owns(Owner, G, Generation, Token) of
                    false -> {error, stale_owner};
                    true ->
                        case write(Path, {record, V, G, C, P, D, none}) of
                            ok -> {ok, nil};
                            Error -> Error
                        end
                end;
            Error -> Error
        end
    end).

file_cancel(Dir, Id) ->
    mutate(path(Dir, Id), fun(Path) ->
        case read(Path) of
            {ok, {record, V, G, _, P, D, Owner}} ->
                case write(Path, {record, V, G, true, P, D, Owner}) of
                    ok -> {ok, nil};
                    Error -> Error
                end;
            Error -> Error
        end
    end).

file_unfinished(Dir, Limit) ->
    case file:list_dir(Dir) of
        {ok, Names} ->
            Ids = lists:filtermap(fun(Name) ->
                Binary = unicode:characters_to_binary(Name),
                case binary:split(Binary, <<".saga">>) of
                    [Hex, <<>>] ->
                        try binary:decode_hex(Hex) of
                            Id ->
                                case read(path(Dir, Id)) of
                                    {ok, {record, _, _, _, Phase, _, Owner}} ->
                                        case Phase =/= finished andalso not alive(Owner) of
                                            true -> {true, Id};
                                            false -> false
                                        end;
                                    _ -> false
                                end
                        catch _:_ -> false end;
                    _ -> false
                end
            end, lists:sort(Names)),
            {ok, lists:sublist(Ids, Limit)};
        {error, E} -> unavailable(E)
    end.

stored({record, V, G, C, _, D, _}) -> {stored, V, G, C, D}.

owns({owner, Token, _, _}, G, G, Token) -> true;
owns(_, _, _, _) -> false.

alive(none) -> false;
alive({owner, _, Incarnation, Pid}) ->
    case Incarnation =:= incarnation() of
        true -> is_process_alive(list_to_pid(binary_to_list(Pid)));
        false -> false
    end.

incarnation() ->
    case persistent_term:get({?MODULE, incarnation}, undefined) of
        undefined ->
            Value = iolist_to_binary([os:getpid(), "-",
                integer_to_binary(erlang:system_time(nanosecond))]),
            persistent_term:put({?MODULE, incarnation}, Value),
            Value;
        Value -> Value
    end.

read(Path) ->
    case file:read_file(Path) of
        {ok, Data} ->
            try binary_to_term(Data, [safe]) of
                {record, V, G, C, P, D, Owner} = R
                  when is_integer(V), V >= 0, is_integer(G), G >= 0, is_boolean(C),
                       (P =:= pending orelse P =:= suspended orelse P =:= finished),
                       is_binary(D) ->
                    case Owner of
                        none -> {ok, R};
                        {owner, T, I, Pid} when is_binary(T), is_binary(I), is_binary(Pid) -> {ok, R};
                        _ -> {error, corrupt}
                    end;
                _ -> {error, corrupt}
            catch _:_ -> {error, corrupt} end;
        {error, enoent} -> {error, not_found};
        {error, E} -> unavailable(E)
    end.

mutate(Path, Run) ->
    Lock = {{?MODULE, Path}, self()},
    Deadline = erlang:monotonic_time(millisecond) + 5000,
    case acquire(Lock, Deadline) of
        true -> try Run(Path) after global:del_lock(Lock, [node()]) end;
        false -> {error, busy}
    end.

acquire(Lock, Deadline) ->
    case global:set_lock(Lock, [node()], 0) of
        true -> true;
        false ->
            case erlang:monotonic_time(millisecond) >= Deadline of
                true -> false;
                false -> timer:sleep(5), acquire(Lock, Deadline)
            end
    end.

write(Path, Record) ->
    Suffix = integer_to_binary(erlang:system_time(nanosecond)),
    Unique = integer_to_binary(erlang:unique_integer([positive])),
    Temp = <<(unicode:characters_to_binary(Path))/binary, ".", Suffix/binary, ".", Unique/binary, ".tmp">>,
    case file:open(Temp, [write, binary, exclusive]) of
        {ok, F} ->
            Result = case file:write(F, term_to_binary(Record)) of
                ok -> file:sync(F);
                Error -> Error
            end,
            file:close(F),
            Saved = case Result of ok -> file:rename(Temp, Path); Other -> Other end,
            file:delete(Temp),
            case Saved of ok -> ok; {error, E} -> unavailable(E) end;
        {error, E} -> unavailable(E)
    end.

unavailable(E) -> {error, {unavailable, atom_to_binary(E)}}.
