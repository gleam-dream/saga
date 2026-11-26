-module(saga_storage).
-export([file_create/3, file_load/2, file_claim/2, file_commit/3, file_release/2,
         file_cancel/2, file_list/2]).

%% One directory per execution: <Directory>/<hex of the id>.saga/. Its `head`
%% file holds {head, Revision, Generation, Cancelled, Phase, Owner, Index},
%% where Owner is `none` or {owner, Token, Incarnation, Pid} and Index maps
%% each record key to the file that holds its bytes, <hex of key>.<revision
%% that wrote it>. A commit writes its records to new files, then replaces
%% `head` with one rename: that rename is the commit, so a commit cut short
%% leaves only unreferenced files, which a later write replaces. A claim
%% stays while its claiming process lives in this VM incarnation; a head
%% written by an earlier VM is unowned. Every mutation holds a VM-local lock
%% on the execution for at most 5 seconds, then fails with `busy`.

dir(Dir, Id) -> filename:join(Dir, <<(binary:encode_hex(Id))/binary, ".saga">>).
head_path(Path) -> filename:join(Path, <<"head">>).

file_create(Dir, Id, Records) ->
    mutate(dir(Dir, Id), fun(Path) ->
        case read(Path) of
            {error, not_found} ->
                case ensure_dir(Path) of
                    ok ->
                        case write_records(Path, 0, Records, #{}) of
                            {ok, Index} ->
                                done(write_head(Path, {head, 0, 0, false, pending, none, Index}));
                            Error -> Error
                        end;
                    Error -> Error
                end;
            {ok, _} -> {error, already_exists};
            Error -> Error
        end
    end).

%% A load takes no lock, so it never delays a commit. A commit removes the
%% record files it replaced once its new head is in place, so a load that
%% read an earlier head can miss a file. Record files are never rewritten
%% (a name holds its key and the revision that wrote it), so the load then
%% reads the head again and reads only the files of the new head it does
%% not hold yet. A missing file under an unchanged head is corrupt.
file_load(Dir, Id) ->
    Path = dir(Dir, Id),
    case read(Path) of
        {ok, Head} -> load(Path, Head, #{});
        Error -> Error
    end.

load(Path, {head, V, G, C, _, _, Index} = Head, Held) ->
    try maps:fold(fun(_, Name, {Read, Missing}) ->
            case maps:find(Name, Held) of
                {ok, Bytes} -> {maps:put(Name, Bytes, Read), Missing};
                error ->
                    case file:read_file(filename:join(Path, Name)) of
                        {ok, Bytes} -> {maps:put(Name, Bytes, Read), Missing};
                        {error, enoent} -> {Read, true};
                        {error, E} -> throw({unavailable, E})
                    end
            end
        end, {#{}, false}, Index) of
        {Read, false} ->
            {ok, {stored, V, G, C, maps:map(fun(_, Name) -> maps:get(Name, Read) end, Index)}};
        {Read, true} ->
            case read(Path) of
                {ok, Head} -> {error, corrupt};
                {ok, Next} -> load(Path, Next, Read);
                Error -> Error
            end
    catch
        throw:{unavailable, E} -> unavailable(E)
    end.

file_claim(Dir, Id) ->
    mutate(dir(Dir, Id), fun(Path) ->
        case read(Path) of
            {ok, {head, V, G, C, P, Owner, Index}} ->
                case alive(Owner) of
                    true -> {error, busy};
                    false ->
                        Token = integer_to_binary(erlang:unique_integer([positive])),
                        Next = {head, V, G + 1, C, P,
                                {owner, Token, incarnation(), list_to_binary(pid_to_list(self()))},
                                Index},
                        case write_head(Path, Next) of
                            ok ->
                                case stored(Path, Next) of
                                    {ok, Stored} -> {ok, {{claim, Id, G + 1, Token}, Stored}};
                                    Error -> Error
                                end;
                            Error -> Error
                        end
                end;
            Error -> Error
        end
    end).

file_commit(Dir, {claim, Id, Generation, Token}, {commit, Expected, Observed, Phase, Records}) ->
    mutate(dir(Dir, Id), fun(Path) ->
        case read(Path) of
            {ok, {head, V, G, C, _, Owner, Index}} ->
                case owns(Owner, G, Generation, Token) of
                    false -> {error, stale_owner};
                    true when C =/= Observed -> {error, cancellation_changed};
                    true when V =/= Expected -> {error, conflict};
                    true ->
                        case write_records(Path, V + 1, Records, Index) of
                            {ok, Next} ->
                                case write_head(Path, {head, V + 1, G, C, Phase, Owner, Next}) of
                                    ok ->
                                        %% The replaced files are no longer referenced.
                                        [file:delete(filename:join(Path, Old))
                                         || {Key, Old} <- maps:to_list(Index),
                                            maps:get(Key, Next) =/= Old],
                                        {ok, nil};
                                    Error -> Error
                                end;
                            Error -> Error
                        end
                end;
            Error -> Error
        end
    end).

file_release(Dir, {claim, Id, Generation, Token}) ->
    mutate(dir(Dir, Id), fun(Path) ->
        case read(Path) of
            {ok, {head, V, G, C, P, Owner, Index}} ->
                case owns(Owner, G, Generation, Token) of
                    false -> {error, stale_owner};
                    true -> done(write_head(Path, {head, V, G, C, P, none, Index}))
                end;
            Error -> Error
        end
    end).

file_cancel(Dir, Id) ->
    mutate(dir(Dir, Id), fun(Path) ->
        case read(Path) of
            {ok, {head, V, G, _, P, Owner, Index}} ->
                done(write_head(Path, {head, V, G, true, P, Owner, Index}));
            Error -> Error
        end
    end).

file_list(Dir, {listing, Phase, After, Limit}) ->
    case file:list_dir(Dir) of
        {ok, Names} ->
            Ids = lists:filtermap(fun(Name) ->
                Binary = unicode:characters_to_binary(Name),
                case binary:split(Binary, <<".saga">>) of
                    [Hex, <<>>] ->
                        try binary:decode_hex(Hex) of
                            Id ->
                                case later(Id, After) andalso read(dir(Dir, Id)) of
                                    {ok, {head, _, _, _, Phase, Owner, _}} ->
                                        case alive(Owner) of
                                            false -> {true, Id};
                                            true -> false
                                        end;
                                    _ -> false
                                end
                        catch _:_ -> false end;
                    _ -> false
                end
            end, Names),
            {ok, lists:sublist(lists:sort(Ids), Limit)};
        {error, E} -> unavailable(E)
    end.

later(_, none) -> true;
later(Id, {some, After}) -> Id > After.

stored(Path, {head, V, G, C, _, _, Index}) ->
    try maps:map(fun(_, Name) ->
            case file:read_file(filename:join(Path, Name)) of
                {ok, Bytes} -> Bytes;
                {error, enoent} -> throw(corrupt);
                {error, E} -> throw({unavailable, E})
            end
        end, Index) of
        Records -> {ok, {stored, V, G, C, Records}}
    catch
        throw:corrupt -> {error, corrupt};
        throw:{unavailable, E} -> unavailable(E)
    end.

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
    case file:read_file(head_path(Path)) of
        {ok, Data} ->
            try binary_to_term(Data, [safe]) of
                {head, V, G, C, P, Owner, Index} = H
                  when is_integer(V), V >= 0, is_integer(G), G >= 0, is_boolean(C),
                       (P =:= pending orelse P =:= suspended orelse P =:= finished),
                       is_map(Index) ->
                    Valid = lists:all(fun({K, N}) -> is_binary(K) andalso is_binary(N) end,
                                      maps:to_list(Index)),
                    case {Valid, Owner} of
                        {false, _} -> {error, corrupt};
                        {true, none} -> {ok, H};
                        {true, {owner, T, I, Pid}} when is_binary(T), is_binary(I), is_binary(Pid) -> {ok, H};
                        _ -> {error, corrupt}
                    end;
                _ -> {error, corrupt}
            catch _:_ -> {error, corrupt} end;
        {error, enoent} -> {error, not_found};
        {error, enotdir} -> {error, corrupt};
        {error, E} -> unavailable(E)
    end.

ensure_dir(Path) ->
    case file:make_dir(Path) of
        ok -> ok;
        {error, eexist} -> ok;
        {error, E} -> unavailable(E)
    end.

%% Writes each record to its own file and returns the index that names them.
write_records(Path, Revision, Records, Index) ->
    Suffix = <<".", (integer_to_binary(Revision))/binary>>,
    maps:fold(fun
        (Key, Bytes, {ok, Acc}) ->
            Name = <<(binary:encode_hex(Key))/binary, Suffix/binary>>,
            case write(filename:join(Path, Name), Bytes) of
                ok -> {ok, maps:put(Key, Name, Acc)};
                Error -> Error
            end;
        (_, _, Error) -> Error
    end, {ok, Index}, Records).

write_head(Path, Head) -> write(head_path(Path), term_to_binary(Head)).

done(ok) -> {ok, nil};
done(Error) -> Error.

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

%% Writes Bytes to a synced temporary file, then renames it over Path.
write(Path, Bytes) ->
    Suffix = integer_to_binary(erlang:system_time(nanosecond)),
    Unique = integer_to_binary(erlang:unique_integer([positive])),
    Temp = <<(unicode:characters_to_binary(Path))/binary, ".", Suffix/binary, ".", Unique/binary, ".tmp">>,
    case file:open(Temp, [write, binary, exclusive]) of
        {ok, F} ->
            Result = case file:write(F, Bytes) of
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
