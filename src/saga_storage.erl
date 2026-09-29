-module(saga_storage).
-export([memory_new/0,memory_close/1,memory_create/2,memory_load/1,memory_claim/1,
         memory_commit/5,memory_release/2,memory_cancel/1,
         file_create/2,file_load/1,file_claim/1,file_commit/5,file_release/2,file_cancel/1]).

memory_new() -> spawn(fun() -> loop(none, none) end).
memory_close(Pid) -> exit(Pid, shutdown), nil.
memory_create(Pid, Data) -> call(Pid, {create,Data}).
memory_load(Pid) -> call(Pid, load).
memory_claim(Pid) -> call(Pid, claim).
memory_commit(Pid,T,R,C,D) -> call(Pid,{commit,T,R,C,D}).
memory_release(Pid,T) -> call(Pid,{release,T}).
memory_cancel(Pid) -> call(Pid,cancel).
call(Pid,Request) ->
    Ref=monitor(process,Pid), Pid ! {self(),Ref,Request},
    receive {Ref,Reply} -> demonitor(Ref,[flush]), Reply;
            {'DOWN',Ref,process,Pid,_} -> {error,{io,<<"storage closed">>}}
    end.
loop(Record, Owner) ->
    receive
        {'DOWN',Ref,process,_,_} ->
            case Owner of {_,Ref} -> loop(Record,none); _ -> loop(Record,Owner) end;
        {From,Ref,Request} ->
            {Reply,Next,NextOwner}=request(Request,From,Record,Owner),
            From ! {Ref,Reply}, loop(Next,NextOwner)
    end.
request({create,D},_,none,O) -> R={record,0,0,false,D}, {{ok,R},R,O};
request({create,_},_,R,O) -> {{error,already_exists},R,O};
request(_,_,none,O) -> {{error,not_found},none,O};
request(load,_,R,O) -> {{ok,R},R,O};
request(claim,From,R,O) ->
    case owner_alive(O) of
        true -> {{error,busy},R,O};
        false -> Next=setelement(3,R,element(3,R)+1),
                 {{ok,Next},Next,{From,monitor(process,From)}}
    end;
request({commit,T,V,C,D},From,R,O) ->
    case O of
        {From,_} -> case check(R,T,V,C) of
            ok -> Next={record,V+1,T,C,D}, {{ok,Next},Next,O};
            Error -> {Error,R,O}
        end;
        _ -> {{error,stale_owner},R,O}
    end;
request({release,T},From,R,{From,Ref}=O) ->
    case element(3,R)=:=T of
        true -> demonitor(Ref,[flush]), {{ok,nil},R,none};
        false -> {{error,stale_owner},R,O}
    end;
request({release,_},_,R,O) -> {{error,stale_owner},R,O};
request(cancel,_,R,O) -> {{ok,nil},setelement(4,R,true),O}.
owner_alive(none) -> false;
owner_alive({Pid,_}) -> is_process_alive(Pid).
check({record,V,T,C,_},T,V,C) -> ok;
check({record,_,G,_,_},T,_,_) when G =/= T -> {error,stale_owner};
check({record,_,_,C,_},_,_,Observed) when C =/= Observed -> {error,cancellation_changed};
check(_,_,_,_) -> {error,conflict}.

file_create(Path,Data) -> mutate(Path,fun() ->
    case file_load(Path) of
        {error,not_found} -> write(Path,{record,0,0,false,Data});
        {ok,_} -> {error,already_exists}; Error -> Error
    end end).
file_load(Path) ->
    case file:read_file(Path) of
        {ok,Data} -> try binary_to_term(Data,[safe]) of
            {record,V,G,C,D}=R when is_integer(V), V>=0, is_integer(G), G>=0,
                                     is_boolean(C), is_binary(D) -> {ok,R};
            _ -> {error,corrupt}
        catch _:_ -> {error,corrupt} end;
        {error,enoent} -> {error,not_found}; {error,E} -> io_error(E)
    end.
file_claim(Path) ->
    case get({?MODULE,Path}) of
        undefined -> file_claim_unowned(Path);
        _ -> {error,busy}
    end.
file_claim_unowned(Path) ->
    Lock={{?MODULE,owner,Path},self()},
    case global:set_lock(Lock,[node()],0) of
        false -> {error,busy};
        true -> Result=mutate(Path,fun() ->
                    case file_load(Path) of
                        {ok,R} -> write(Path,setelement(3,R,element(3,R)+1));
                        Error -> Error
                    end end),
                case Result of
                    {ok,R} -> put({?MODULE,Path},element(3,R));
                    _ -> global:del_lock(Lock,[node()])
                end, Result
    end.
file_commit(Path,T,V,C,D) ->
    case get({?MODULE,Path}) of
        T -> mutate(Path,fun() -> case file_load(Path) of
            {ok,R} -> case check(R,T,V,C) of
                ok -> write(Path,{record,V+1,T,C,D}); Error -> Error end;
            Error -> Error end end);
        _ -> {error,stale_owner}
    end.
file_release(Path,T) ->
    case get({?MODULE,Path}) of
        T -> erase({?MODULE,Path}), global:del_lock({{?MODULE,owner,Path},self()},[node()]), {ok,nil};
        _ -> {error,stale_owner}
    end.
file_cancel(Path) -> mutate(Path,fun() -> case file_load(Path) of
    {ok,R} -> case write(Path,setelement(4,R,true)) of {ok,_} -> {ok,nil}; Error -> Error end;
    Error -> Error end end).
mutate(Path,Run) ->
    Lock={{?MODULE,mutation,Path},self()},
    case global:set_lock(Lock,[node()],infinity) of
        true -> try Run() after global:del_lock(Lock,[node()]) end;
        false -> {error,busy}
    end.
write(Path,Record) ->
    Suffix=integer_to_binary(erlang:system_time(nanosecond)),
    Unique=integer_to_binary(erlang:unique_integer([positive])),
    Temp= <<Path/binary,".",Suffix/binary,".",Unique/binary,".tmp">>,
    case file:open(Temp,[write,binary,exclusive]) of
        {ok,F} ->
            Result=case file:write(F,term_to_binary(Record)) of
                ok -> file:sync(F); Error -> Error end,
            file:close(F),
            Saved=case Result of ok -> file:rename(Temp,Path); Other -> Other end,
            file:delete(Temp),
            case Saved of ok -> {ok,Record}; {error,E} -> io_error(E) end;
        {error,E} -> io_error(E)
    end.
io_error(E) -> {error,{io,atom_to_binary(E)}}.
