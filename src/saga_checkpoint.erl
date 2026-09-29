-module(saga_checkpoint).
-export([encode/4,decode/4,header/1]).

header(Bytes) ->
    try binary_to_term(Bytes,[safe]) of
        {envelope,1,Ref,Stamp,_,_,_,_} when is_binary(Ref),is_binary(Stamp) -> {ok,{Ref,Stamp}};
        _ -> {error,<<"invalid checkpoint header">>}
    catch _:_ -> {error,<<"invalid checkpoint header">>} end.

%% The schema is the full wire contract, including every nested variant.
%% Only application output/error fields cross the supplied checked codecs.
%% Neither arbitrary native terms nor executable runtime state are accepted.
encode(Value,O,E,U) ->
    try walk(Value,envelope,{encode,O,E,U}) of
        Encoded -> {ok,term_to_binary(Encoded)}
    catch throw:{codec,Reason} -> {error,{codec_failure,Reason}}; _:_ -> {error,{invalid_state,<<"invalid checkpoint">>}} end.
decode(Bytes,O,E,U) ->
    try
        Value=binary_to_term(Bytes,[safe]),
        Decoded=walk(Value,envelope,{decode,O,E,U}),
        validate(Decoded),
        {ok,Decoded}
    catch throw:{codec,Reason} -> {error,{codec_failure,Reason}}; _:_ -> {error,{invalid_state,<<"invalid checkpoint">>}} end.

walk(Value,string,_) when is_binary(Value) ->
    Value=unicode:characters_to_binary(Value), Value;
walk(Value,int,_) when is_integer(Value) -> Value;
walk(Value,natural,_) when is_integer(Value),Value>=0 -> Value;
walk(Value,{list,Type},Codecs) when is_list(Value) ->
    [walk(Item,Type,Codecs) || Item <- Value];
walk(none,{option,_},_) -> none;
walk({some,Value},{option,Type},Codecs) -> {some,walk(Value,Type,Codecs)};
walk({A,B},{pair,TA,TB},Codecs) -> {walk(A,TA,Codecs),walk(B,TB,Codecs)};
walk(Value,Type,{Mode,O,E,U}) when Type=:=output;Type=:=error;Type=:=undo ->
    case Mode of decode -> true=is_binary(Value); encode -> ok end,
    F=case Type of output -> O; error -> E; undo -> U end,
    case F(Value) of
        {ok,Result} -> case Mode of encode -> true=is_binary(Result); decode -> ok end, Result;
        {error,Reason} when is_binary(Reason) -> throw({codec,Reason})
    end;
walk(Value,Type,Codecs) ->
    {Tag,Fields}=case Value of
        Atom when is_atom(Atom) -> {Atom,[]};
        Tuple when is_tuple(Tuple),tuple_size(Tuple)>0 ->
            [Name|Args]=tuple_to_list(Tuple), {Name,Args}
    end,
    {Tag,Specs}=lists:keyfind(Tag,1,schema(Type)),
    true=length(Fields)=:=length(Specs),
    Results=[walk(V,S,Codecs) || {V,S} <- lists:zip(Fields,Specs)],
    case Results of [] -> Tag; _ -> list_to_tuple([Tag|Results]) end.

schema(envelope) -> [{envelope,[int,string,string,string,{option,snapshot},{option,outcome},{option,checkpoint_failure}]}];
schema(checkpoint_failure) -> [{storage_failure,[storage_error]}, {codec_failure,[string]}, {invalid_state,[string]}, {uncertain,[required]}];
schema(storage_error) -> [{not_found,[]},{already_exists,[]},{busy,[]},{conflict,[]},{stale_owner,[]},{cancellation_changed,[]},{corrupt,[]},{io,[string]}];
schema(required) -> [{required,[string,reconciliation_action,string]}];
schema(reconciliation_action) -> [{activity,[]},{compensation,[]},{undo,[]}];
schema(snapshot) -> [{snapshot,[{list,saved_node},{list,natural},phase,{list,{pair,natural,failure}},{list,unknown},{option,int}]}];
schema(saved_node) -> [{saved_node,[progress,{list,string},int]}];
schema(progress) -> [{waiting,[]},{attempting,[natural]},{compensating,[natural]},
    {retry_scheduled,[natural]},{succeeded,[]},{failed_step,[]},{interrupted,[]},
    {undoing,[]},{undone,[]},{undo_failed_step,[]},{skipped,[]}];
schema(phase) -> [{saved_running,[]},{saved_settling,[trigger,settlement]},{saved_rollback,[trigger,settlement]}];
schema(trigger) -> [{trigger_failure,[cause]},{trigger_unresolved,[address,error]},{trigger_cancel,[cancel]}];
schema(cancel) -> [{cancel_requested,[]},{owner_exited,[]}];
schema(outcome) -> [{completed,[output]},{completed_with_unknown_effects,[output,{list,unknown}]},
    {failed,[cause,settlement]},{cancelled,[cancel,settlement]},{unresolved,[address,error,settlement]}];
schema(cause) -> [{step_failed,[address,error]},{step_crashed,[address,crash]},
    {step_timed_out,[address]},{retry_limit_reached,[address,failure]},
    {retry_superseded,[address,failure]},{output_crashed,[crash]},{deadline_exceeded,[]}];
schema(failure) -> [{returned,[error]},{crashed,[crash]},{timed_out,[]}];
schema(crash) -> [{crash,[crash_class,string]}];
schema(crash_class) -> [{error_class,[]},{exit_class,[]},{throw_class,[]}];
schema(address) -> [{step_address,[{list,string},string,natural]}];
schema(settlement) -> [{settlement,[{list,address},{list,undo_failure},{list,address},
    {list,address},{list,address},{list,compensation_failure},{list,cause},{list,unknown}]}];
schema(undo_failure) -> [{undo_failed,[address,undo]},{undo_crashed,[address,crash]},{undo_timed_out,[address]}];
schema(compensation_failure) -> [{cleanup_failed,[address,undo]},
    {compensation_crashed,[address,crash]},{compensation_timed_out,[address]}];
schema(unknown) -> [{unknown_effect,[address,action,ending]}];
schema(action) -> [{step_attempt,[natural]},{step_compensation,[natural]},{step_undo,[]}];
schema(ending) -> [{action_crashed,[crash]},{action_timed_out,[]},{action_interrupted,[]}].

validate({envelope,1,_,_,_,none,none,_}) -> ok;
validate({envelope,1,_,_,_,{some,{snapshot,Nodes,Journal,Phase,Failures,_,_}},_,_}) ->
    N=length(Nodes),
    true=lists:all(fun(I) -> I<N end,Journal),
    true=length(Journal)=:=length(lists:usort(Journal)),
    true=lists:all(fun({I,_}) -> I<N end,Failures),
    true=length(Failures)=:=length(lists:ukeysort(1,Failures)),
    true=length([ok || {saved_node,undoing,_,_} <- Nodes])=<1,
    lists:foreach(fun({saved_node,S,V,_}) ->
        case S of
            succeeded -> [_Input,_Output,Kind]=V, true=(Kind=:=<<"undo">> orelse Kind=:=<<"none">>);
            undoing -> [_Input,_Output,Kind]=V, true=(Kind=:=<<"undo">> orelse Kind=:=<<"none">>);
            undone -> [_Input,_Output,Kind]=V, true=(Kind=:=<<"undo">> orelse Kind=:=<<"none">>);
            undo_failed_step -> [_Input,_Output,Kind]=V, true=(Kind=:=<<"undo">> orelse Kind=:=<<"none">>);
            {attempting,A} -> true=A>0, true=length(V)=<1;
            {compensating,A} -> true=A>0, true=length(V)=<1;
            {retry_scheduled,A} -> true=A>0, []=V;
            _ -> []=V
        end
    end,Nodes),
    lists:foreach(fun(I) -> {saved_node,succeeded,_,_}=lists:nth(I+1,Nodes) end,Journal),
    case Phase of saved_running -> []=[ok || {saved_node,undoing,_,_} <- Nodes]; _ -> ok end,
    ok.
