-module(saga_checkpoint).
-export([encode/4,decode/4,header/1]).

header(Bytes) ->
    try binary_to_term(Bytes,[safe]) of
        {envelope,1,Ref,Stamp,_,_,_,_} when is_binary(Ref),is_binary(Stamp) -> {ok,{Ref,Stamp}};
        {envelope,2,Ref,Stamp,_,_,_,_,_} when is_binary(Ref),is_binary(Stamp) -> {ok,{Ref,Stamp}};
        _ -> {error,nil}
    catch _:_ -> {error,nil} end.

%% The schema is the full wire contract, including every nested variant.
%% Only application output/error fields cross the supplied checked codecs.
%% Neither arbitrary native terms nor executable runtime state are accepted.
encode(Value,O,E,U) ->
    try walk(Value,envelope,{encode,O,E,U}) of
        Encoded -> {ok,term_to_binary(Encoded)}
    catch throw:{codec,Type,Reason} -> {error,{codec_failure,boundary(Type),Reason}}; _:_ -> {error,{invalid_state,malformed}} end.
decode(Bytes,O,E,U) ->
    try
        Value=upgrade(binary_to_term(Bytes,[safe])),
        Decoded=walk(Value,envelope,{decode,O,E,U}),
        validate(Decoded),
        {ok,Decoded}
    catch throw:{codec,Type,Reason} -> {error,{codec_failure,boundary(Type),Reason}}; _:_ -> {error,{invalid_state,malformed}} end.

%% Format 1 had no correlation: it reads as a format 1 envelope with none saved.
upgrade({envelope,1,Ref,Stamp,Input,Snapshot,Outcome,Issue}) ->
    {envelope,1,Ref,Stamp,Input,Snapshot,Outcome,Issue,none};
upgrade(Value) -> Value.

boundary(output) -> run_output;
boundary(error) -> run_error;
boundary(undo) -> run_undo_error.

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
        {error,Reason} -> throw({codec,Type,Reason})
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

schema(envelope) -> [{envelope,[int,string,string,string,{option,snapshot},{option,outcome},{option,checkpoint_failure},{option,string}]}];
schema(checkpoint_failure) -> [{storage_failure,[storage_error]}, {codec_failure,[boundary,codec_error]},
    {invalid_state,[problem]}, {uncertain,[required]}, {too_large,[natural,natural]}];
schema(storage_error) -> [{not_found,[]},{already_exists,[]},{busy,[]},{conflict,[]},{stale_owner,[]},
    {cancellation_changed,[]},{corrupt,[]},{unavailable,[string]},{timed_out,[]}];
schema(boundary) -> [{run_input,[]},{run_output,[]},{run_error,[]},{run_undo_error,[]},
    {step_input,[saved_address]},{step_output,[saved_address]}];
schema(saved_address) -> [{address,[{list,string},string,natural]}];
schema(codec_error) -> [{encode_failed,[string]},{decode_failed,[string]},{json_decode_failed,[json_error]},
    {round_trip_failed,[codec_error]},{codec_raised,[string]}];
schema(json_error) -> [{unexpected_end_of_input,[]},{unexpected_byte,[string]},{unexpected_sequence,[string]},
    {unable_to_decode,[{list,decode_error}]}];
schema(decode_error) -> [{decode_error,[string,string,{list,string}]}];
schema(problem) -> [{malformed,[]},{foreign_execution,[string]},{graph_mismatch,[]},
    {concurrency_below_in_flight,[natural,natural]},{undo_not_restorable,[saved_address]},
    {compensation_input_missing,[saved_address]},{decider_missing_after_mapping,[saved_address]}];
schema(required) -> [{required,[saved_address,required_action,key]}];
schema(required_action) -> [{attempt_action,[natural]},{compensation_action,[natural]},{undo_action,[]}];
schema(key) -> [{key,[string,natural,string]}];
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
schema(ending) -> [{action_crashed,[crash]},{action_timed_out,[]},{action_interrupted,[]},{action_returned_unknown,[]}].

validate({envelope,F,_,_,_,_,_,_,_}=Envelope) when F=:=1;F=:=2 -> validate_body(Envelope).

validate_body({envelope,_,_,_,_,none,none,_,_}) -> ok;
validate_body({envelope,_,_,_,_,{some,{snapshot,Nodes,Journal,Phase,Failures,_,_}},_,_,_}) ->
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
