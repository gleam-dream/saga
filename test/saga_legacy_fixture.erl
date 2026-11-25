-module(saga_legacy_fixture).
-export([downgrade/1, shape/1, held_as_saved_before/1]).

%% Rewrites a format 2 checkpoint with no correlation as the format 1 record
%% that earlier releases saved: the same fields, without the correlation.
downgrade(Bytes) ->
    {envelope,2,Ref,Stamp,Input,Snapshot,Outcome,Issue,none} = binary_to_term(Bytes),
    term_to_binary({envelope,1,Ref,Stamp,Input,Snapshot,Outcome,Issue}).

%% The format and the saved correlation of a checkpoint.
shape(Bytes) ->
    case binary_to_term(Bytes) of
        {envelope,Format,_,_,_,_,_,_} -> {Format,none};
        {envelope,Format,_,_,_,_,_,_,Correlation} -> {Format,Correlation}
    end.

%% Rewrites a held outcome as earlier releases saved it: the encoded error
%% alone, where the current format saves the returned failure.
held_as_saved_before(Bytes) ->
    {envelope,F,Ref,Stamp,Input,Snapshot,{some,{unresolved,Step,{returned,Error},Settlement}},Issue,Correlation} =
        binary_to_term(Bytes),
    term_to_binary({envelope,F,Ref,Stamp,Input,Snapshot,{some,{unresolved,Step,Error,Settlement}},Issue,Correlation}).
