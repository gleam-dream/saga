-module(saga_checkpoint_fixture).
-export([shape/1]).

%% The format and the saved correlation of a checkpoint's run record.
shape(Bytes) ->
    {header,_,{envelope,Format,_,_,_,_,_,_,Correlation}} = binary_to_term(Bytes),
    {Format,Correlation}.
