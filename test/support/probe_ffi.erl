-module(probe_ffi).

-export([ensure/2]).

%% Runs `Body`, always running `After` afterwards — even if `Body` raises —
%% then re-raises the original exception (if any). Used by `with_run` so a
%% failing test assertion never leaks a blocked coordinator or gate process.
ensure(Body, After) ->
    try
        Body()
    after
        After()
    end.
