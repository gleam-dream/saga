-module(cleanup_ffi).

-export([ensure/2]).

%% Runs `Body`, always running `After` afterwards — even if `Body` raises —
%% then re-raises the original exception (if any). This is the external
%% consumer's own try/after cleanup helper: it must not import saga's
%% internal test support, so it is a small, independent copy of the same
%% pattern saga's own tests use (see saga's test/support/probe.gleam).
ensure(Body, After) ->
    try
        Body()
    after
        After()
    end.
