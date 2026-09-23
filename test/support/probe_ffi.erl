-module(probe_ffi).

-export([ensure/2, mailbox_length/0, flush_mailbox/0, native_throw/0]).

%% Runs `Body`, always running `After` afterwards — even if `Body` raises —
%% then re-raises the original exception (if any). Used by `with_run` so a
%% failing test assertion never leaks a blocked coordinator or gate process.
ensure(Body, After) ->
    try
        Body()
    after
        After()
    end.

%% The calling process's own mailbox length, for asserting that a monitor's
%% `Down` message never leaks into a test process's mailbox after
%% `run`/`await` (see `saga/execution`'s demonitor-with-flush contract).
mailbox_length() ->
    {message_queue_len, N} = erlang:process_info(self(), message_queue_len),
    N.

%% Drains every message currently in the calling process's mailbox, so a
%% test can start from a known-empty baseline.
flush_mailbox() ->
    receive
        _ -> flush_mailbox()
    after 0 ->
        ok
    end.

%% Raises a native `throw`, for asserting that a step body's `throw` is
%% reported with `ThrowClass`, not folded into `ErrorClass`.
native_throw() ->
    throw(native_throw_boom).
