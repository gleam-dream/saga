-module(saga_test_panic).

-export([message/1]).

%% Runs `Fun` and returns the message of the Gleam panic it raises, or
%% `{error, nil}` when it returns.
message(Fun) ->
    try
        Fun(),
        {error, nil}
    catch
        error:#{gleam_error := panic, message := Message} -> {ok, Message}
    end.
