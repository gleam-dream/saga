-module(local_key_test_ffi).

-export([first_keys_of_fresh_vms/1]).

%% Boots `Count` fresh Erlang VMs one after another, as a restarted
%% application would, and returns the idempotency key of the first local run
%% in each. The VMs share this VM's code path and are not distributed.
first_keys_of_fresh_vms(Count) ->
    [first_key_of_fresh_vm() || _ <- lists:seq(1, Count)].

first_key_of_fresh_vm() ->
    Paths = lists:append([["-pa", Path] || Path <- code:get_path()]),
    Peer =
        case peer:start_link(#{connection => standard_io, args => Paths}) of
            {ok, Pid} -> Pid;
            {ok, Pid, _Node} -> Pid
        end,
    try
        peer:call(Peer, local_key_test, first_local_key, [], 30000)
    after
        peer:stop(Peer)
    end.
