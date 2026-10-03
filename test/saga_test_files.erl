-module(saga_test_files).
-export([fresh_directory/1, remove_directory/1, claim_from_immortal/2,
         remember_claim/2, genuine_claim/1]).

fresh_directory(Path) ->
    _ = file:del_dir_r(Path),
    ok = file:make_dir(Path),
    nil.

remove_directory(Path) ->
    _ = file:del_dir_r(Path),
    nil.

%% Claims from a process that lives forever, so the claim never ends.
claim_from_immortal(Storage, Id) ->
    Self = self(),
    spawn(fun() ->
        Self ! {claimed, 'saga@storage':do_claim(Storage, Id)},
        receive never -> ok end
    end),
    receive {claimed, Result} -> Result end.

remember_claim(Id, Claim) -> persistent_term:put({?MODULE, Id}, Claim), nil.

genuine_claim(Id) ->
    case persistent_term:get({?MODULE, Id}, undefined) of
        undefined -> {error, nil};
        Claim -> {ok, Claim}
    end.

