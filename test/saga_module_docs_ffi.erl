-module(saga_module_docs_ffi).
-export([public_sources/0]).

%% Every public module's source path, with its text. Modules under
%% src/saga/internal are excluded, matching gleam.toml's internal_modules.
public_sources() ->
    Paths = ["src/saga.gleam" | filelib:wildcard("src/saga/**/*.gleam")],
    [{unicode:characters_to_binary(Path), read(Path)}
     || Path <- lists:sort(Paths), not lists:prefix("src/saga/internal/", Path)].

read(Path) ->
    {ok, Text} = file:read_file(Path),
    Text.
