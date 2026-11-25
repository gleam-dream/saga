-module(saga_test_heap).
-export([total_heap_words/0]).

%% The calling process's total heap size, in words.
total_heap_words() ->
    {total_heap_size, Words} = erlang:process_info(self(), total_heap_size),
    Words.
