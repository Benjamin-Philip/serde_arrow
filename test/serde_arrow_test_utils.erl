-module(serde_arrow_test_utils).
-export([byte_buffer/1]).

%% This function returns a buffer of type byte, given a binary as input
byte_buffer(Binary) ->
    serde_arrow_buffer:from_erlang(Binary, {bin, undefined}).
