-module(serde_arrow_test_utils).
-export([byte_buffer/1, pad/1]).

%% This function returns a buffer of type bin, given a binary as input
byte_buffer(Binary) ->
    serde_arrow_buffer:from_erlang(Binary, {bin, undefined}).

%% Returns a binary with 0 padded to a certain length.
pad(ByteLen) ->
    <<<<0>> || _X <- lists:seq(1, ByteLen)>>.
