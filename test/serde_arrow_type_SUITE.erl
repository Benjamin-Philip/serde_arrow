-module(serde_arrow_type_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [
        bit_length,
        byte_length,
        serialize
    ].

bit_length(_Config) ->
    ?assertEqual(serde_arrow_type:bit_length({s, 8}), 8),
    ?assertEqual(serde_arrow_type:bit_length({u, 8}), 8),
    ?assertEqual(serde_arrow_type:bit_length({f, 32}), 32),

    ?assertEqual(serde_arrow_type:bit_length(arrow_boolean), 1).

byte_length(_Config) ->
    ?assertEqual(serde_arrow_type:byte_length({s, 8}), 1),
    ?assertEqual(serde_arrow_type:byte_length({u, 8}), 1),
    ?assertEqual(serde_arrow_type:byte_length({f, 32}), 4),

    %% This case is stub for the current function's stub implementation
    ?assertEqual(serde_arrow_type:byte_length(arrow_boolean), 1).

serialize(_Config) ->
    ?assertEqual(serde_arrow_type:serialize(1, {s, 64}), <<1:64/little-signed-integer>>),
    ?assertEqual(serde_arrow_type:serialize(1, {u, 64}), <<1:64/little-unsigned-integer>>),
    ?assertEqual(serde_arrow_type:serialize(1, {f, 64}), <<1.0:64/little-float>>).
