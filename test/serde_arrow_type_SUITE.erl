-module(serde_arrow_type_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() -> [deserialize_int_8, deserialize_int_16, deserialize_int_32, deserialize_int_64].

deserialize_int_8(_Config) ->
    ArrowInteger = <<-72:1/integer-signed-little-unit:8>>,
    ErlangInteger = serde_arrow_type:deserialize(ArrowInteger, {int, 8}),
    ?assertEqual(ErlangInteger, -72).

deserialize_int_16(_Config) ->
    ArrowInteger = <<-32678:1/integer-signed-little-unit:16>>,
    ErlangInteger = serde_arrow_type:deserialize(ArrowInteger, {int, 16}),
    ?assertEqual(ErlangInteger, -32678).

deserialize_int_32(_Config) ->
    ArrowInteger = <<-2_147_483_648:1/integer-signed-little-unit:32>>,
    ErlangInteger = serde_arrow_type:deserialize(ArrowInteger, {int, 32}),
    ?assertEqual(ErlangInteger, -2_147_483_648).

deserialize_int_64(_Config) ->
    ArrowInteger = <<-9_223_372_036_854_775_808:1/integer-signed-little-unit:64>>,
    ErlangInteger = serde_arrow_type:deserialize(ArrowInteger, {int, 64}),
    ?assertEqual(ErlangInteger, -9_223_372_036_854_775_808).
