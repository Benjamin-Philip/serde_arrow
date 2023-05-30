-module(serde_arrow_offsets_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("serde_arrow_buffer.hrl").

all() ->
    [
        valid_offset_on_no_nulls,
        valid_offset_on_undefined,
        valid_offset_on_nil,
        valid_offset_on_undefined_and_nil
    ].

valid_offset_on_no_nulls(_Config) ->
    Offsets = offsets([<<1, 2>>, <<3>>, <<4, 5, 6>>]),
    Buffer = buffer([0, 2, 3, 6]),
    ?assertEqual(Offsets, Buffer).

valid_offset_on_undefined(_Config) ->
    Offsets = offsets([<<1, 2>>, <<3>>, undefined, <<4, 5, 6>>]),
    Buffer = buffer([0, 2, 3, 3, 6]),
    ?assertEqual(Offsets, Buffer).

valid_offset_on_nil(_Config) ->
    Offsets = offsets([<<1, 2>>, <<3>>, nil, <<4, 5, 6>>]),
    Buffer = buffer([0, 2, 3, 3, 6]),
    ?assertEqual(Offsets, Buffer).

valid_offset_on_undefined_and_nil(_Config) ->
    Offsets = offsets([<<1, 2>>, <<3>>, undefined, <<4, 5, 6>>, nil]),
    Buffer = buffer([0, 2, 3, 3, 6, 6]),
    ?assertEqual(Offsets, Buffer).

%%%%%%%%%%%
%% Utils %%
%%%%%%%%%%%

offsets(Values) ->
    serde_arrow_offsets:new(Values, bin).

buffer(Values) ->
    serde_arrow_buffer:from_erlang(Values, {s, 32}).
