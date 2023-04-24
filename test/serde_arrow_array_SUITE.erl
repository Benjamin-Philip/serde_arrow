-module(serde_arrow_array_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("serde_arrow_buffer.hrl").

all() ->
    [
        valid_layout_on_access,
        valid_type_on_access,
        valid_len_on_access,
        valid_null_count_on_access,
        valid_validity_bitmap_on_access,
        valid_offsets_on_access,
        valid_data_on_access
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Array Data and Metadata Access Tests %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

valid_layout_on_access(_Config) ->
    Array = serde_arrow_primitive_array:new([1, 2, 3], {s, 8}),
    ?assertEqual(serde_arrow_array:layout(Array), primitive).

valid_type_on_access(_Config) ->
    Array = serde_arrow_primitive_array:new([1, 2, 3], {s, 8}),
    ?assertEqual(serde_arrow_array:type(Array), {s, 8}).

valid_len_on_access(_Config) ->
    Array = serde_arrow_primitive_array:new([1, 2, 3], {s, 8}),
    ?assertEqual(serde_arrow_array:len(Array), 3).

valid_null_count_on_access(_Config) ->
    Array = serde_arrow_primitive_array:new([1, 2, 3], {s, 8}),
    ?assertEqual(serde_arrow_array:null_count(Array), 0).

valid_validity_bitmap_on_access(_Config) ->
    Array = serde_arrow_primitive_array:new([1, 2, 3], {s, 8}),
    ?assertEqual(serde_arrow_array:validity_bitmap(Array), undefined).

valid_offsets_on_access(_Config) ->
    Array = serde_arrow_primitive_array:new([1, 2, 3], {s, 8}),
    ?assertEqual(serde_arrow_array:offsets(Array), undefined).

valid_data_on_access(_Config) ->
    Array = serde_arrow_primitive_array:new([1, 2, 3], {s, 8}),
    ?assertEqual((serde_arrow_array:data(Array))#buffer.data, <<1, 2, 3, 0:(61 * 8)>>).
