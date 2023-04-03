-module(serde_arrow_primitive_array_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("serde_arrow_primitive_array.hrl").

all() ->
    [
        valid_len_on_new,
        valid_null_count_on_new,
        valid_validity_bitmap_on_new,
        valid_len_on_access,
        valid_null_count_on_access,
        valid_validity_bitmap_on_access
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Primitive Array Creation Tests %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

valid_len_on_new(_Config) ->
    Array = serde_arrow_primitive_array:new([1, 2, 3], {s, 8}),
    ?assertEqual(Array#primitive_array.len, 3).

valid_null_count_on_new(_Config) ->
    Array1 = serde_arrow_primitive_array:new([1, 2, 3], {s, 8}),
    ?assertEqual(Array1#primitive_array.null_count, 0),

    Array2 = serde_arrow_primitive_array:new([1, undefined, 2, 3], {s, 8}),
    ?assertEqual(Array2#primitive_array.null_count, 1),

    Array3 = serde_arrow_primitive_array:new([1, nil, 2, 3], {s, 8}),
    ?assertEqual(Array3#primitive_array.null_count, 1),

    Array4 = serde_arrow_primitive_array:new([1, undefined, nil, 2, 3], {s, 8}),
    ?assertEqual(Array4#primitive_array.null_count, 2).

valid_validity_bitmap_on_new(_Config) ->
    Array1 = serde_arrow_primitive_array:new([1, 2, 3], {s, 8}),
    ?assertEqual(Array1#primitive_array.validity_bitmap, undefined),

    Array2 = serde_arrow_primitive_array:new([1, undefined, 2, 3], {s, 8}),
    ?assertEqual(Array2#primitive_array.validity_bitmap, [
        <<0:1, 0:1, 0:1, 0:1, 1:1, 1:1, 0:1, 1:1>>
    ]),

    Array3 = serde_arrow_primitive_array:new([1, nil, 2, 3], {s, 8}),
    ?assertEqual(Array3#primitive_array.validity_bitmap, [
        <<0:1, 0:1, 0:1, 0:1, 1:1, 1:1, 0:1, 1:1>>
    ]),

    Array4 = serde_arrow_primitive_array:new([1, nil, undefined, 3], {s, 8}),
    ?assertEqual(Array4#primitive_array.validity_bitmap, [
        <<0:1, 0:1, 0:1, 0:1, 1:1, 0:1, 0:1, 1:1>>
    ]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Primitive Array Data and Metadata Access Tests %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

valid_len_on_access(_Config) ->
    Array = serde_arrow_primitive_array:new([1, 2, 3], {s, 8}),
    ?assertEqual(serde_arrow_primitive_array:len(Array), 3).

valid_null_count_on_access(_Config) ->
    Array = serde_arrow_primitive_array:new([1, 2, 3], {s, 8}),
    ?assertEqual(serde_arrow_primitive_array:null_count(Array), 0).

valid_validity_bitmap_on_access(_Config) ->
    Array = serde_arrow_primitive_array:new([1, 2, 3], {s, 8}),
    ?assertEqual(serde_arrow_primitive_array:validity_bitmap(Array), undefined).
