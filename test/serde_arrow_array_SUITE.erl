-module(serde_arrow_array_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("serde_arrow_buffer.hrl").

all() ->
    [
        %% creation tests
        valid_array_on_from_erlang,

        %% data and metadata tests
        valid_layout_on_access,
        valid_type_on_access,
        valid_len_on_access,
        valid_element_len_on_access,
        valid_null_count_on_access,
        valid_validity_bitmap_on_access,
        valid_offsets_on_access,
        valid_data_on_access
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Array Creation Tests %%
%%%%%%%%%%%%%%%%%%%%%%%%%%

valid_array_on_from_erlang(_Config) ->
    Given1 = serde_arrow_array:from_erlang(fixed_primitive, [1, 2, 3], #{type => {s, 8}}),
    Expected1 = serde_arrow_fixed_primitive_array:from_erlang([1, 2, 3], {s, 8}),
    ?assertEqual(Given1, Expected1),

    Given2 = serde_arrow_array:from_erlang(variable_binary, [<<1>>, <<2>>], #{}),
    Expected2 = serde_arrow_variable_binary_array:from_erlang([<<1>>, <<2>>]),
    ?assertEqual(Given2, Expected2).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Array Data and Metadata Access Tests %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

valid_layout_on_access(_Config) ->
    Array = serde_arrow_array:from_erlang(fixed_primitive, [1, 2, 3], {s, 8}),
    ?assertEqual(serde_arrow_array:layout(Array), fixed_primitive).

valid_type_on_access(_Config) ->
    Array = serde_arrow_array:from_erlang(fixed_primitive, [1, 2, 3], {s, 8}),
    ?assertEqual(serde_arrow_array:type(Array), {s, 8}).

valid_len_on_access(_Config) ->
    Array = serde_arrow_array:from_erlang(fixed_primitive, [1, 2, 3], {s, 8}),
    ?assertEqual(serde_arrow_array:len(Array), 3).

valid_element_len_on_access(_Config) ->
    Array = serde_arrow_array:from_erlang(fixed_primitive, [1, 2, 3], {s, 8}),
    ?assertEqual(serde_arrow_array:element_len(Array), undefined).

valid_null_count_on_access(_Config) ->
    Array = serde_arrow_array:from_erlang(fixed_primitive, [1, 2, 3], {s, 8}),
    ?assertEqual(serde_arrow_array:null_count(Array), 0).

valid_validity_bitmap_on_access(_Config) ->
    Array = serde_arrow_array:from_erlang(fixed_primitive, [1, 2, 3], {s, 8}),
    ?assertEqual(serde_arrow_array:validity_bitmap(Array), undefined).

valid_offsets_on_access(_Config) ->
    Array = serde_arrow_array:from_erlang(fixed_primitive, [1, 2, 3], {s, 8}),
    ?assertEqual(serde_arrow_array:offsets(Array), undefined).

valid_data_on_access(_Config) ->
    Array = serde_arrow_array:from_erlang(fixed_primitive, [1, 2, 3], {s, 8}),
    ?assertEqual(serde_arrow_array:data(Array), serde_arrow_buffer:from_erlang([1, 2, 3], {s, 8})).
