-module(serde_arrow_variable_list_array_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("serde_arrow_array.hrl").

all() ->
    [
        valid_layout_on_new,
        valid_type_on_new,
        valid_len_on_new,
        valid_element_len_on_new,
        valid_null_count_on_new,
        valid_validity_bitmap_on_new,
        valid_offsets_on_new,
        valid_data_on_new,
        valid_nested_data_on_new,
        crashes_on_invalid_data,

        %% Behaviour Adherence
        new_callback
    ].

valid_layout_on_new(_Config) ->
    Array = array([[1, 2, 3]], {s, 8}),
    ?assertEqual(Array#array.layout, variable_list).

valid_type_on_new(_Config) ->
    Array1 = array([[1, 2, 3]], {s, 8}),
    ?assertEqual(Array1#array.type, {s, 8}),

    %% Normalizes the type
    Array2 = array([[1, 2, 3]], s8),
    ?assertEqual(Array2#array.type, {s, 8}).

valid_len_on_new(_Config) ->
    Array = array([[1], [2], [3]], {s, 8}),
    ?assertEqual(Array#array.len, 3).

valid_element_len_on_new(_Config) ->
    Array = array([[1, 2], [3, 4]], {s, 8}),
    ?assertEqual(Array#array.element_len, undefined).

valid_null_count_on_new(_Config) ->
    Array1 = array([[1], [2], [3]], {s, 8}),
    ?assertEqual(Array1#array.null_count, 0),

    Array2 = array([[1], undefined, [2], [3]], {s, 8}),
    ?assertEqual(Array2#array.null_count, 1),

    Array3 = array([[1], nil, [2], [3]], {s, 8}),
    ?assertEqual(Array3#array.null_count, 1),

    Array4 = array([[1], undefined, nil, [2], [3]], {s, 8}),
    ?assertEqual(Array4#array.null_count, 2),

    Array5 = array([[1], undefined, [nil], [2], [3]], {s, 8}),
    ?assertEqual(Array5#array.null_count, 1).

valid_validity_bitmap_on_new(_Config) ->
    %% Does not allocate bitmap on no nulls
    Array1 = array([[1], [2], [3]], {s, 8}),
    ?assertEqual(Array1#array.validity_bitmap, undefined),

    %% Respects both Erlang's and Elixir's conventions
    Array2 = array([[1], undefined, [2], [3]], {s, 8}),
    ?assertEqual(
        Array2#array.validity_bitmap,
        serde_arrow_test_utils:byte_buffer(<<0:1, 0:1, 0:1, 0:1, 1:1, 1:1, 0:1, 1:1>>)
    ),

    Array3 = array([[1], nil, [2], [3]], {s, 8}),
    ?assertEqual(
        Array3#array.validity_bitmap,
        serde_arrow_test_utils:byte_buffer(<<0:1, 0:1, 0:1, 0:1, 1:1, 1:1, 0:1, 1:1>>)
    ),

    Array4 = array([[1], undefined, nil, [2], [3]], {s, 8}),
    ?assertEqual(
        Array4#array.validity_bitmap,
        serde_arrow_test_utils:byte_buffer(<<0:1, 0:1, 0:1, 1:1, 1:1, 0:1, 0:1, 1:1>>)
    ),

    %% Correctly validates on input greater than 8 elements
    Array5 = array(
        [[1], [2], undefined, [4], [5], [6], [7], [8], nil, [10]], {s, 8}
    ),
    ?assertEqual(
        Array5#array.validity_bitmap,
        serde_arrow_test_utils:byte_buffer(
            <<1:1, 1:1, 1:1, 1:1, 1:1, 0:1, 1:1, 1:1, 0:1, 0:1, 0:1, 0:1, 0:1, 0:1, 1:1, 0:1>>
        )
    ).

valid_offsets_on_new(_Config) ->
    Array1 = array([[1, 2], [3], undefined, [4], nil, [5]], s8),
    Buffer1 = serde_arrow_buffer:from_erlang([0, 2, 3, 3, 4, 4, 5], {s, 32}),
    ?assertEqual(Array1#array.offsets, Buffer1),

    %% Nested Offset
    Array2 = array(
        [[[1, 2], [3, 4]], [[5, 6, 7], nil, [8]], [[9, 11]]], {variable_list, s8, undefined}
    ),
    Buffer2 = serde_arrow_buffer:from_erlang([0, 4, 8, 10], {s, 32}),
    ?assertEqual(Array2#array.offsets, Buffer2),

    Array3 = array([[[1, 2], [3, 4]], [[5, 6], [6, 7]]], {fixed_list, s8, 4}),
    Buffer3 = serde_arrow_buffer:from_erlang([0, 4, 8], {s, 32}),
    ?assertEqual(Array3#array.offsets, Buffer3).

valid_data_on_new(_Config) ->
    %% Works without any nulls
    Array1 = array([[1, 2], [3, 4, 5]], {s, 8}),
    Data1 = primitive([1, 2, 3, 4, 5]),
    ?assertEqual(Array1#array.data, Data1),

    %% Works with undefined and nil
    Array2 = array([[1, 2], undefined, [3, 4, 5]], {s, 8}),
    Data2 = primitive([1, 2, 3, 4, 5]),
    ?assertEqual(Array2#array.data, Data2),

    Array3 = array([[1, 2], nil, [3, 4, 5]], {s, 8}),
    Data3 = Data2,
    ?assertEqual(Array3#array.data, Data3),

    Array4 = array([[1, 2], undefined, nil, [3, 4, 5]], {s, 8}),
    Data4 = primitive([1, 2, 3, 4, 5]),
    ?assertEqual(Array4#array.data, Data4),

    %% Works with undefined in deepest level of nesting
    Array5 = array([[1, 2, undefined], [nil, 3, 4, 5]], {s, 8}),
    Data5 = primitive([1, 2, undefined, nil, 3, 4, 5]),
    ?assertEqual(Array5#array.data, Data5).

valid_nested_data_on_new(_Config) ->
    %% Works without any nulls
    Array1 = array([[[1, 2], [3, 4, 5]], [[6], [7, 8, 9]]], {variable_list, s8, undefined}),
    Data1 = array([[1, 2], [3, 4, 5], [6], [7, 8, 9]], s8),
    ?assertEqual(Array1#array.data, Data1),

    %% Works with undefined and nil
    Array2 = array([[[1, 2], [3, 4]], undefined, [[5, 6], [7, 8]]], {variable_list, s8, undefined}),
    Data2 = array([[1, 2], [3, 4], [5, 6], [7, 8]], s8),
    ?assertEqual(Array2#array.data, Data2),

    Array3 = array([[[1, 2], [3, 4]], nil, [[5, 6], [7, 8]]], {variable_list, s8, undefined}),
    Data3 = Data2,
    ?assertEqual(Array3#array.data, Data3),

    Array4 = array(
        [[[1, 2], [3, 4]], undefined, [[5, 6], [7, 8]], nil], {variable_list, s8, undefined}
    ),
    Data4 = array(
        [[1, 2], [3, 4], [5, 6], [7, 8]], s8
    ),
    ?assertEqual(Array4#array.data, Data4),

    %% Level 2 Nesting
    Array5 = array(
        [[[[1, 2, 3], [4, 5]]], [[[6, 7, 8], [9, 10]]]],
        {variable_list, {variable_list, s8, undefined}, undefined}
    ),
    Data5 = array([[[1, 2, 3], [4, 5]], [[6, 7, 8], [9, 10]]], {variable_list, s8, undefined}),
    ?assertEqual(Array5#array.data, Data5),

    %% Nesting of other layouts
    Array6 = array([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], {fixed_list, s8, 2}),
    Data6 = serde_arrow_fixed_list_array:new([[1, 2], [3, 4], [5, 6], [7, 8]], s8),
    ?assertEqual(Array6#array.data, Data6).

crashes_on_invalid_data(_Config) ->
    %% No Nesting
    ?assertError(badarg, array([1, 2, 3], s8)),

    %% Nesting in input and type do not match
    ?assertError(badarg, array([[[1, 2, 3]]], s8)),
    ?assertError(badarg, array([[1, 2, 3, 4]], {variable_list, s8, undefined})),
    ?assertError(badarg, array([[[[1, 2, 3]]]], {variable_list, s8, undefined})),

    %% Nesting between elements is inconsistent
    ?assertError(badarg, array([[1], [[2]], [[[3]]]], s8)),
    ?assertError(function_clause, array([[1], [[2]], [[[3]]]], {fixed_list, s8, 1})).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Array Behaviour Adherence Tests %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

new_callback(_Config) ->
    Array = array([[1, 2], [3, 4]], {s, 8}),
    Callback = array([[1, 2], [3, 4]], #{type => {s, 8}}),
    ?assertEqual(Callback, Array),

    ?assertError(badarg, array([[1, 2]], #{foo => bar})).

%%%%%%%%%%%
%% Utils %%
%%%%%%%%%%%

array(Values, Type) ->
    serde_arrow_variable_list_array:new(Values, Type).

primitive(Values) ->
    serde_arrow_fixed_primitive_array:new(Values, {s, 8}).
