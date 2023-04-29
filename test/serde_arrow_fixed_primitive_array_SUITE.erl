-module(serde_arrow_fixed_primitive_array_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("serde_arrow_array.hrl").

all() ->
    [
        valid_layout_on_new,
        valid_type_on_new,
        valid_len_on_new,
        valid_null_count_on_new,
        valid_validity_bitmap_on_new,
        valid_offsets_on_new,
        valid_data_on_new,

        %% Behaviour Adherence
        new_callback
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Fixed Primitive Array Creation Tests %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

valid_layout_on_new(_Config) ->
    Array = serde_arrow_fixed_primitive_array:new([1, 2, 3], {s, 8}),
    ?assertEqual(Array#array.layout, fixed_primitive).

valid_type_on_new(_Config) ->
    Array = serde_arrow_fixed_primitive_array:new([1, 2, 3], {s, 8}),
    ?assertEqual(Array#array.layout, fixed_primitive).

valid_len_on_new(_Config) ->
    Array = serde_arrow_fixed_primitive_array:new([1, 2, 3], {s, 8}),
    ?assertEqual(Array#array.len, 3).

valid_null_count_on_new(_Config) ->
    Array1 = serde_arrow_fixed_primitive_array:new([1, 2, 3], {s, 8}),
    ?assertEqual(Array1#array.null_count, 0),

    Array2 = serde_arrow_fixed_primitive_array:new([1, undefined, 2, 3], {s, 8}),
    ?assertEqual(Array2#array.null_count, 1),

    Array3 = serde_arrow_fixed_primitive_array:new([1, nil, 2, 3], {s, 8}),
    ?assertEqual(Array3#array.null_count, 1),

    Array4 = serde_arrow_fixed_primitive_array:new([1, undefined, nil, 2, 3], {s, 8}),
    ?assertEqual(Array4#array.null_count, 2).

valid_validity_bitmap_on_new(_Config) ->
    %% Does not allocate bitmap on no nulls
    Array1 = serde_arrow_fixed_primitive_array:new([1, 2, 3], {s, 8}),
    ?assertEqual(Array1#array.validity_bitmap, undefined),

    %% Respects both Erlang's and Elixir's conventions
    Array2 = serde_arrow_fixed_primitive_array:new([1, undefined, 2, 3], {s, 8}),
    ?assertEqual(
        Array2#array.validity_bitmap,
        serde_arrow_test_utils:byte_buffer(<<0:1, 0:1, 0:1, 0:1, 1:1, 1:1, 0:1, 1:1>>)
    ),

    Array3 = serde_arrow_fixed_primitive_array:new([1, nil, 2, 3], {s, 8}),
    ?assertEqual(
        Array3#array.validity_bitmap,
        serde_arrow_test_utils:byte_buffer(<<0:1, 0:1, 0:1, 0:1, 1:1, 1:1, 0:1, 1:1>>)
    ),

    Array4 = serde_arrow_fixed_primitive_array:new([1, undefined, nil, 2], {s, 8}),
    ?assertEqual(
        Array4#array.validity_bitmap,
        serde_arrow_test_utils:byte_buffer(<<0:1, 0:1, 0:1, 0:1, 1:1, 0:1, 0:1, 1:1>>)
    ),

    %% Correctly validates on input greater than 8 elements
    Array5 = serde_arrow_fixed_primitive_array:new(
        [1, 2, undefined, 4, 5, 6, 7, 8, nil, 10], {s, 8}
    ),
    ?assertEqual(
        Array5#array.validity_bitmap,
        serde_arrow_test_utils:byte_buffer(
            <<1:1, 1:1, 1:1, 1:1, 1:1, 0:1, 1:1, 1:1, 0:1, 0:1, 0:1, 0:1, 0:1, 0:1, 1:1, 0:1>>
        )
    ).

valid_offsets_on_new(_Config) ->
    Array = serde_arrow_fixed_primitive_array:new([1, 2, 3], {s, 8}),
    ?assertEqual(Array#array.offsets, undefined).

valid_data_on_new(_Config) ->
    %% Works without any nulls
    Array1 = serde_arrow_fixed_primitive_array:new([1, 2, 3], {s, 8}),
    Data1 =
        <<1/little-signed-integer, 2/little-signed-integer, 3/little-signed-integer,
            <<0:(61 * 8)>>/bitstring>>,
    ?assertEqual(Array1#array.data#buffer.data, Data1),

    %% Works with undefined and nil
    Array2 = serde_arrow_fixed_primitive_array:new([1, 2, undefined, 3], {s, 8}),
    Data2 =
        <<1/little-signed-integer, 2/little-signed-integer, 0, 3/little-signed-integer,
            <<0:(60 * 8)>>/bitstring>>,
    ?assertEqual(Array2#array.data#buffer.data, Data2),

    Array3 = serde_arrow_fixed_primitive_array:new([1, 2, nil, 3], {s, 8}),
    Data3 = Data2,
    ?assertEqual(Array3#array.data#buffer.data, Data3),

    Array4 = serde_arrow_fixed_primitive_array:new([1, 2, undefined, nil, 3], {s, 8}),
    Data4 =
        <<1/little-signed-integer, 2/little-signed-integer, 0, 0, 3/little-signed-integer,
            <<0:(59 * 8)>>/bitstring>>,
    ?assertEqual(Array4#array.data#buffer.data, Data4).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Array Behaviour Adherence Tests %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

new_callback(_Config) ->
    Array = serde_arrow_fixed_primitive_array:new([1, 2], {s, 8}),
    Callback = serde_arrow_fixed_primitive_array:new([1, 2], [{type, {s, 8}}]),
    ?assertEqual(Callback, Array),

    ?assertError(badarg, serde_arrow_fixed_primitive_array:new([1, 2], [])).
