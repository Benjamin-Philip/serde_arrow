-module(serde_arrow_binary_array_SUITE).

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
        valid_data_on_new
    ].

valid_layout_on_new(_Config) ->
    Array = serde_arrow_binary_array:new([<<1>>, <<2>>, <<3, 4>>]),
    ?assertEqual(Array#array.layout, binary).

valid_type_on_new(_Config) ->
    Array = serde_arrow_binary_array:new([<<1>>, <<2>>, <<3>>]),
    ?assertEqual(Array#array.type, bin).

valid_len_on_new(_Config) ->
    Array1 = serde_arrow_binary_array:new([<<1, 2>>, <<3>>]),
    ?assertEqual(Array1#array.len, 2),

    Array2 = serde_arrow_binary_array:new([<<1, 2>>, undefined, <<3>>]),
    ?assertEqual(Array2#array.len, 3).

valid_null_count_on_new(_Config) ->
    Array1 = serde_arrow_binary_array:new([<<1, 2>>, <<3>>]),
    ?assertEqual(Array1#array.null_count, 0),

    Array2 = serde_arrow_binary_array:new([<<1>>, undefined, <<2, 3>>]),
    ?assertEqual(Array2#array.null_count, 1),

    Array3 = serde_arrow_binary_array:new([<<1>>, nil, <<2, 3>>]),
    ?assertEqual(Array3#array.null_count, 1),

    Array4 = serde_arrow_binary_array:new([<<1>>, undefined, nil, <<2, 3>>]),
    ?assertEqual(Array4#array.null_count, 2).

valid_validity_bitmap_on_new(_Config) ->
    %% Does not allocate bitmap on no nulls
    Array1 = serde_arrow_binary_array:new([<<1, 2>>, <<3>>]),
    ?assertEqual(Array1#array.validity_bitmap, undefined),

    %% Respects both Erlang's and Elixir's conventions
    Array2 = serde_arrow_binary_array:new([<<1>>, undefined, <<2, 3>>]),
    ?assertEqual(
        Array2#array.validity_bitmap,
        serde_arrow_test_utils:byte_buffer(<<0:1, 0:1, 0:1, 0:1, 0:1, 1:1, 0:1, 1:1>>)
    ),

    Array3 = serde_arrow_binary_array:new([<<1>>, nil, <<2, 3>>]),
    ?assertEqual(
        Array3#array.validity_bitmap,
        serde_arrow_test_utils:byte_buffer(<<0:1, 0:1, 0:1, 0:1, 0:1, 1:1, 0:1, 1:1>>)
    ),

    Array4 = serde_arrow_binary_array:new([<<1>>, undefined, nil, <<2, 3>>]),
    ?assertEqual(
        Array4#array.validity_bitmap,
        serde_arrow_test_utils:byte_buffer(<<0:1, 0:1, 0:1, 0:1, 1:1, 0:1, 0:1, 1:1>>)
    ),

    %% Correctly validates on input greater than 8 elements
    Array5 = serde_arrow_binary_array:new([
        <<1>>, <<2>>, undefined, <<4>>, <<5>>, <<6>>, <<7>>, <<8>>, nil, <<10, 11, 12>>
    ]),
    ?assertEqual(
        Array5#array.validity_bitmap,
        serde_arrow_test_utils:byte_buffer(
            <<1:1, 1:1, 1:1, 1:1, 1:1, 0:1, 1:1, 1:1, 0:1, 0:1, 0:1, 0:1, 0:1, 0:1, 1:1, 0:1>>
        )
    ).

valid_offsets_on_new(_Config) ->
    Array = serde_arrow_binary_array:new([<<1, 2>>, <<3>>, undefined, <<4>>, nil, <<5>>]),
    %% ?assertEqual(Array#array.offsets, undefined).
    undefined.

valid_data_on_new(_Config) ->
    Array1 = serde_arrow_binary_array:new([<<1, 2>>, <<3>>, <<4>>, <<5>>]),
    Buffer1 = serde_arrow_test_utils:byte_buffer(<<1, 2, 3, 4, 5>>),
    %% ?assertEqual(Array1#array.data, Buffer1),

    Array2 = serde_arrow_binary_array:new([
        <<1, 2>>, <<3, 4, 5>>, undefined, <<6, 7, 8>>, nil, <<9, 10>>
    ]),
    Buffer2 = serde_arrow_test_utils:byte_buffer(<<1, 2, 3, 4, 5, 6, 7, 8, 9, 10>>),
    ?assertEqual(Array2#array.data, Buffer2).
