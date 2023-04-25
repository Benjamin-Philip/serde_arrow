-module(serde_arrow_bitmap_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [
        valid_null_count,
        valid_validity_bitmap
    ].

valid_null_count(_Config) ->
    {_Bitmap1, NullCount1} = serde_arrow_bitmap:validity_bitmap([1, 2, 3]),
    ?assertEqual(NullCount1, 0),

    %% Respects both Erlang's and Elixir's conventions
    {_Bitmap2, NullCount2} = serde_arrow_bitmap:validity_bitmap([1, undefined, 2, 3]),
    ?assertEqual(NullCount2, 1),

    {_Bitmap3, NullCount3} = serde_arrow_bitmap:validity_bitmap([1, nil, 2, 3]),
    ?assertEqual(NullCount3, 1),

    {_Bitmap4, NullCount4} = serde_arrow_bitmap:validity_bitmap([1, undefined, nil, 2, 3]),
    ?assertEqual(NullCount4, 2),

    %% Works with input greater than 8 elements
    {_Bitmap5, NullCount5} = serde_arrow_bitmap:validity_bitmap([
        1, undefined, nil, 2, 3, 4, 5, 6, 7
    ]),
    ?assertEqual(NullCount5, 2).

valid_validity_bitmap(_Config) ->
    %% Does not allocate bitmap on no nulls
    {Bitmap1, _NullCount1} = serde_arrow_bitmap:validity_bitmap([1, 2, 3]),
    ?assertEqual(Bitmap1, undefined),

    %% Respects both Erlang's and Elixir's conventions
    {Bitmap2, _NullCount2} = serde_arrow_bitmap:validity_bitmap([1, undefined, 2, 3]),
    Buffer2 = serde_arrow_test_utils:byte_buffer(<<0:1, 0:1, 0:1, 0:1, 1:1, 1:1, 0:1, 1:1>>),
    ?assertEqual(Bitmap2, Buffer2),

    {Bitmap3, _NullCount3} = serde_arrow_bitmap:validity_bitmap([1, nil, 2, 3]),
    Buffer3 = serde_arrow_test_utils:byte_buffer(<<0:1, 0:1, 0:1, 0:1, 1:1, 1:1, 0:1, 1:1>>),
    ?assertEqual(Bitmap3, Buffer3),

    {Bitmap4, _NullCount4} = serde_arrow_bitmap:validity_bitmap([1, undefined, nil, 2, 3]),
    Buffer4 = serde_arrow_test_utils:byte_buffer(<<0:1, 0:1, 0:1, 1:1, 1:1, 0:1, 0:1, 1:1>>),
    ?assertEqual(Bitmap4, Buffer4),

    %% Works with input greater than 8 elements
    {Bitmap5, _NullCount5} = serde_arrow_bitmap:validity_bitmap([
        1, 2, undefined, 4, 5, 6, 7, 8, nil, 10
    ]),
    Buffer5 = serde_arrow_test_utils:byte_buffer(
        <<1:1, 1:1, 1:1, 1:1, 1:1, 0:1, 1:1, 1:1, 0:1, 0:1, 0:1, 0:1, 0:1, 0:1, 1:1, 0:1>>
    ),
    ?assertEqual(Bitmap5, Buffer5).
