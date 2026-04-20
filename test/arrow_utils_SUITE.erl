-module(arrow_utils_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [
        nesting,
        flatten,
        pad_len
    ].

nesting(_Config) ->
    ?assertEqual(arrow_utils:nesting([1, 2, 3]), 1),
    ?assertEqual(arrow_utils:nesting([undefined, nil, 3]), 1),
    ?assertEqual(arrow_utils:nesting([undefined, nil]), 1),
    ?assertEqual(arrow_utils:nesting([[1], [2], [3]]), 2),
    ?assertEqual(arrow_utils:nesting([[[[[1]]]]]), 5).

flatten(_Config) ->
    ?assertEqual(arrow_utils:flatten([1, 2, 3]), [1, 2, 3]),
    ?assertEqual(arrow_utils:flatten([[1, 2, 3]]), [1, 2, 3]),
    ?assertEqual(arrow_utils:flatten([[1], [2], [3]]), [1, 2, 3]),
    ?assertEqual(arrow_utils:flatten([[[[[1, 2], [3, 4]]]]]), [[[[1, 2], [3, 4]]]]),
    ?assertEqual(arrow_utils:flatten([[1], undefined, [2], nil, [3]]), [1, 2, 3]),
    ?assertEqual(
        arrow_utils:flatten([[1], undefined, [2], nil, [3]], fun() -> [foobar] end, 1),
        [
            1, foobar, 2, foobar, 3
        ]
    ),
    ?assertError(
        badarg,
        arrow_utils:flatten([[1], undefined, [2], nil, [3]], fun() -> [foobar] end, 2)
    ).

pad_len(_Config) ->
    ?assertEqual(arrow_utils:pad_len(10), 54),
    ?assertEqual(arrow_utils:pad_len(64), 0),
    ?assertEqual(arrow_utils:pad_len(74), 54),

    ?assertEqual(arrow_utils:pad_len(33, 8), 7).
