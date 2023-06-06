-module(serde_arrow_utils_SUITE).

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
    ?assertEqual(serde_arrow_utils:nesting([1, 2, 3]), 1),
    ?assertEqual(serde_arrow_utils:nesting([undefined, nil, 3]), 1),
    ?assertEqual(serde_arrow_utils:nesting([undefined, nil]), 1),
    ?assertEqual(serde_arrow_utils:nesting([[1], [2], [3]]), 2),
    ?assertEqual(serde_arrow_utils:nesting([[[[[1]]]]]), 5).

flatten(_Config) ->
    ?assertEqual(serde_arrow_utils:flatten([1, 2, 3]), [1, 2, 3]),
    ?assertEqual(serde_arrow_utils:flatten([[1, 2, 3]]), [1, 2, 3]),
    ?assertEqual(serde_arrow_utils:flatten([[1], [2], [3]]), [1, 2, 3]),
    ?assertEqual(serde_arrow_utils:flatten([[[[[1, 2], [3, 4]]]]]), [[[[1, 2], [3, 4]]]]),
    ?assertEqual(serde_arrow_utils:flatten([[1], undefined, [2], nil, [3]]), [1, 2, 3]),
    ?assertEqual(
        serde_arrow_utils:flatten([[1], undefined, [2], nil, [3]], fun() -> [foobar] end, 1),
        [
            1, foobar, 2, foobar, 3
        ]
    ),
    ?assertError(
        badarg,
        serde_arrow_utils:flatten([[1], undefined, [2], nil, [3]], fun() -> [foobar] end, 2)
    ).

pad_len(_Config) ->
    ?assertEqual(serde_arrow_utils:pad_len(10), 54),
    ?assertEqual(serde_arrow_utils:pad_len(64), 0),
    ?assertEqual(serde_arrow_utils:pad_len(74), 54),

    ?assertEqual(serde_arrow_utils:pad_len(33, 8), 7).
