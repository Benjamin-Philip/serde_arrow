-module(serde_arrow_utils_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [
        nesting,
        flatten
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
        serde_arrow_utils:flatten([[1], undefined, [2], nil, [3]], fun() -> [foobar] end), [
            1, foobar, 2, foobar, 3
        ]
    ),
    ?assertEqual(
        serde_arrow_utils:flatten([[1], undefined, [2], nil, [3]], fun() -> [foobar] end, fun(X) ->
            X ++ [baz]
        end),
        [
            1, baz, foobar, 2, baz, foobar, 3, baz
        ]
    ).
