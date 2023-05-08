%% @private Utils module
-module(serde_arrow_utils).
-export([nesting/1, flatten/1, flatten/2, flatten/3]).

%% Finds the nesting level of deep list
%%
%% Assumes each sublist has the same nesting
-spec nesting(List :: list() | term()) -> pos_integer().
nesting([H | _T]) when is_list(H) ->
    1 + nesting(H);
nesting([H | T]) when (H =:= undefined) orelse (H =:= nil) ->
    nesting(T);
nesting(_H) ->
    1.

%% Flattens a deeplist by one level
%%
%% Assumes each sublist has the same nesting
-spec flatten(List :: list()) -> list().
flatten(List) ->
    flatten(List, fun() -> [] end).

-spec flatten(List :: list(), Null :: fun(() -> list())) -> list().
flatten(List, Null) ->
    flatten(List, Null, fun(X) -> X end).

-spec flatten(List :: list(), Null :: fun(() -> list()), Apply :: fun((list()) -> list())) ->
    list().
flatten([H | T], Null, Apply) when is_list(H) ->
    Apply(H) ++ flatten(T, Null, Apply);
flatten([H | T], Null, Apply) when (H =:= undefined) orelse (H =:= nil) ->
    Null() ++ flatten(T, Null, Apply);
flatten(H, _Null, _Apply) ->
    H.
