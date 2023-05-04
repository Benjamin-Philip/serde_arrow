%% @private Utils module
-module(serde_arrow_utils).
-export([nesting/1, flatten/1, flatten/2]).

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

-spec flatten(List :: list(), Fun :: fun(() -> list())) -> list().
flatten([H | T], Fun) when is_list(H)  ->
    H ++ flatten(T, Fun);
flatten([H | T], Fun) when (H =:= undefined) orelse (H =:= nil) ->
    Fun() ++ flatten(T, Fun);
flatten(H, _Fun) ->
    H.
