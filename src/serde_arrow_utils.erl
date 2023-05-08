%% @private Utils module
-module(serde_arrow_utils).
-export([nesting/1, flatten/1, flatten/3]).

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
flatten([H | T]) when is_list(H) ->
    H ++ flatten(T);
flatten([H | T]) when (H =:= undefined) orelse (H =:= nil) ->
    flatten(T);
flatten(H) ->
    H.

-spec flatten(List :: list(), Null :: fun(() -> list()), Length :: pos_integer()) ->
    list().
flatten([H | T], Null, Length) when is_list(H) ->
    if
        length(H) =:= Length -> H ++ flatten(T, Null, Length);
        true -> erlang:error(badarg)
    end;
flatten([H | T], Null, Length) when (H =:= undefined) orelse (H =:= nil) ->
    Null() ++ flatten(T, Null, Length);
flatten(H, _Null, _Length) ->
    H.
