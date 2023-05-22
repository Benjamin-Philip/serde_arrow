%% @doc Provides support for Arrow's Fixed-Size List Layout.
%%
%% This module provides support for the Fixed-Size List Layout[1], which is an
%% layout that supports storing a list of lists of a specific length and
%% nesting.
%%
%% == Invalid Input ==
%%
%% It is important that care is taken when passing input values to this module.
%% For performance reasons, the input is not validated. The function crashes on
%% nesting that is inconsitent: a. with the type, b. between elements. The lists
%% are checked to have the same length as each other at the top level, but not
%% for deeper levels. The behaviour of the module on invalid input
%% <strong>CANNOT BE GUARANTEED</strong>. Therefore, one must be careful to not
%% to <strong>CRASH THE PROCESS</strong> or worse still, <strong>PRODUCE INVALID
%% OUTPUT</strong>.
%%
%% Any input must follow the following rules:
%%
%% <ol>
%%  <li>The length of each element must be consistent with the type</li>
%%  <li>The nesting of each element must be consistent with the type</li>
%%  <li>The length of each element must be consistent with each other</li>
%%  <li>The nesting of each element must be consistent with each other</li>
%%  <li>
%%      The nested type is a `fixed_list' (as only then can fixed size be
%%      guaranteed)
%%  </li>
%% </ol>
%%
%% [1]: https://arrow.apache.org/docs/format/Columnar.html#variable-size-list-layout
-module(serde_arrow_fixed_list_array).
-behaviour(serde_arrow_array).

-export([new/2]).

-include("serde_arrow_array.hrl").

%% @doc Creates a Fixed-Size List Array given the values and type.
%%
%% Accepts a map with the type, or the type directly.
%% @end
-spec new(Values :: list(), Type :: map() | serde_arrow_type:arrow_type()) -> Array :: #array{}.
new(Values, Opts) when is_map(Opts) ->
    case maps:get(type, Opts, undefined) of
        undefined ->
            erlang:error(badarg);
        Type when is_tuple(Type) orelse is_atom(Type) ->
            new(Values, Type)
    end;
new([[H | _] | _] = Values, GivenType) when
      (is_tuple(GivenType) andalso tuple_size(GivenType) =:= 2) orelse is_atom(GivenType),
      not is_list(H)
->
    Len = length(Values),
    ElementLen = element_len(Values),
    Type = serde_arrow_type:normalize(GivenType),
    {Bitmap, NullCount} = serde_arrow_bitmap:validity_bitmap(Values),
    Flattened = serde_arrow_utils:flatten(Values, fun() -> [undefined] end, ElementLen),
    Array = serde_arrow_fixed_primitive_array:new(Flattened, Type),
    #array{
        layout = fixed_list,
        type = Type,
        len = Len,
        element_len = ElementLen,
        null_count = NullCount,
        validity_bitmap = Bitmap,
        data = Array
    };
new(Value, {fixed_list, NestedType, Size} = Type) ->
    Len = length(Value),
    {Bitmap, NullCount} = serde_arrow_bitmap:validity_bitmap(Value),
    Shape = shape(Value, NestedType),
    Null = [list_from_shape(Shape)],
    Flattened = serde_arrow_utils:flatten(Value, fun() -> Null end, Size),
    Array = serde_arrow_fixed_list_array:new(Flattened, NestedType),
    #array{
        layout = fixed_list,
        type = Type,
        len = Len,
        element_len = Size,
        null_count = NullCount,
        validity_bitmap = Bitmap,
        data = Array
    };
new(_Value, _Layout) ->
    erlang:error(badarg).

%%%%%%%%%%%
%% Utils %%
%%%%%%%%%%%

-spec element_len(List :: [serde_arrow_type:native_type()]) -> ElementLen :: pos_integer().
element_len([H | _T]) when is_list(H) ->
    length(H);
element_len([H | T]) when (H =:= undefined) orelse (H =:= nil) ->
    element_len(T);
element_len([]) ->
    erlang:error(badarg).

-spec shape(Values :: list(), Type :: serde_arrow_type:arrow_type()) -> [pos_integer()].
shape(Values, {_, Type, Size}) ->
    [Size] ++ shape(serde_arrow_utils:flatten(Values), Type);
shape(Values, Type) when is_atom(Type) orelse tuple_size(Type) =:= 2 ->
    [element_len(Values)].

-spec list_from_shape(list()) -> list().
list_from_shape([H]) ->
    lists:duplicate(H, undefined);
list_from_shape([H | T]) ->
    lists:duplicate(H, list_from_shape(T)).
