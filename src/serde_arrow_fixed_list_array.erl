%% @doc Provides support for Arrow's Fixed-Size List Layout.
-module(serde_arrow_fixed_list_array).
-behaviour(serde_arrow_array).

-export([new/2]).

-include("serde_arrow_array.hrl").

%% @doc Creates a Fixed-Size List Array given the values and type.
%%
%% Accepts a map with the type, or the type directly.
%% @end
-spec new(Values :: list(), Opts :: map()) -> Array :: #array{}.
new(Values, Opts) when is_map(Opts) ->
    case maps:get(type, Opts, undefined) of
        undefined ->
            erlang:error(badarg);
        Type when is_tuple(Type) orelse is_atom(Type) ->
            new(Values, Type)
    end;
new(Value, GivenType) when (is_tuple(GivenType) andalso tuple_size(GivenType) =:= 2) orelse is_atom(GivenType) ->
    Len = length(Value),
    ElementLen = element_len(Value),
    Type = serde_arrow_type:normalize(GivenType),
    {Bitmap, NullCount} = serde_arrow_bitmap:validity_bitmap(Value),
    Flattened = serde_arrow_utils:flatten(Value, fun() -> [undefined] end),
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
new(Value, {fixed_list, NestedType, Size} = Type) when tuple_size(Type) =:= 3 ->
    Len = length(Value),
    {Bitmap, NullCount} = serde_arrow_bitmap:validity_bitmap(Value),
    Shape = shape(Value, NestedType),
    Null = [list_from_shape(Shape)],
    Flattened = serde_arrow_utils:flatten(Value, fun() -> Null end),
    Array = serde_arrow_fixed_list_array:new(Flattened, NestedType),
    #array{
        layout = fixed_list,
        type = Type,
        len = Len,
        element_len = Size,
        null_count = NullCount,
        validity_bitmap = Bitmap,
        data = Array
      }.

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
shape(Values, {_, Type, Size})  ->
    [Size] ++ shape(serde_arrow_utils:flatten(Values), Type);
shape(Values, Type) when is_atom(Type) orelse tuple_size(Type) =:= 2 ->
    [element_len(Values)].

-spec list_from_shape(list()) -> list().
list_from_shape([H]) ->
    lists:duplicate(H, undefined);
list_from_shape([H | T]) ->
    lists:duplicate(H, list_from_shape(T)).
