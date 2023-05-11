-module(serde_arrow_variable_list_array).

-behaviour(serde_arrow_array).

-export([new/2]).

-include("serde_arrow_array.hrl").
-spec new(Values :: list(), Type :: map() | serde_arrow_type:arrow_type()) -> Array :: #array{}.
new(Values, Opts) when is_map(Opts) ->
    case maps:get(type, Opts, undefined) of
        undefined ->
            erlang:error(badarg);
        Type when is_tuple(Type) orelse is_atom(Type) ->
            new(Values, Type)
    end;
new(Values, GivenType) when
    is_tuple(GivenType), tuple_size(GivenType) =:= 2;
    is_atom(GivenType)
->
    Len = length(Values),
    {Bitmap, NullCount} = serde_arrow_bitmap:validity_bitmap(Values),
    Type = serde_arrow_type:normalize(GivenType),
    %% Data
    Flattened = serde_arrow_utils:flatten(Values),
    Array = serde_arrow_fixed_primitive_array:new(Flattened, Type),
    %% Offsets
    [0 | FlatOffsets] = serde_arrow_offsets:new_list(Flattened, Type),
    Offsets = serde_arrow_buffer:new([0 | offsets(Values, FlatOffsets, 0)], {s, 32}),
    #array{
        layout = variable_list,
        type = Type,
        len = Len,
        element_len = undefined,
        null_count = NullCount,
        validity_bitmap = Bitmap,
        offsets = Offsets,
        data = Array
    };
new(Values, {Layout, NestedType, Size} = Type) when
    Layout =:= variable_list, Size =:= undefined;
    Layout =:= fixed_list, is_integer(Size)
->
    Len = length(Values),
    {Bitmap, NullCount} = serde_arrow_bitmap:validity_bitmap(Values),
    Flattened = serde_arrow_utils:flatten(Values),
    Array = serde_arrow_array:new(Layout, Flattened, NestedType),
    Offsets = serde_arrow_buffer:new(nested_offsets(Values, Type), {s, 32}),
    #array{
        layout = variable_list,
        type = Type,
        len = Len,
        element_len = undefined,
        null_count = NullCount,
        validity_bitmap = Bitmap,
        offsets = Offsets,
        data = Array
    }.

%%%%%%%%%%%
%% Utils %%
%%%%%%%%%%%

-spec offsets(Values :: list(), Offsets :: [non_neg_integer()], CurOffset :: non_neg_integer()) ->
    [non_neg_integer()].
offsets([H | T], Offsets, CurOffset) when (H =:= undefined) orelse (H =:= nil) ->
    [CurOffset | offsets(T, Offsets, CurOffset)];
offsets([H | T], Offsets, _CurOffset) ->
    Len = length(H),
    {_HOffsets, [CurOffset | TOffsets]} = lists:split(Len - 1, Offsets),
    [CurOffset | offsets(T, TOffsets, CurOffset)];
offsets([], _Offset, _CurOffset) ->
    [].

nested_offsets(List, Type) ->
    case serde_arrow_utils:nesting(List) of
        2 ->
            [0 | FlatOffsets] = serde_arrow_offsets:new_list(
                serde_arrow_utils:flatten(List), serde_arrow_type:normalize(Type)
            ),
            [0 | offsets(List, FlatOffsets, 0)];
        _Nesting ->
            Flattened = serde_arrow_utils:flatten(List),
            [0 | FlatOffsets] = nested_offsets(Flattened, element(2, Type)),
            [0 | offsets(List, FlatOffsets, 0)]
    end.
