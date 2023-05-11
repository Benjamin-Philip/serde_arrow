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
    (is_tuple(GivenType) andalso tuple_size(GivenType) =:= 2) orelse is_atom(GivenType)
->
    Type = serde_arrow_type:normalize(GivenType),
    %% Data
    Flattened = serde_arrow_utils:flatten(Values),
    Array = serde_arrow_fixed_primitive_array:new(Flattened, Type),
    %% Offsets
    [0 | FlatOffsets] = serde_arrow_offsets:new_list(Flattened, Type),
    Offset = serde_arrow_buffer:new([0 | offsets(Values, FlatOffsets, 0)], {s, 32}),
    init(Values, Type, Array, Offset);
new(Values, {variable_list, NestedType, undefined} = Type) ->
    Flattened = serde_arrow_utils:flatten(Values),
    Array = new(Flattened, NestedType),
    Offset = serde_arrow_buffer:new(nested_offsets(Values, Type), {s, 32}),
    init(Values, Type, Array, Offset);
new(Values, {fixed_list, NestedType, _Size} = Type) ->
    Flattened = serde_arrow_utils:flatten(Values),
    Array = serde_arrow_fixed_list_array:new(Flattened, NestedType),
    Offset = serde_arrow_buffer:new(nested_offsets(Values, Type), {s, 32}),
    init(Values, Type, Array, Offset).

%%%%%%%%%%%
%% Utils %%
%%%%%%%%%%%

-spec init(
    Values :: list(), Type :: serde_arrow_type:arrow_type(), Data :: #array{}, Offsets :: list()
) -> #array{}.
init(Values, Type, Data, Offsets) ->
    Len = length(Values),
    {Bitmap, NullCount} = serde_arrow_bitmap:validity_bitmap(Values),
    #array{
        layout = variable_list,
        type = Type,
        len = Len,
        element_len = undefined,
        null_count = NullCount,
        validity_bitmap = Bitmap,
        offsets = Offsets,
        data = Data
    }.
%% -spec init_with_offsets(
%%     Values :: list(), Type :: serde_arrow_type:arrow_type(), Data :: #array{}, Offsets :: list()
%% ) -> {#array{}, [non_neg_integer()]}.
%% init_with_offsets(Values, Type, Data, Offsets) ->
%%     {init(Values, Type, Data, Offsets), Offsets}.

offsets([H | T], Offsets, CurOffset) when (H =:= undefined) orelse (H =:= nil) ->
    [CurOffset | offsets(T, Offsets, CurOffset)];
offsets([H | T], Offsets, _CurOffset) ->
    Len = length(H),
    {HOffsets, TOffsets} = lists:split(Len, Offsets),
    CurOffset = lists:last(HOffsets),
    [CurOffset | offsets(T, TOffsets, CurOffset)];
offsets([], _Offset, _CurOffset) ->
    [].

nested_offsets(List, Type) ->
    case serde_arrow_utils:nesting(List) of
        2 ->
            [0 | FlatOffsets] = serde_arrow_offsets:new_list(serde_arrow_utils:flatten(List), serde_arrow_type:normalize(Type)),
            [0 | offsets(List, FlatOffsets, 0)];
        _Nesting ->
            Flattened = serde_arrow_utils:flatten(List),
            [0 | FlatOffsets] = nested_offsets(Flattened, element(2, Type)),
            [0 | offsets(List, FlatOffsets, 0)]
    end.
