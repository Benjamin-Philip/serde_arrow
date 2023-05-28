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
%% nesting that is inconsitent: a. with the type, b. between elements. The
%% behaviour on invalid input <strong>CANNOT BE GUARANTEED</strong>. Therefore,
%% one must be careful to not to <strong>CRASH THE PROCESS</strong> or worse
%% still, <strong>PRODUCE INVALID OUTPUT</strong>.
%%
%% Any input must follow the following rules:
%%
%% <ol>
%%  <li>The nesting of each element must be consistent with the type</li>
%%  <li>The nesting of each element must be consistent with each other</li>
%% </ol>
%%
%% [1]: https://arrow.apache.org/docs/format/Columnar.html#variable-size-list-layout
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
new(Values, GivenType) ->
    Len = length(Values),
    {Bitmap, NullCount} = serde_arrow_bitmap:validity_bitmap(Values),
    Type = serde_arrow_type:normalize(GivenType),
    Flattened = serde_arrow_utils:flatten(Values),
    {Data, Offsets} =
        case Type of
            {_, _} ->
                Array = serde_arrow_fixed_primitive_array:new(Flattened, Type),
                [0 | FlatOffsets] = serde_arrow_offsets:new_list(Flattened, Type),
                Offset = serde_arrow_buffer:from_erlang(
                    [0 | offsets(Values, FlatOffsets, 0)], {s, 32}
                ),
                {Array, Offset};
            {Layout, NestedType, _Size} ->
                Array = serde_arrow_array:new(Layout, Flattened, NestedType),
                Offset = serde_arrow_buffer:from_erlang(nested_offsets(Values, Type), {s, 32}),
                {Array, Offset}
        end,
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
