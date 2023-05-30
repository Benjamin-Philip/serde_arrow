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

-export([from_erlang/2]).

-include("serde_arrow_array.hrl").

-spec from_erlang(Values :: list(), Type :: map() | serde_arrow_type:arrow_type()) ->
    Array :: #array{}.
from_erlang(Values, Opts) when is_map(Opts) ->
    case maps:get(type, Opts, undefined) of
        undefined ->
            erlang:error(badarg);
        Type when is_tuple(Type) orelse is_atom(Type) ->
            from_erlang(Values, Type)
    end;
from_erlang(Values, GivenType) ->
    Len = length(Values),
    {Bitmap, NullCount} = serde_arrow_bitmap:validity_bitmap(Values),
    Type = serde_arrow_type:normalize(GivenType),
    Flattened = serde_arrow_utils:flatten(Values),
    Data =
        case Type of
            {bin, undefined} ->
                serde_arrow_variable_binary_array:from_erlang(Flattened);
            {_, _} ->
                serde_arrow_fixed_primitive_array:from_erlang(Flattened, Type);
            {Layout, NestedType, _Size} ->
                serde_arrow_array:from_erlang(Layout, Flattened, NestedType)
        end,
    FlatOffsets =
        case Data#array.offsets of
            undefined ->
                fixed_offsets(Type, Data#array.len);
            Offset ->
                Offset#buffer.data
        end,
    Offsets = serde_arrow_buffer:from_erlang(offsets(Values, FlatOffsets, 0), {s, 32}, Len + 1),
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
offsets(Values, [0 | TOffsets], CurOffset) ->
    [CurOffset | offsets(Values, TOffsets, CurOffset)];
offsets([H | T], Offsets, _CurOffset) ->
    Len = length(H),
    {_HOffsets, [CurOffset | TOffsets]} = lists:split(Len - 1, Offsets),
    [CurOffset | offsets(T, TOffsets, CurOffset)];
offsets([], _Offset, _CurOffset) ->
    [].

-spec fixed_offsets(Type :: serde_arrow_type:arrow_longhand_type(), Length :: pos_integer()) ->
    [non_neg_integer()].
fixed_offsets(Type, Length) when tuple_size(Type) =:= 2 ->
    ByteLen = serde_arrow_type:byte_length(Type),
    lists:seq(0, Length * ByteLen, ByteLen);
fixed_offsets({fixed_list, Type, Size}, Length) ->
    fixed_offsets(Type, Length * Size).
