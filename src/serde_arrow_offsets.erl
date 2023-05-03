%% @doc Provides support for Apache Arrow's Offsets
%%
%% Arrow has a concept of offsets[1] in order to tell the length of a slot[2] or
%% a single element in an array of variable-size elements. This module provides
%% support for generating offsets.
%%
%% There are couple of things to remember about offsets:
%%
%% <ol>
%%  <li>
%%      Each element in the offsets coresponds to the distance in bytes of the
%%      coresponding element in the values from the beginning of the buffer.
%%
%%      I.E., distance of values[j] from beginning of the buffer = offsets[j].
%%  </li>
%%  <li>
%%      The very last element in the offsets is the length of the buffer, or
%%      distance of the end of the last slot from the beginning of the buffer.
%%  </li>
%%  <li>
%%      Thus, the offsets is one element longer than the values, or:
%%
%%      length(offsets) == length(values) + 1
%%  </li>
%%  <li>
%%      Therefore, in order to find the length of a slot, we subtract the offset
%%      the current element from the offset of the next element, or:
%%
%%      slot[j] = offsets[j + 1] - offsets[j]
%%  </li>
%%  <li>
%%      Null values have an offset of 0 as they take no memory in the buffer.
%%      Thus, the previous offset and the current offset are equivalent if the
%%      current element is a null.
%%  </li>
%% </ol>
%%
%% [1]: [https://arrow.apache.org/docs/format/Columnar.html#variable-size-binary-layout]
%%
%% [2]: [https://arrow.apache.org/docs/format/Columnar.html#terminology]
%% @end
-module(serde_arrow_offsets).
-export([new/2]).

-include("serde_arrow_buffer.hrl").

%% @doc Returns the offsets array given some values And their type.
-spec new(
    Value :: [serde_arrow_type:erlang_type()],
    Type :: serde_arrow_type:arrow_longhand_type()
) ->
    Buffer :: #buffer{}.
new(Values, Type) ->
    Offsets = offsets(Values, [0], 0, Type),
    serde_arrow_buffer:new(Offsets, {s, 32}).

-spec offsets(
    Value :: [serde_arrow_type:erlang_type()],
    Acc :: [non_neg_integer()],
    Offset :: non_neg_integer(),
    Type :: serde_arrow_type:arrow_longhand_type()
    ) -> Offsets :: [non_neg_integer()].
offsets([Value | Rest], Acc, Offset, Type) when (Value =:= undefined) orelse (Value =:= nil) ->
    offsets(Rest, [Offset | Acc], Offset, Type);
offsets([Value | Rest], Acc, Offset, Type) ->
    CurOffset = Offset + len(Value, Type),
    offsets(Rest, [CurOffset | Acc], CurOffset, Type);
offsets([], Acc, _Offset, _Type) ->
    lists:reverse(Acc).

-spec len(
    Value :: serde_arrow_type:type() | undefined | nil, Type :: serde_arrow_type:arrow_longhand_type()
) -> non_neg_integer().
len(Value, bin) ->
    byte_size(Value);
len(Value, Type) ->
    byte_size(serde_arrow_type:serialize(Value, Type)).
