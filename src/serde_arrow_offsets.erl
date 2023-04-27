-module(serde_arrow_offsets).
-export([new/2]).

-include("serde_arrow_buffer.hrl").

-spec new(
    Value :: [serde_arrow_type:erlang_type()],
    Type :: serde_arrow_type:arrow_type()
) ->
    Buffer :: #buffer{}.
new(Values, Type) ->
    Offsets = offsets(Values, [0], 0, Type),
    serde_arrow_buffer:new(Offsets, {s, 32}).

-spec offsets(
    Value :: [serde_arrow_type:erlang_type()],
    Acc :: [non_neg_integer()],
    Offset :: non_neg_integer(),
    Type :: serde_arrow_type:arrow_type()
) -> Offsets :: [serde_arrow_type:erlang_type()].
offsets([Value | Rest], Acc, Offset, Type) when (Value =:= undefined) orelse (Value =:= nil) ->
    offsets(Rest, [Offset | Acc], Offset, Type);
offsets([Value | Rest], Acc, Offset, Type) ->
    CurOffset = Offset + len(Value, Type),
    offsets(Rest, [CurOffset | Acc], CurOffset, Type);
offsets([], Acc, _Offset, _Type) ->
    lists:reverse(Acc).

-spec len(
    Value :: serde_arrow_type:type() | undefined | nil, Type :: serde_arrow_type:arrow_type()
) -> non_neg_integer().
len(Value, bin) ->
    byte_size(Value);
len(Value, Type) ->
    byte_size(serde_arrow_type:serialize(Value, Type)).
