-module(serde_arrow_ipc_record_batch).
-export([from_erlang/1]).
-export_type([field_node/0, buffer/0]).

-include("serde_arrow_ipc_record_batch.hrl").
-include("serde_arrow_array.hrl").

-type field_node() :: #{length => pos_integer(), null_count => non_neg_integer()}.
-type buffer() :: #{offset => non_neg_integer(), length => pos_integer()}.

-spec from_erlang(Arrays :: [#array{}]) -> RecordBatch :: #record_batch{}.
from_erlang(Arrays) ->
    FieldNodes = lists:map(
        fun(X) -> #{length => X#array.len, null_count => X#array.null_count} end, Arrays
    ),
    {Buffers, _} = lists:foldl(
        fun(Array, {Acc, CurOffset}) ->
            {Data, NewOffset} = array_data(Array, CurOffset),
            {lists:reverse(Data) ++ Acc, NewOffset}
        end,
        {[], 0},
        Arrays
    ),
    [Array | _] = Arrays,
    Length = Array#array.len,
    #record_batch{length = Length, nodes = FieldNodes, buffers = lists:reverse(Buffers)}.

%% Returns the buffer data for all the buffers and nested arrays in an array.
-spec array_data(Array :: #array{}, CurOffset :: non_neg_integer()) ->
    {[buffer()], NewOffset :: non_neg_integer()}.
array_data(Array, CurOffset) ->
    Bitmap = Array#array.validity_bitmap,
    Offsets = Array#array.offsets,
    Data = Array#array.data,

    {Buffers, SndOffset} =
        case {Bitmap, Offsets} of
            {undefined, undefined} ->
                {[], CurOffset};
            {undefined, _} ->
                {Buffer, Offset} = buffer_data(Offsets, CurOffset),
                {[Buffer], Offset};
            {_, undefined} ->
                {Buffer, Offset} = buffer_data(Bitmap, CurOffset),
                {[Buffer], Offset};
            _ ->
                {BitmapBuffer, Offset} = buffer_data(Bitmap, CurOffset),
                {OffsetBuffer, OtherOffset} = buffer_data(Offsets, Offset),
                {[BitmapBuffer, OffsetBuffer], OtherOffset}
        end,

    if
        is_record(Data, array) ->
            {DataBuffers, NewOffset} = array_data(Data, SndOffset),
            {Buffers ++ DataBuffers, NewOffset};
        true ->
            {DataBuffer, NewOffset} = buffer_data(Data, SndOffset),
            {Buffers ++ [DataBuffer], NewOffset}
    end.

-spec buffer_data(Buffer :: #buffer{}, CurOffset :: non_neg_integer()) ->
    {BufferData :: buffer(), NewOffset :: pos_integer()}.
buffer_data(Buffer, CurOffset) ->
    {
        #{offset => CurOffset, length => Buffer#buffer.length},
        serde_arrow_buffer:size(Buffer) + CurOffset
    }.
