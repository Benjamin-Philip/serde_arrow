%% @doc Provides a record and functions to deal with RecordBatches
%%
%% A RecordBatch[1] represents a list of equal length arrays and their
%% coresponding buffers. This module provides a record and a function to manage
%% all the metadata required to represent a RecordBatch. Metadata such as:
%%
%% <ol>
%%  <li>
%%      `length': The number of rows or records. In other words, the length of
%%      an array.
%%  </li>
%%  <li>
%%      `nodes': A list of maps, where each map has the length and null count
%%      of an array
%%  </li>
%%  <li>
%%      `buffers': A list of maps, where each map has the length and the offset
%%      (from the beginning of the message body) of a buffer of an array.
%%  </li>
%%  <li>
%%      `compression': The compression applied on the body of the Record Batch.
%%      Can either by `undefined' (i.e. no compression), `zstd' for Zstandard[2],
%%      or `lz4_frame' for LZ4 Frame[3]. Defaults to `undefined'.
%%  </li>
%%
%% Currently, compression is not supported, but it has been added for forwards
%% comapatibility.
%%
%% You can find RecordBatches in the Arrow spec here[4].
%%
%% [1]: [https://github.com/apache/arrow/blob/16328f0ccc73b7df665b4a18feb6adf26b7aa0e2/format/Message.fbs#L81-L102]
%%
%% [2]: [https://facebook.github.io/zstd/]
%%
%% [3]: [https://android.googlesource.com/platform/external/lz4/+/HEAD/doc/lz4_Frame_format.md]
%%
%% [4]: [https://arrow.apache.org/docs/format/Columnar.html#recordbatch-message]
%% @end
-module(serde_arrow_ipc_record_batch).
-export([from_erlang/1]).
-export_type([field_node/0, buffer/0]).

-include("serde_arrow_ipc_record_batch.hrl").
-include("serde_arrow_array.hrl").

-type field_node() :: #{length => pos_integer(), null_count => non_neg_integer()}.
-type buffer() :: #{offset => non_neg_integer(), length => pos_integer()}.

%% @doc Creates a RecordBatch given a list of arrays
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
