%% @doc Buffer implementation for `serde_arrow'.
%% This module adds suppport for buffers, or Contiguous Memory Regions.
%%
%% There are multiple things to know about buffers[1]:
%%
%% <ol>
%%  <li>
%%      Each value it stores is called an element or a slot[2].
%%  </li>
%%  <li>
%%      Each slot's length (in bytes) is a positive integer. As a result when we
%%      say that a slot has a length of 1, we mean that each slot has a length of
%%      1 byte.
%%  </li>
%%  <li>
%%      The buffer's length in the metadata refers to the unpadded binary's size in bytes.
%%  </li>
%%  <li>
%%      All buffers have a size that is a multiple of 64. If their data's length
%%      is not a multiple of 64, it must be padded (in this implementation, by
%%      zeros).
%%  </li>
%%  <li>
%%      Null values are represented in this implementation by zeros.
%%  </li>
%%  <li>
%%      In this implementation buffers can be initialized from raw bytes as data
%%      apart datatypes supported by Arrow. This is so that the Validity Bitmap
%%      Buffer can be initialized.
%%  </li>
%% </ol>
%%
%% [1]: [https://arrow.apache.org/docs/format/Glossary.html#term-buffer]
%%
%% [2]: [https://arrow.apache.org/docs/format/Glossary.html#term-slot]
%% @end
-module(serde_arrow_buffer).
-export([new/2, from_binary/3]).

-include("serde_arrow_buffer.hrl").

%% @doc Creates a new buffer from a list of Erlang values or binaries, given its
%% type
%% @end
-spec new(
    Value :: [serde_arrow_type:erlang_type()],
    Type :: serde_arrow_type:arrow_longhand_type()
) ->
    Buffer :: #buffer{}.
new(Values, Type) ->
    ElementLen = serde_arrow_type:byte_length(Type),
    Bin = <<(slot(X, Type, ElementLen)) || X <- Values>>,
    Len = byte_size(Bin),
    from_binary(Bin, Type, Len).

%% @doc Returns a new buffer given a raw binary
%%
%% The following are its parameters in order:
%%
%% <ol>
%%  <li>A padded or unpadded binary</li>
%%  <li>The type of the value stored by the buffer</li>
%%  <li>Length of the binary in bytes</li>
%% </ol>
%% @end
-spec from_binary(
    Values :: binary(),
    Type :: serde_arrow_type:arrow_longhand_type(),
    Len :: pos_integer()
) -> Buffer :: #buffer{}.
from_binary(Values, Type, Len) ->
    PadLen = 64 - Len rem 64,
    Bin = pad(Values, PadLen),
    #buffer{type = Type, length = Len, data = Bin}.

-spec slot(
    Value :: serde_arrow_type:erlang_type(),
    Type :: serde_arrow_type:arrow_longhand_type(),
    ElementLen :: pos_integer() | undefined
) -> binary().
slot(Value, _Type, ElementLen) when (Value =:= undefined) orelse (Value =:= nil) ->
    pad(<<>>, ElementLen);
slot(Value, Type, _ElementLen) ->
    serde_arrow_type:serialize(Value, Type).

-spec pad(Binary :: binary(), PadLen :: pos_integer() | undefined) -> binary().
pad(_Binary, undefined) ->
    <<>>;
pad(Binary, PadLen) when is_integer(PadLen) ->
    <<Binary/bitstring, <<0:(PadLen * 8)>>/bitstring>>.
