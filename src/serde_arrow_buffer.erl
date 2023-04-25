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
%%      say a buffer's element length is 1, we mean that each slot has a length of
%%      1 byte. Same goes for buffer length.
%%  </li>
%%  <li>
%%      All buffers have a length that is a multiple of 64. If their data's length
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
-export([new/2, new/3, from_binary/4]).

-include("serde_arrow_buffer.hrl").

%% @doc Creates a new buffer from a list of Erlang values, given its type
-spec new(Value :: [serde_arrow_type:erlang_type()], Type :: serde_arrow_type:arrow_type()) ->
    Buffer :: #buffer{}.
new(Values, Type) -> new(Values, Type, length(Values)).

%% @doc Creates a new buffer from a list of Erlang values, given its type and length
-spec new(
    Value :: [serde_arrow_type:erlang_type()],
    Type :: serde_arrow_type:arrow_type(),
    Len :: pos_integer()
) ->
    Buffer :: #buffer{}.
new(Values, Type, Len) ->
    ElementLen = serde_arrow_type:byte_length(Type),
    Bin = <<(slot(X, Type, ElementLen)) || X <- Values>>,
    from_binary(Bin, Type, Len, ElementLen).

%% @doc Returns a new buffer given a raw binary
%%
%% The following are its parameters in order:
%%
%% <ol>
%%  <li>A padded or unpadded binary</li>
%%  <li>The type of the value stored by the buffer</li>
%%  <li>Length of the binary in bytes</li>
%%  <li>The length of each element of the buffer in bytes</li>
%% </ol>
%% @end
-spec from_binary(
    Values :: binary(),
    Type :: serde_arrow_type:arrow_type() | byte,
    Len :: pos_integer(),
    ElementLen :: pos_integer()
) -> Buffer :: #buffer{}.
from_binary(Values, Type, Len, ElementLen) ->
    PadLen = 64 - Len rem 64,
    Bin = pad(Values, PadLen),
    #buffer{type = Type, length = Len, element_length = ElementLen, data = Bin}.

slot(Value, _Type, ElementLen) when (Value =:= undefined) orelse (Value =:= nil) ->
    pad(<<>>, ElementLen);
slot(Value, Type, _ElementLen) ->
    serde_arrow_type:serialize(Value, Type).

pad(Binary, PadLen) ->
    <<Binary/bitstring, <<0:(PadLen * 8)>>/bitstring>>.
