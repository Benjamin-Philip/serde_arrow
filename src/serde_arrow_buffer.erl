%% Buffer implementation for `serde_arrow'.
-module(serde_arrow_buffer).
-export([new/2, new/3, from_binary/4]).

-include("serde_arrow_buffer.hrl").

%% This function returns a new buffer, or Contiguous Memory Region[1].
%%
%% There are multiple things to know about buffers:
%%
%% 1. Each value it stores is called an element or a slot[2].
%%
%% 2. Each slot's length (in bytes) is a positive integer. As a result when we
%%    say a buffer's element length is 1, we mean that each slot has a length of
%%    1 byte. Same goes for buffer length.
%%
%% 3. All buffers have a length that is a multiple of 64. If their data's length
%%    is not a multiple of 64, it must be padded (in this implementation, by
%%    zeros).
%%
%% 4. Null values are represented in this implementation by zeros.
%%
%% 5. In this implementation buffers can be initialized from raw bytes as data
%%    apart datatypes supported by Arrow. This is so that the Validity Bitmap
%%    Buffer can be initialized.
%%
%% [1]: https://arrow.apache.org/docs/format/Glossary.html#term-buffer
%% [2]: https://arrow.apache.org/docs/format/Glossary.html#term-slot

new(Values, Type) -> new(Values, Type, length(Values)).
new(Values, Type, Len) ->
    ElementLen = serde_arrow_type:byte_length(Type),
    Bin = <<(slot(X, Type, ElementLen)) || X <- Values>>,
    from_binary(Bin, Type, Len, ElementLen).

%% TODO Rethink what all is needed for a buffer.
%%
%% This function returns a new buffer, given, in this order:
%%
%% 1. A padded or unpadded binary
%%
%% 2. The type of the value stored by the buffer
%%
%% 3. Length of the binary in bytes
%%
%% 4. The length of each element of the buffer in bytes

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
