%% @doc Provides support for Arrow's Variable-Sized Binary Layout.
%%
%% The Variable-Sized Binary Layout[1] provides support for storing binaries of
%% varying length in a way similar to the primitive layout, i.e. in a 1
%% Dimensional Array.
-module(serde_arrow_binary_array).
-export([new/1]).

-include("serde_arrow_array.hrl").

-spec new(Value :: [serde_arrow_type:erlang_type()]) -> Array :: #array{}.
new(Value) ->
    Len = length(Value),
    {Bitmap, NullCount} = serde_arrow_bitmap:validity_bitmap(Value),
    Bin = serde_arrow_buffer:new(Value, bin),
    #array{
        layout = binary,
        type = bin,
        len = Len,
        null_count = NullCount,
        validity_bitmap = Bitmap,
        data = Bin
    }.
