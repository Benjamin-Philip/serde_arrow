%% @doc Provides support for Arrow's Variable-Sized Binary Layout.
%%
%% The Variable-Sized Binary Layout[1] provides support for storing binaries of
%% varying length in a way similar to the primitive layout, i.e. in a 1
%% Dimensional Array.
-module(serde_arrow_variable_binary_array).
-behaviour(serde_arrow_array).

-export([new/1, new/2]).

-include("serde_arrow_array.hrl").

%% @doc Creates a Variable-Sized Binary Array given the values and options in the form of
%% a proplist
-spec new(Values :: [serde_arrow_type:erlang_type()], Opts :: map()) -> Array :: #array{}.
new(Values, _Opts) ->
    new(Values).

%% @doc Creates a Variable-Sized Binary Array given the values
-spec new(Values :: [serde_arrow_type:erlang_type()]) -> Array :: #array{}.
new(Values) ->
    Len = length(Values),
    {Bitmap, NullCount} = serde_arrow_bitmap:validity_bitmap(Values),
    Bin = serde_arrow_buffer:new(Values, bin),
    Offsets = serde_arrow_offsets:new(Values, bin),
    #array{
        layout = variable_binary,
        type = bin,
        len = Len,
        null_count = NullCount,
        validity_bitmap = Bitmap,
        offsets = Offsets,
        data = Bin
    }.
