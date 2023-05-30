%% @doc Provides support for Arrow's Variable-Sized Binary Layout.
%%
%% The Variable-Sized Binary Layout[1] provides support for storing binaries of
%% varying length in a way similar to the primitive layout, i.e. in a 1
%% Dimensional Array.
-module(serde_arrow_variable_binary_array).
-behaviour(serde_arrow_array).

-export([from_erlang/1, from_erlang/2]).

-include("serde_arrow_array.hrl").

%% @doc Creates a Variable-Sized Binary Array given the values and options in the form of
%% a map, from its erlang representation.
-spec from_erlang(Values :: [serde_arrow_type:native_type()], Opts :: map()) -> Array :: #array{}.
from_erlang(Values, _Opts) ->
    from_erlang(Values).

%% @doc Creates a Variable-Sized Binary Array given the values
-spec from_erlang(Values :: [serde_arrow_type:native_type()]) -> Array :: #array{}.
from_erlang(Values) ->
    Len = length(Values),
    {Bitmap, NullCount} = serde_arrow_bitmap:validity_bitmap(Values),
    Offsets = serde_arrow_offsets:new(Values, {bin, undefined}, Len),
    Bin = <<X || X <- Values, X =/= undefined, X =/= nil>>,
    Data = serde_arrow_buffer:from_erlang(Bin, {bin, undefined}),
    #array{
        layout = variable_binary,
        type = {bin, undefined},
        len = Len,
        null_count = NullCount,
        validity_bitmap = Bitmap,
        offsets = Offsets,
        data = Data
    }.
