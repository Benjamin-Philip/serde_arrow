%% @doc Provides support for
%% <a href="https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout">
%% Arrow's Fixed-Size Primitive Layout
%% </a>.
%%
%% The Primitive Layout has the following metadata and is implemented in Erlang
%% using a record:
%%
%% <ol>
%%  <li>
%%       `type', of type {@link serde_arrow_type:arrow_type()}, which represents
%%       the Logical Type of the Array.
%% </li>
%%  <li>`len', of type `//Erlang//pos_integer()', which represents the Array's Length.</li>
%%  <li>
%%      `null_count', of type  `pos_integer()', which represents the Array's
%%      Null Count, or the number of undefined values in the Array.
%%  </li>
%%  <li>
%%      `validity_bitmap', of type  `binary()', which represents the Array's
%%      <a href="https://arrow.apache.org/docs/format/Columnar.html#validity-bitmaps">
%%         Validity Bitmap
%%      </a>.
%%  </li>
%%  <li>
%%      `value', of type  `binary()', which represents the Array's Value Buffer.
%%  </li>
%% </ol>
%% @end
-module(serde_arrow_primitive_array).
-behaviour(serde_arrow_array).

-export([
    new/2
    %% type/1,
    %% len/1,
    %% null_count/1,
    %% validity_bitmap/1,
    %% data/1
]).

-include("serde_arrow_array.hrl").

%%%%%%%%%%%%%%%%%%%%
%% Array Creation %%
%%%%%%%%%%%%%%%%%%%%

%% TODO: Write better documentation.

-spec new(Value :: [serde_arrow_type:erlang_type()], Type :: serde_arrow_type:arrow_type()) ->
    Array :: #array{}.
%% @doc Creates a new primitive array.
%% @end
new(Value, Type) ->
    Len = length(Value),
    {Bitmap, NullCount} = serde_arrow_bitmap:validity_bitmap(Value),
    Bin = serde_arrow_buffer:new(Value, Type),
    #array{
        layout = primitive,
        type = Type,
        len = Len,
        null_count = NullCount,
        validity_bitmap = Bitmap,
        data = Bin
    }.
