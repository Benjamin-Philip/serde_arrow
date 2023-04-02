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
%%      Null Count, or the number of nil values in the Array.
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
    new/2,
    type/1,
    len/1,
    null_count/1,
    validity_bitmap/1
]).

-include("serde_arrow_primitive_array.hrl").

%%%%%%%%%%%%%%%%%%%%
%% Array Creation %%
%%%%%%%%%%%%%%%%%%%%

%% TODO: Write better documentation.

-spec new(Value :: [serde_arrow_type:erlang_type()], Type :: serde_arrow_type:arrow_type()) ->
    Array :: #primitive_array{}.
%% @doc Creates a new primitive array.
%% @end
new(Value, Type) ->
    Len = length(Value),
    {NullCount, Bitmap} = validity(Value, Len),
    Bin = buffer(Value, Type),
    #primitive_array{
        type = Type, len = Len, null_count = NullCount, validity_bitmap = Bitmap, value = Bin
    }.

%% TODO: Extract helper functions to a utils module.

validity(Value, Len) ->
    ValidityList = lists:map(
        fun(X) ->
            case X of
                nil ->
                    0;
                _ ->
                    1
            end
        end,
        Value
    ),
    NullCount = Len - lists:sum(ValidityList),
    case NullCount of
        0 ->
            {0, nil};
        _ ->
            {NullCount, bitmap(ValidityList, Len)}
    end.

bitmap(ValidityList, Len) ->
    %% We need to split the list every 8th element and then pad the very last
    %% element such that every resulting list has a length of 8 elements.
    %%
    %% If we try to split a list with only 8 elements, we will get a copy of the
    %% list and an empty list in the resulting tuple. In order to get over this
    %% empty list, we pad one bit extra and drop the final list.

    PadLen = 9 - Len rem 8,
    Padding = lists:duplicate(PadLen, 0),
    SplitValidityList = lists:droplast(
        tuple_to_list(lists:split(8, lists:append(ValidityList, Padding)))
    ),

    %% So far each bit has been represented by an Erlang integer.
    %% Here we do two things:
    %%
    %% 1. We reverse the order of bits in each byte in order to repsect Arrow's
    %%    bit-endianness: https://en.wikipedia.org/wiki/Bit_numbering
    %%
    %% 2. We convert the integer bits to actual bits.
    %%
    %% We are doing 2 distinct operations over a single list traversal as it may
    %% have a noticeable performance difference for large validity buffer.
    %% We also don't generate the initial ValidityList as a binary because we
    %% need its sum for the null count.

    Bytes = lists:map(fun(Byte) -> <<<<X:1>> || X <- lists:reverse(Byte)>> end, SplitValidityList),
    %% buffer(Bytes, byte)
    Bytes.

%% TODO: Write a buffer implementation.

buffer(_, _) ->
    "Foobar!".

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Primitive Array Data and Metadata Access %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec type(Array :: #primitive_array{}) -> Type :: serde_arrow_type:arrow_type().
type(Array) ->
    Array#primitive_array.type.

-spec len(Array :: #primitive_array{}) -> Len :: pos_integer().
len(Array) ->
    Array#primitive_array.len.

-spec null_count(Array :: #primitive_array{}) -> NullCount :: pos_integer().
null_count(Array) ->
    Array#primitive_array.null_count.

-spec validity_bitmap(Array :: #primitive_array{}) -> ValidityBitmap :: binary() | nil.
validity_bitmap(Array) ->
    Array#primitive_array.validity_bitmap.
