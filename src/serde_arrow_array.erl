%% @doc Provides a record, a behaviour, and functions to work with Apache Arrow
%% Arrays.
%%
%% This module as mentioned, provides conveniences for working with Apache Arrow
%% Arrays[1] of different Layouts[2]. Firstly, it provides a record to represent
%% all the Layout. Secondly, it provides a behaviour for different layouts to
%% adhere to common functionality. Lastly, it provides functions to work with
%% Arrays in a common manner.
%%
%% == The Structure of an Array ==
%%
%% `serde_arrow''s implementation of an Array has the following fields in its
%% record definition:
%%
%% <ol>
%%  <li>
%%      `layout', of type {@link atom()}, which represents the Layout of the
%%      Array.
%%  </li>
%%  <li>
%%       `type', of type {@link serde_arrow_type:arrow_type()}, which represents
%%       the Logical Type[3] of the Array.
%% </li>
%%  <li>`len', of type {@link pos_integer()}, which represents the Array's Length[4].</li>
%%  <li>
%%      `null_count', of type  {@link non_neg_integer()}, which represents the
%%      Array's Null Count[4], or the number of undefined values in the Array.
%%  </li>
%%  <li>
%%      `validity_bitmap', which is a buffer (`serde_arrow_buffer') or the atom
%%       `undefined', which represents the Array's Validity Bitmap[5].
%%  </li>
%%  <li>
%%      `offsets', which is a buffer (`serde_arrow_buffer') which represents the
%%      Offsets[6], or the start position of each slot in the data buffer of an
%%      Array.
%%  </li>
%%  <li>
%%      `data', which is a buffer (`serde_arrow_buffer'), which represents the
%%      Array's Value Buffer, whose layout differs based on the Array Layout.
%%  </li>
%% </ol>
%%
%% Certain fields are not required for certain layouts. For example, for the
%% Fixed-Sized Primitive Layout, the offsets field is not required, in which
%% case it is assigned as `undefined'. Similarly, the `validity_bitmap' is not
%% required if there are no null values, in which case it is also assigned as
%% `undefined'.
%%
%% == The Behaviour of an Array ==
%%
%% As of right now, a layout needs to implement the `c:new/2' callback. This is
%% then used in the `new/3' function to create new arrays.
%%
%% == Functions for working with Arrays ==
%%
%% As of right now, only functions to access the various fields, and to create
%% new arrays exist.
%%
%% == References ==
%%
%% [1]: [https://arrow.apache.org/docs/format/Glossary.html#term-array]
%%
%% [2]: [https://arrow.apache.org/docs/format/Glossary.html#term-physical-layout]
%%
%% [3]: [https://arrow.apache.org/docs/format/Glossary.html#term-type]
%%
%% [4]: [https://arrow.apache.org/docs/format/Columnar.html#null-count]
%%
%% [5]: [https://arrow.apache.org/docs/format/Columnar.html#validity-bitmaps]
%%
%% [6]: [https://arrow.apache.org/docs/format/Columnar.html#variable-size-binary-layout]
%% @end
-module(serde_arrow_array).
-export([
    new/3,
    layout/1,
    type/1,
    len/1,
    null_count/1,
    validity_bitmap/1,
    offsets/1,
    data/1
]).

-include("serde_arrow_array.hrl").

-export_type([layout/0]).
-type layout() :: fixed_primitive | variable_binary.
%% Represents the Layout of an Array.

%%%%%%%%%%%%%%%%%%%%
%% Array Creation %%
%%%%%%%%%%%%%%%%%%%%

-callback new(Value :: [serde_arrow_type:erlang_type()], Type :: serde_arrow_type:arrow_type()) ->
    Array :: #array{}.
%% Creates a new array of a certain layout, given its value and type.

%% @doc A common way to create a new array, given its layout, value, and type.
-spec new(
    Layout :: layout(),
    Value :: [serde_arrow_type:erlang_type()],
    Type :: serde_arrow_type:arrow_type()
) ->
    Array :: #array{}.
new(fixed_primitive, Value, Type) ->
    serde_arrow_fixed_primitive_array:new(Value, Type).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Array Data and Metadata Access %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Returns the layout of an array.
-spec layout(Array :: #array{}) -> Layout :: layout().
layout(Array) ->
    Array#array.layout.

%% @doc Returns the type of an array.
-spec type(Array :: #array{}) -> Type :: serde_arrow_type:arrow_type().
type(Array) ->
    Array#array.type.

%% @doc Returns the length of an array.
-spec len(Array :: #array{}) -> Length :: pos_integer().
len(Array) ->
    Array#array.len.

%% @doc Returns the null count of an array.
-spec null_count(Array :: #array{}) -> NullCount :: non_neg_integer().
null_count(Array) ->
    Array#array.null_count.

%% @doc Returns the validity bitmap of an array.
-spec validity_bitmap(Array :: #array{}) -> ValidityBitmap :: #buffer{} | undefined.
validity_bitmap(Array) ->
    Array#array.validity_bitmap.

%% @doc Returns the offsets of an array.
-spec offsets(Array :: #array{}) -> Offsets :: #buffer{} | undefined.
offsets(Array) ->
    Array#array.offsets.

%% @doc Returns the data of an array.
-spec data(Array :: #array{}) -> Data :: #buffer{} | undefined.
data(Array) ->
    Array#array.data.
