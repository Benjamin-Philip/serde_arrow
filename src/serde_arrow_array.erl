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
-type layout() :: primitive.

%% TODO: Write better documentation.

%%%%%%%%%%%%%%%%%%%%
%% Array Creation %%
%%%%%%%%%%%%%%%%%%%%

-callback new(Value :: [serde_arrow_type:erlang_type()], Type :: serde_arrow_type:arrow_type()) ->
    Array :: #array{}.
%% Creates a new array, given its value and type.

%% A common way to create a new array, given its layout, value, and type.
-spec new(
    Layout :: layout(),
    Value :: [serde_arrow_type:erlang_type()],
    Type :: serde_arrow_type:arrow_type()
) ->
    Array :: #array{}.
new(primitive, Value, Type) ->
    serde_arrow_primitive_array:new(Value, Type).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Array Data and Metadata Access %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Returns the layout of an array.
-spec layout(Array :: #array{}) -> Layout :: layout().
layout(Array) ->
    Array#array.layout.

%% Returns the type of an array.
-spec type(Array :: #array{}) -> Type :: serde_arrow_type:arrow_type().
type(Array) ->
    Array#array.type.

%% Returns the length of an array.
-spec len(Array :: #array{}) -> Length :: pos_integer().
len(Array) ->
    Array#array.len.

%% Returns the null count of an array.
-spec null_count(Array :: #array{}) -> NullCount :: pos_integer().
null_count(Array) ->
    Array#array.null_count.

%% Returns the validity bitmap of an array.
-spec validity_bitmap(Array :: #array{}) -> ValidityBitmap :: #buffer{} | undefined.
validity_bitmap(Array) ->
    Array#array.validity_bitmap.

%% Returns the offsets of an array.
-spec offsets(Array :: #array{}) -> Offsets :: #buffer{} | undefined.
offsets(Array) ->
    Array#array.offsets.

%% Returns the data of an array.
-spec data(Array :: #array{}) -> Data :: #buffer{} | undefined.
data(Array) ->
    Array#array.data.
