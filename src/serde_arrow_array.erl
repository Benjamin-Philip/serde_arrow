-module(serde_arrow_array).
-export([offsets/1]).

-include("serde_arrow_primitive_array.hrl").

-export_type([array/0]).
-type array() :: #primitive_array{}.

-optional_callbacks([offsets/1]).

%% TODO: Write better documentation.

%%%%%%%%%%%%%%%%%%%%
%% Array Creation %%
%%%%%%%%%%%%%%%%%%%%

-callback new(Value :: [serde_arrow_type:erlang_type()], Type :: serde_arrow_type:arrow_type()) ->
    Array :: array().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Array Data and Metadata Access %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-callback type(Array :: array()) -> Type :: serde_arrow_type:arrow_type().
%% Returns the type of an array.

-callback len(Array :: array()) -> Length :: pos_integer().
%% Returns the length of an array.

-callback null_count(Array :: array()) -> NullCount :: pos_integer().
%% Returns the null count of an array.

-callback validity_bitmap(Array :: array()) -> ValidityBitmap :: binary() | nil.
%% Returns the validity bitmap of an array.

-callback offsets(Array :: array()) -> Offsets :: [pos_integer()] | nil.
%% Returns the offsets of an array.

-spec offsets(_Array :: array()) -> Offsets :: nil.
offsets(_Array) ->
    nil.
