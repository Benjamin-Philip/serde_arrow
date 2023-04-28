%%%-----------------------------------------------------------------------------
%% @doc The `serde_arrow_type' module provides types in order to represent Apache
%% Arrow specific primitive logical datatypes, as well as Erlang datatypes which
%% `serde_arrow' supports serializing and deserializing.
%% @end
%%%-----------------------------------------------------------------------------
-module(serde_arrow_type).
-export([bit_length/1, byte_length/1, serialize/2]).
-export_type([
    arrow_type/0,
    arrow_bool/0,
    arrow_int/0,
    arrow_uint/0,
    arrow_float/0,
    arrow_bin/0,
    erlang_type/0
]).

-type arrow_type() ::
    arrow_bool()
    | arrow_int()
    | arrow_uint()
    | arrow_float()
    | arrow_bin().
%% Any primitive logical type in Apache Arrow that is supported by `serde_arrow'.

-type arrow_bool() :: bool.
%% Apache Arrow Boolean. One of `True' or `False'

-type arrow_int() ::
    {s, 8}
    | {s, 16}
    | {s, 32}
    | {s, 64}.
%% Any signed integer in Apache Arrow. Includes `Int 8', `Int 16', `Int 32' and
%% `Int 64'.

-type arrow_uint() ::
    {u, 8}
    | {u, 16}
    | {u, 32}
    | {u, 64}.
%% Any unsigned integer in Apache Arrow. Includes `UInt 8', `UInt 16', `UInt 32'
%% and `UInt 64'.

-type arrow_float() ::
    {f, 16}
    | {f, 32}
    | {f, 64}.
%% Any floating point number in Apache Arrow. Includes `Float 16', `Float 32'
%% and `Float 64'.

-type arrow_bin() :: bin.
%% Any binary whose length in bits is a multiple of 8.

-type erlang_type() ::
    boolean()
    | undefined
    | nil
    | integer()
    | float()
    | binary().
%% Any Erlang datatype which `serde_arrow' supports serializing from and
%% deserializing into.

-spec bit_length(Type :: arrow_type()) -> Length :: pos_integer() | undefined.
%% @doc Returns the size of the type in bits.
bit_length({Type, Size}) when (Type =:= s) orelse (Type =:= u) orelse (Type =:= f) ->
    Size;
bit_length(bool) ->
    1;
bit_length(bin) ->
    erlang:error(badarg).

-spec byte_length(Type :: arrow_type()) -> Length :: pos_integer() | undefined.
%% @doc Returns the size of the type in bytes.
byte_length(bool) ->
    %% This is a stub function.
    %% TODO Find out the Arrow convention for Boolean Buffers
    1;
byte_length(bin) ->
    undefined;
byte_length(Type) ->
    round(bit_length(Type) / 8).

serialize(Value, {s, Size}) ->
    <<Value:Size/little-signed-integer>>;
serialize(Value, {u, Size}) ->
    <<Value:Size/little-unsigned-integer>>;
serialize(Value, {f, Size}) ->
    <<Value:Size/little-float>>;
serialize(Value, bin) ->
    Value.
