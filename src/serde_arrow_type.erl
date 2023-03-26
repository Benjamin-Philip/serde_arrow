%%%-----------------------------------------------------------------------------
%% @doc The `serde_arrow_type' module provides types in order to represent Apache
%% Arrow specific primitive logical datatypes, as well as Erlang datatypes which
%% `serde_arrow' supports serializing and deserializing.
%% @end
%%%-----------------------------------------------------------------------------
-module(serde_arrow_type).
-export_type([
    arrow_type/0,
    arrow_bool/0,
    arrow_int/0,
    arrow_uint/0,
    arrow_float/0,
    erlang_type/0
]).

-type arrow_type() ::
    arrow_bool()
    | arrow_int()
    | arrow_uint()
    | arrow_float().
%% Any primitive logical type in Apache Arrow that is supported by `serde_arrow'.

-type arrow_bool() ::
    arrow_true
    | arrow_false.
%% Apache Arrow Boolean. One of `True' or `False'

-type arrow_int() ::
    {int, 8}
    | {int, 16}
    | {int, 32}
    | {int, 64}.
%% Any signed integer in Apache Arrow. Includes `Int 8', `Int 16', `Int 32' and
%% `Int 64'.

-type arrow_uint() ::
    {uint, 8}
    | {uint, 16}
    | {uint, 32}
    | {uint, 64}.
%% Any unsigned integer in Apache Arrow. Includes `UInt 8', `UInt 16', `UInt 32'
%% and `UInt 64'.

-type arrow_float() ::
    {float, 16}
    | {float, 32}
    | {float, 64}.
%% Any floating point number in Apache Arrow. Includes `Float 16', `Float 32'
%% and `Float 64'.

-type erlang_type() ::
    boolean()
    | nil
    | integer()
    | float().
%% Any Erlang datatype which `serde_arrow' supports serializing from and
%% deserializing into.