%%%-----------------------------------------------------------------------------
%% @doc The `serde_arrow_type' module provides types in order to represent Apache
%% Arrow specific primitive logical datatypes, as well as Erlang datatypes which
%% `serde_arrow' supports serializing and deserializing.
%% @end
%%%-----------------------------------------------------------------------------
-module(serde_arrow_type).
-export_type([
    arrow_type/0,
    arrow_int/0,
    erlang_type/0
]).

-type arrow_type() :: arrow_int().
%% Any primitive logical type in Apache Arrow that is supported by `serde_arrow'.

-type arrow_int() ::
    {int, 8}
    | {int, 16}
    | {int, 32}
    | {int, 64}.
%% Any signed integer in Apache Arrow. Includes `Int 8', `Int 16', `Int 32' and
%% `Int 64'.

-type erlang_type() :: integer().
%% Any Erlang datatype which `serde_arrow' supports serializing from and
%% deserializing into.

