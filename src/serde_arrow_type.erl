%%%-----------------------------------------------------------------------------
%% @doc `serde_arrow_type' provides the following functionality:
%%
%% <ol>
%%   <li> Representation of Apache Arrow's primitive logical types.</li>
%%   <li> Serialization of Erlang datatypes into its Arrow equivalent.</li>
%%   <li> Serialization of Arrow datatypes into its Erlang equivalent.</li>
%% </ol>
%%
%% @end
%%%-----------------------------------------------------------------------------
-module(serde_arrow_type).
-export([deserialize/2]).
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

%% @doc Deserializes Erlang values from Apache Arrow values.
%%
%% == Signed Integers ==
%%
%% All signed integers (i.e. `Int 8' to `Int 64') are convereted into erlang
%% integers:
%%
%% ```
%% 1> serde_arrow_type:deserialize(<<-72:1/integer-signed-little-unit:8>>, {int, 8})
%% -72
%% '''
%%
%% ```
%% 2> serde_arrow_type:deserialize(<<-9_223_372_036_854_775_808:1/integer-signed-little-unit:64>>, {int, 64})
%% -9_223_372_036_854_775_808
%% '''
%% @end
-spec deserialize
    (binary(), arrow_type()) -> erlang_type();
    (binary(), arrow_int()) -> integer().

%%-----------
%% Integers
%%-----------

%% For some reason we can't pattern match the unit with a variable as it results
%% in a syntax error. So, we need to write a case for each size manually.

deserialize(Arrow, {int, 8}) ->
    <<Erlang:1/integer-signed-little-unit:8>> = Arrow,
    Erlang;
deserialize(Arrow, {int, 16}) ->
    <<Erlang:1/integer-signed-little-unit:16>> = Arrow,
    Erlang;
deserialize(Arrow, {int, 32}) ->
    <<Erlang:1/integer-signed-little-unit:32>> = Arrow,
    Erlang;
deserialize(Arrow, {int, 64}) ->
    <<Erlang:1/integer-signed-little-unit:64>> = Arrow,
    Erlang.
