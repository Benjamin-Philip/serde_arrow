-module(arrow_format_nif).
-export([test_decode/1, test_encode/1, serialize_message/1, serialize_footer/1]).

-include("cargo.hrl").
-on_load(init/0).
-define(NOT_LOADED, not_loaded(?LINE)).

%%%===================================================================
%%% API
%%%===================================================================

test_decode(_) ->
    ?NOT_LOADED.

test_encode(_) ->
    ?NOT_LOADED.

serialize_message(_) ->
    ?NOT_LOADED.

serialize_footer(_) ->
    ?NOT_LOADED.

%%%===================================================================
%%% NIF
%%%===================================================================

init() ->
    ?load_nif_from_crate(arrow_format_nif, 0).

not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).
