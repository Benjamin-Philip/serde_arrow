-module(arrow_format_nif).

%% FIXME: Remove once we add some functions
-dialyzer(no_unused).

-include("cargo.hrl").
-on_load(init/0).
-define(NOT_LOADED, not_loaded(?LINE)).

%%%===================================================================
%%% API
%%%===================================================================

%%%===================================================================
%%% NIF
%%%===================================================================

init() ->
    ?load_nif_from_crate(arrow_format_nif, 0).

not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).
