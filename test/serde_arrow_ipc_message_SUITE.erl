-module(serde_arrow_ipc_message_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("serde_arrow_ipc_message.hrl").

-define(IDField, serde_arrow_ipc_field:from_erlang({s, 8}, "id")).
-define(NameField, serde_arrow_ipc_field:from_erlang({bin, undefined}, "name")).
-define(AgeField, serde_arrow_ipc_field:from_erlang({u, 8}, "age")).
-define(AnnualMarksField,
    serde_arrow_ipc_field:from_erlang({fixed_list, {u, 8}, 5}, "annual_marks")
).
-define(Schema, [?IDField, ?NameField, ?AgeField, ?AnnualMarksField]).

all() ->
    [
        valid_version_on_from_erlang,
        valid_header_on_from_erlang,
        valid_body_length_on_from_erlang,
        valid_custom_metadata_on_from_erlang,
        valid_body_on_from_erlang
    ].

valid_version_on_from_erlang(_Config) ->
    ?assertEqual((from_erlang(?Schema))#message.version, v5).

valid_header_on_from_erlang(_Config) ->
    ?assertEqual((from_erlang(?Schema))#message.header, ?Schema).

valid_body_length_on_from_erlang(_Config) ->
    ?assertEqual((from_erlang(?Schema))#message.body_length, 0).

valid_custom_metadata_on_from_erlang(_Config) ->
    ?assertEqual((from_erlang(?Schema))#message.custom_metadata, []).

valid_body_on_from_erlang(_Config) ->
    ?assertEqual((from_erlang(?Schema))#message.body, undefined).

%%%%%%%%%%%
%% Utils %%
%%%%%%%%%%%

from_erlang(X) ->
    serde_arrow_ipc_message:from_erlang(X).

from_erlang(X, Y) ->
    serde_arrow_ipc_message:from_erlang(X, Y).
