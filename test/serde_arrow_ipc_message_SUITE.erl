-module(serde_arrow_ipc_message_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("serde_arrow_ipc_message.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Schema Encapsulated Message %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(IDField, serde_arrow_ipc_field:from_erlang({s, 8}, "id")).
-define(NameField, serde_arrow_ipc_field:from_erlang({bin, undefined}, "name")).
-define(AgeField, serde_arrow_ipc_field:from_erlang({u, 8}, "age")).
-define(AnnualMarksField,
    serde_arrow_ipc_field:from_erlang({fixed_list, {u, 8}, 3}, "annual_marks")
).
-define(Fields, [?IDField, ?NameField, ?AgeField, ?AnnualMarksField]).
-define(Schema, serde_arrow_ipc_schema:from_erlang(?Fields)).
-define(SchemaMsg, from_erlang(?Schema)).
-define(SchemaEMF, to_ipc(?SchemaMsg)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% RecordBatch Encapsulated Message %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(ID, serde_arrow_array:from_erlang(fixed_primitive, [0, 1, 2, undefined], {s, 8})).
-define(Name,
    serde_arrow_array:from_erlang(
        variable_binary, [<<"alice">>, <<"bob">>, <<"charlie">>, undefined], {bin, undefined}
    )
).
-define(Age, serde_arrow_array:from_erlang(fixed_primitive, [10, 20, 30, undefined], {s, 8})).
-define(AnnualMarks,
    serde_arrow_array:from_erlang(
        fixed_list, [[100, 97, 98], [100, 99, 96], [100, 98, 95], undefined], {s, 8}
    )
).
-define(Columns, [?ID, ?Name, ?AnnualMarks]).
-define(RecordBatch, serde_arrow_ipc_record_batch:from_erlang(?Columns)).
-define(Body, <<<<(serde_arrow_array:to_arrow(Array))/binary>> || Array <- ?Columns>>).
-define(RecordBatchMsg, from_erlang(?RecordBatch, ?Body)).
-define(RecordBatchEMF, to_ipc(?RecordBatchMsg)).

all() ->
    [
        valid_version_on_from_erlang,
        valid_header_on_from_erlang,
        valid_body_length_on_from_erlang,
        valid_custom_metadata_on_from_erlang,
        valid_body_on_from_erlang,

        valid_continuation_on_to_ipc,
        valid_metadata_size_on_to_ipc,
        valid_metadata_on_to_ipc,
        valid_body_on_to_ipc
    ].

%%%%%%%%%%%%%%%%%
%% from_erlang %%
%%%%%%%%%%%%%%%%%

valid_version_on_from_erlang(_Config) ->
    ?assertEqual((?SchemaMsg)#message.version, v5),
    ?assertEqual((?RecordBatchMsg)#message.version, v5).

valid_header_on_from_erlang(_Config) ->
    ?assertEqual((?SchemaMsg)#message.header, ?Schema),
    ?assertEqual((?RecordBatchMsg)#message.header, ?RecordBatch).

valid_body_length_on_from_erlang(_Config) ->
    ?assertEqual((?SchemaMsg)#message.body_length, 0),
    ?assertEqual((?RecordBatchMsg)#message.body_length, byte_size(?Body)).

valid_custom_metadata_on_from_erlang(_Config) ->
    ?assertEqual((?SchemaMsg)#message.custom_metadata, []),
    ?assertEqual((?RecordBatchMsg)#message.custom_metadata, []).

valid_body_on_from_erlang(_Config) ->
    ?assertEqual((?SchemaMsg)#message.body, undefined),
    ?assertEqual((?RecordBatchMsg)#message.body, ?Body).

%%%%%%%%%%%%%%
%% to_ipc/1 %%
%%%%%%%%%%%%%%

valid_continuation_on_to_ipc(_Config) ->
    <<Continuation:32/signed-integer, _Rest/binary>> = ?RecordBatchEMF,
    ?assertEqual(Continuation, -1).

valid_metadata_size_on_to_ipc(_Config) ->
    <<_:32, MetadataSize:32, _Rest/binary>> = ?RecordBatchEMF,
    ?assertEqual(MetadataSize, 8).

valid_metadata_on_to_ipc(_Config) ->
    <<_:32, _:32, Metadata:8/binary, _Rest/binary>> = ?RecordBatchEMF,
    ?assertEqual(Metadata, <<1, 2, 3, 4, 5, 6, 7, 8>>).

valid_body_on_to_ipc(_Config) ->
    <<_:32, _:32, _:8, Body1>> = ?RecordBatchEMF,
    ?assertEqual(Body1, ?Body),

    <<_:32, _:32, _:8, Body2/binary>> = ?SchemaEMF,
    ?assertEqual(Body2, <<>>).

%%%%%%%%%%%
%% Utils %%
%%%%%%%%%%%

from_erlang(X) ->
    serde_arrow_ipc_message:from_erlang(X).

from_erlang(X, Y) ->
    serde_arrow_ipc_message:from_erlang(X, Y).

to_ipc(Msg) ->
    serde_arrow_ipc_message:to_ipc(Msg).
