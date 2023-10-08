-module(serde_arrow_ipc_file_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("serde_arrow_ipc_message.hrl").
-include("serde_arrow_ipc_file.hrl").

-include("serde_arrow_ipc_marks_data.hrl").

all() ->
    [
        valid_version_on_from_erlang,
        valid_schema_on_from_erlang,
        valid_dictionaries_on_from_erlang,
        valid_record_batches_on_from_erlang,
        valid_body_on_from_erlang
    ].

%%%%%%%%%%%%%%%%%
%% from_erlang %%
%%%%%%%%%%%%%%%%%

valid_version_on_from_erlang(_Config) ->
    ?assertEqual((?File)#file.footer#footer.version, v5).

valid_schema_on_from_erlang(_Config) ->
    ?assertEqual((?File)#file.footer#footer.schema, ?Schema).

valid_dictionaries_on_from_erlang(_Config) ->
    ?assertEqual((?File)#file.footer#footer.dictionaries, []).

valid_record_batches_on_from_erlang(_Config) ->
    RecordBatchBlock = #block{
        offset = byte_size(?SchemaEMF),
        metadata_length = serde_arrow_ipc_message:metadata_len(?RecordBatchEMF),
        body_length = (?RecordBatchMsg)#message.body_length
    },
    ?assertEqual((?File)#file.footer#footer.record_batches, [RecordBatchBlock]).

valid_body_on_from_erlang(_Config) ->
    ?assertEqual((?File)#file.body, ?Stream).
