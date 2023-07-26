-module(arrow_format_nif_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("serde_arrow_ipc_message.hrl").

-include("serde_arrow_ipc_marks_data.hrl").

all() ->
    [test_decode, test_encode].

%%%%%%%%%%%%%%%%%
%% test_decode %%
%%%%%%%%%%%%%%%%%

test_decode(_Config) ->
    ?assertEqual(arrow_format_nif:test_decode(?SchemaMsg), ok),

    %% Flatbuffers doesn't need the body. So, don't provide it.
    RecordBatchMsg = (?RecordBatchMsg)#message{body = undefined},
    ?assertEqual(arrow_format_nif:test_decode(RecordBatchMsg), ok).

%%%%%%%%%%%%%%%%%
%% test_encode %%
%%%%%%%%%%%%%%%%%

test_encode(_Config) ->
    ?assertEqual(arrow_format_nif:test_encode(schema), ?SchemaMsg),

    %% Flatbuffers can't access the body. So, the NIF won't return it.
    RecordBatchMsg = (?RecordBatchMsg)#message{body = undefined},
    ?assertEqual(arrow_format_nif:test_decode(record_batch), RecordBatchMsg).
