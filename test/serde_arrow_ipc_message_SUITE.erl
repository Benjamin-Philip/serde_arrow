-module(serde_arrow_ipc_message_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("serde_arrow_ipc_message.hrl").

-include("serde_arrow_ipc_marks_data.hrl").

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
        valid_body_on_to_ipc,

        valid_stream_on_to_stream,

        metadata_len,

        body_from_erlang
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
    ?assertEqual(MetadataSize, 300).

valid_metadata_on_to_ipc(_Config) ->
    <<_:32, _:32, MsgMetadata:300/binary, _Rest/binary>> = ?RecordBatchEMF,
    PlainMetadata = arrow_format_nif:serialize_message((?RecordBatchMsg)#message{body = undefined}),
    ?assertEqual(MsgMetadata, PlainMetadata).

valid_body_on_to_ipc(_Config) ->
    <<_:32, _:32, _:300/binary, Body1/binary>> = ?RecordBatchEMF,
    ?assertEqual(Body1, ?Body),

    <<_:32, _:32, _:363/binary, Body2/binary>> = ?SchemaEMF,
    ?assertEqual(Body2, <<>>).

%%%%%%%%%%%%%%%%%
%% to_stream/1 %%
%%%%%%%%%%%%%%%%%

valid_stream_on_to_stream(_Config) ->
    <<Schema:371/binary, RecordBatch:948/binary, EOS/binary>> = ?Stream,

    ?assertEqual(Schema, ?SchemaEMF),
    ?assertEqual(RecordBatch, ?RecordBatchEMF),
    ?assertEqual(EOS, <<-1:32, 0:32>>),

    ?assertEqual(serde_arrow_ipc_message:to_stream([?SchemaEMF, ?RecordBatchEMF]), ?Stream).

%%%%%%%%%%%%%%%%%%%%
%% metadata_len/1 %%
%%%%%%%%%%%%%%%%%%%%

metadata_len(_Config) ->
    ?assertEqual(serde_arrow_ipc_message:metadata_len(?RecordBatchEMF), 300).

%%%%%%%%%%%%%%%%%%%%%%%%
%% body_from_erlang/1 %%
%%%%%%%%%%%%%%%%%%%%%%%%

body_from_erlang(_Config) ->
    ?assertEqual(serde_arrow_ipc_message:body_from_erlang(?Columns), ?Body).
