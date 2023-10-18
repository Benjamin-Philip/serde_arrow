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
        valid_custom_metadata_on_from_erlang,
        valid_body_on_from_erlang,

        valid_magic_string_on_to_ipc,
        valid_footer_on_to_ipc,
        valid_stream_on_to_ipc
    ].

%%%%%%%%%%%%%%%%%%%
%% from_erlang/2 %%
%%%%%%%%%%%%%%%%%%%

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

valid_custom_metadata_on_from_erlang(_Config) ->
    ?assertEqual((?File)#file.footer#footer.custom_metadata, []).

valid_body_on_from_erlang(_Config) ->
    ?assertEqual((?File)#file.body, ?Stream).

%%%%%%%%%%%%%%
%% to_ipc/1 %%
%%%%%%%%%%%%%%

valid_magic_string_on_to_ipc(_Config) ->
    StreamSz = byte_size(?Stream) * 8,
    FooterSz = byte_size(?SerializedFile) * 8 - (64 + StreamSz + 32 + 48),
    ?assertMatch(
        <<"ARROW1", "00", __Stream:StreamSz/bitstring, _Footer:FooterSz/bitstring,
            _FooterSz:32/signed-little-integer, "ARROW1">>,
        ?SerializedFile
    ).

valid_footer_on_to_ipc(_Config) ->
    StreamSz = byte_size(?Stream) * 8,
    FooterSz = byte_size(?SerializedFile) * 8 - (64 + StreamSz + 32 + 48),
    <<_ARROW_MAGIC:64/bitstring, _Stream:StreamSz/bitstring, Footer:FooterSz/bitstring,
        _FooterSz:32/signed-little-integer, "ARROW1">> = ?SerializedFile,
    ?assertEqual(Footer, <<"Footer!">>).

valid_stream_on_to_ipc(_Config) ->
    Sz = byte_size(?Stream) * 8,
    <<_ARROW_MAGIC:64/bitstring, Stream:Sz/bitstring, _Rest/bitstring>> = ?SerializedFile,
    ?assertEqual(Stream, ?Stream).
