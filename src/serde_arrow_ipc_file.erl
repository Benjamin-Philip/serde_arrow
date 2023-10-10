%% @doc Provides records and functions to deal with the IPC File.
-module(serde_arrow_ipc_file).
-export([from_erlang/2, to_ipc/1]).

-include("serde_arrow_ipc_message.hrl").
-include("serde_arrow_ipc_file.hrl").

%%%%%%%%%%%%%%%%%%%
%% from_erlang/2 %%
%%%%%%%%%%%%%%%%%%%

%% @doc Creates a file given a schema message and a list of record batch messages.
-spec from_erlang(Schema :: #message{}, RecordBatches :: [#message{}]) -> #file{}.
from_erlang(SchemaMsg, RecordBatches) ->
    SchemaEMF = serde_arrow_ipc_message:to_ipc(SchemaMsg),
    {RecordBatchBlocks, EMFs} = blocks(byte_size(SchemaEMF), RecordBatches, [], []),
    Schema = SchemaMsg#message.header,
    Footer = #footer{
        version = v5, schema = Schema, dictionaries = [], record_batches = RecordBatchBlocks
    },
    Stream = serde_arrow_ipc_message:to_stream([SchemaEMF | EMFs]),
    #file{footer = Footer, body = Stream}.

%% Returns the blocks as wells as the intermediate EMFs used to generate each block.
-spec blocks(
    Offset :: non_neg_integer(),
    Messages :: [#message{}],
    Blocks :: [#block{}],
    EMFs :: [binary()]
) -> {Blocks :: [#block{}], EMFs :: [binary()]}.
blocks(_Offset, [], Blocks, EMFs) ->
    {lists:reverse(Blocks), lists:reverse(EMFs)};
blocks(Offset, [H | T], Blocks, EMFs) ->
    EMF = serde_arrow_ipc_message:to_ipc(H),
    MetadataLen = serde_arrow_ipc_message:metadata_len(EMF),
    Block = #block{
        offset = Offset, metadata_length = MetadataLen, body_length = H#message.body_length
    },

    blocks(Offset + byte_size(EMF), T, [Block | Blocks], [EMF | EMFs]).

%%%%%%%%%%%%%%
%% to_ipc/1 %%
%%%%%%%%%%%%%%

%% @doc Serializes a file into the IPC File Format
-spec to_ipc(File :: #file{}) -> SerializedFile :: binary().
to_ipc(File) ->
    Footer = <<"Footer!">>,
    Sz = byte_size(Footer),
    <<"ARROW1", "00", (File#file.body)/binary, Footer/bitstring, Sz:4/little-signed-integer-unit:8,
        "ARROW1">>.
