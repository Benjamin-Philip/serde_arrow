%% @doc Provides a record and functions to deal with the Encapsulated Message
%% Format.
%%
%% A Message is a serialized form of a Schema[1] or a RecordBatch[2] (which may
%% be required to read a serialized array) along with some metadata. This module
%% provides a record and a function to manage all the metadata required to
%% represent a message. Metadata such as:
%%
%% <ol>
%%  <li>
%%      `version': The Apache Arrow Format Version. One of v1..v5. Defaults to v5.
%%  </li>
%%  <li>
%%      `header': The metadata of the Schema or RecordBatch
%%  </li>
%%  <li>
%%      `body_length': The length of the body in bytes
%%  </li>
%%  <li>
%%      `custom_metadata': A list of custom metadata in key-value format
%%  </li>
%%  <li>
%%      `body': The actual body. Can be undefined (in the case of Schema) or a
%%      binary (in the case of Record Batch).
%%  </li>
%% </ol>
%%
%% Currently, changing the version and custom metadata are not supported, but
%% they have been added for forwards comapatibility.
%%
%% This module also provides the `to_ipc/1' function which serializes the
%% message into the Encapsulated Message Format[3]. However, this function gives
%% <em>incomplete output</em> with invalid metadata, because of an unsatisfied
%% dependency on flatbuffers, which is required for serializing the metadata.
%%
%% [1]: [https://arrow.apache.org/docs/format/Columnar.html#schema-message]
%%
%% [2]: [https://arrow.apache.org/docs/format/Columnar.html#recordbatch-message]
%%
%% [3]: [https://arrow.apache.org/docs/format/Columnar.html#encapsulated-message-format]
%% @end
-module(serde_arrow_ipc_message).
-export([from_erlang/1, from_erlang/2, to_ipc/1, to_stream/1, metadata_len/1, body_from_erlang/1]).
-export_type([metadata_version/0, key_value/0]).

-include("serde_arrow_ipc_message.hrl").

-type metadata_version() :: v1 | v2 | v3 | v4 | v5.
%% The Arrow version. See the definition for more info:
%% [https://github.com/apache/arrow/blob/3456131ab7350bee5d9569ffd63d3f0ee713991c/format/Schema.fbs#L28-L49]

-type key_value() :: #{key => string(), value => string()}.
%% Key-Value structure for custom metadata. See the definition for more info:
%% [https://github.com/apache/arrow/blob/3456131ab7350bee5d9569ffd63d3f0ee713991c/format/Schema.fbs#L432-L439]

%% @doc Creates a message given a data header.
-spec from_erlang(Header :: #schema{} | #record_batch{}) -> Message :: #message{}.
from_erlang(Header) ->
    #message{header = Header, body_length = 0}.

%% @doc Creates a message given a data header and a body.
-spec from_erlang(Header :: #schema{} | #record_batch{}, Body :: binary()) -> Message :: #message{}.
from_erlang(Header, Body) ->
    #message{header = Header, body = Body, body_length = byte_size(Body)}.

%% @doc Serializes a message into the Encapsulated Message Format.
-spec to_ipc(Message :: #message{}) -> EMF :: binary().
to_ipc(Message) ->
    %% 0xFFFFFFFF in int32
    Continuation = <<-1:32>>,
    %% This is a stub value till we can serialize flatbuffers
    Metadata = arrow_format_nif:serialize_message(Message#message{body = undefined}),
    MetadataSize = <<(byte_size(Metadata)):32>>,
    Body =
        case Message#message.body of
            undefined ->
                <<>>;
            Bin ->
                Bin
        end,

    <<Continuation/binary, MetadataSize/binary, Metadata/binary, Body/binary>>.

%% @doc Serializes a list of messages or EMFs into a Stream.
-spec to_stream(Messages :: [#message{}] | [binary()]) -> Stream :: binary().
to_stream([H | _] = Messages) ->
    Msgs =
        if
            is_tuple(H) -> <<(to_ipc(Msg)) || Msg <- Messages>>;
            is_binary(H) -> <<Msg || Msg <- Messages>>
        end,

    %% 0xFFFFFFFF 0x00000000
    %% This is technically an EMF with zero length metadata and body
    EOS = <<-1:32, 0:32>>,

    <<Msgs/binary, EOS/binary>>.

%%%%%%%%%%%%%%%%%%%%
%% metadata_len/1 %%
%%%%%%%%%%%%%%%%%%%%

%% @doc Returns the Metadata Length of an EMF.
-spec metadata_len(EMF :: binary()) -> MetadataLen :: non_neg_integer().
metadata_len(EMF) ->
    <<_:32, MetadataLen:32, _Rest/binary>> = EMF,
    MetadataLen.

%%%%%%%%%%%%%%%%%%%%%%%%
%% body_from_erlang/1 %%
%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Returns the body of message from a list of arrays.
%%
%% Shorthand for:
%% ```
%% <<<<(serde_arrow_array:to_arrow(Array))/binary>> || Array <- Columns>>
%% '''
%% @end
-spec body_from_erlang(Columns :: [#array{}]) -> Body :: binary().
body_from_erlang(Columns) ->
    <<<<(serde_arrow_array:to_arrow(Array))/binary>> || Array <- Columns>>.
