-module(serde_arrow_ipc_message).
-export([from_erlang/1]).
-export_type([metadata_version/0, key_value/0]).

-include("serde_arrow_ipc_message.hrl").

-type metadata_version() :: v1 | v2 | v3 | v4 | v5.
%% The Arrow version. See the definition for more info:
%% https://github.com/apache/arrow/blob/3456131ab7350bee5d9569ffd63d3f0ee713991c/format/Schema.fbs#L28-L49

-type key_value() :: #{key => string(), value => string()}.
%% Key-Value structure for custom metadata. See the definition for more info:
%% https://github.com/apache/arrow/blob/3456131ab7350bee5d9569ffd63d3f0ee713991c/format/Schema.fbs#L432-L439

-spec from_erlang(Header :: #schema{}) -> Message :: #message{}.
from_erlang(Header) ->
    #message{header = Header, body_length = 0}.
