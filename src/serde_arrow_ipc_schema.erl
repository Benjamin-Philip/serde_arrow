-module(serde_arrow_ipc_schema).
-export([from_erlang/1]).
-export_type([endianness/0, feature/0]).

-include("serde_arrow_ipc_schema.hrl").

-type endianness() :: little | big.
%% Endianness of the data

-type feature() :: unused | dictionary_replacement | compressed_body.
%% Features used in the data which may not be present in other implementations.
%% See the definition:
%%
%% https://github.com/apache/arrow/blob/3456131ab7350bee5d9569ffd63d3f0ee713991c/format/Schema.fbs#L51-L78

-spec from_erlang(Fields :: [#field{}]) -> Schema :: #schema{}.
from_erlang(Fields) ->
    #schema{fields = Fields}.
