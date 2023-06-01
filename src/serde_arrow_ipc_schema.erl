%% @doc Provides a record and functions to deal with Schemas
%%
%% A Schema[1] represents a table, or a list of arrays of equal length. This
%% module provides a record and a function to manage all the metadata required
%% to represent a schema. Metadata such as:
%%
%% <ol>
%%  <li>
%%      `endianness': The Endianness of the table. One of `little' or `big'.
%%      Defaults to `little'.
%%  </li>
%%  <li>
%%      `fields': The list of fields[2] in a table.
%%  </li>
%%  <li>
%%      `type': The Layout of the column
%%  </li>
%%  <li>
%%      `custom_metadata': A list of custom metadata in key-value format
%%  </li>
%%  <li>
%%      `features': Any features used by the table which may not be present in
%%      other implementations of Arrow.
%%  </li>
%% </ol>
%%
%% Currently, big endianness, custom metadata and features are not supported,
%% but they have been added for forwards comapatibility.
%%
%% You can find Schemas in the Arrow spec here[3].
%%
%% [1]: [https://github.com/apache/arrow/blob/3456131ab7350bee5d9569ffd63d3f0ee713991c/format/Schema.fbs#L514-L530]
%%
%% [2]: [https://github.com/apache/arrow/blob/3456131ab7350bee5d9569ffd63d3f0ee713991c/format/Schema.fbs#L469-L492]
%%
%% [3]: [https://arrow.apache.org/docs/format/Columnar.html#schema-message]
%% @end
-module(serde_arrow_ipc_schema).
-export([from_erlang/1]).
-export_type([endianness/0, feature/0]).

-include("serde_arrow_ipc_schema.hrl").

-type endianness() :: little | big.
%% Endianness of the data. Either `little' or `big'.

-type feature() :: unused | dictionary_replacement | compressed_body.
%% Features used in the data which may not be present in other implementations.
%% See the definition:
%% [https://github.com/apache/arrow/blob/3456131ab7350bee5d9569ffd63d3f0ee713991c/format/Schema.fbs#L51-L78]

%% @doc Creates a Schema given an ordered list of fields.
-spec from_erlang(Fields :: [#field{}]) -> Schema :: #schema{}.
from_erlang(Fields) ->
    #schema{fields = Fields}.
