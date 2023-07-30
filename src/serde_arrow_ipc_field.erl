%% @doc Provides a record and functions to deal with individual fields in
%% a Schema
%%
%% A Field[1] represents a single column in a table (which is represented by a
%% Schema[2]). This module provides a record and a function to manage all the
%% metadata required to represent a column. Metadata such as:
%%
%% <ol>
%%  <li>
%%      `name': The Name of the column. This can either be a string or null.
%%  </li>
%%  <li>
%%      `nullable': A boolean representing whether a column can have null values.
%%      Generally true.
%%  </li>
%%  <li>
%%      `type': The Type of the column, and is of type
%%      `serde_arrow_ipc_type:ipc_type()'
%%  </li>
%%  <li>
%%      `dictionary': A boolean representing if the column is dictionary encoded
%%  </li>
%%  <li>
%%      `children': The child fields of a nested datatype
%%  </li>
%%  <li>
%%      `custom_metadata': A list of custom metadata in key-value format
%%  </li>
%% </ol>
%%
%% Currently, dictionary encoding and custom metadata are not supported, but
%% they have been added for forwards comapatibility.
%%
%% [1]: [https://github.com/apache/arrow/blob/3456131ab7350bee5d9569ffd63d3f0ee713991c/format/Schema.fbs#L469-L492]
%%
%% [2]: [https://github.com/apache/arrow/blob/3456131ab7350bee5d9569ffd63d3f0ee713991c/format/Schema.fbs#L514-L530]
%% @end
-module(serde_arrow_ipc_field).
-export([from_erlang/1, from_erlang/2, from_erlang/3]).

-include("serde_arrow_ipc_schema.hrl").

%% @doc Creates a field given the type of the column. Assigns the name as
%% `undefined'.
-spec from_erlang(Type :: serde_arrow_ipc_type:ipc_type()) -> Field :: #field{}.
from_erlang(Type) ->
    from_erlang(Type, undefined, []).

%% @doc Creates a field given the type and name of the column.
-spec from_erlang(Type :: serde_arrow_ipc_type:ipc_type(), Name :: string() | undefined) ->
    Field :: #field{}.
from_erlang(Type, Name) ->
    from_erlang(Type, Name, []).

%% @doc Creates a field given the type, name and children of the column.
-spec from_erlang(
    Type :: serde_arrow_ipc_type:ipc_type(), Name :: string() | undefined, Children :: [#field{}]
) ->
    Field :: #field{}.
from_erlang(Type, Name, Children) ->
    #field{name = Name, type = Type, children = Children}.
