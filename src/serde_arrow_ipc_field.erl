-module(serde_arrow_ipc_field).
-export([from_erlang/1, from_erlang/2]).

-include("serde_arrow_ipc_schema.hrl").

-spec from_erlang(Type :: serde_arrow_type:arrow_longhand_type()) -> Field :: #field{}.
from_erlang(Type) ->
    from_erlang(Type, undefined).

-spec from_erlang(Type :: serdelayoutow_type:arrow_longhand_type(), Name :: string() | undefined) ->
    Field :: #field{}.
from_erlang(Type, Name) when tuple_size(Type) =:= 2 ->
    #field{name = Name, type = layout(Type)};
from_erlang({_, NestedType, _} = Type, Name) ->
    #field{name = Name, type = layout(Type), children = [from_erlang(NestedType)]}.

-spec layout(Type :: serde_arrow_type:arrow_longhand_type()) ->
    Layout :: serde_arrow_array:layout().
layout({_, Size}) when is_integer(Size) ->
    fixed_primitive;
layout({_, undefined}) ->
    variable_binary;
layout({Layout, _, _}) ->
    Layout.
