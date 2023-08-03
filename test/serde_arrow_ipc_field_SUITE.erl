-module(serde_arrow_ipc_field_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("serde_arrow_ipc_field.hrl").

-define(S8, {int, #{bit_width => 8, is_signed => true}}).

all() ->
    [
        valid_name_on_from_erlang,
        valid_nullable_on_from_erlang,
        valid_type_on_from_erlang,
        valid_dictionary_on_from_erlang,
        valid_children_on_from_erlang,
        valid_custom_metadata_on_from_erlang
    ].

valid_name_on_from_erlang(_Config) ->
    Field1 = from_erlang(?S8, "id"),
    ?assertEqual(Field1#field.name, "id"),

    Field2 = from_erlang(?S8),
    ?assertEqual(Field2#field.name, undefined).

valid_nullable_on_from_erlang(_Config) ->
    ?assertEqual((from_erlang(?S8))#field.nullable, true).

valid_type_on_from_erlang(_Config) ->
    ?assertEqual((from_erlang(?S8))#field.type, ?S8).

valid_dictionary_on_from_erlang(_Config) ->
    ?assertEqual((from_erlang(?S8))#field.dictionary, undefined).

valid_children_on_from_erlang(_Config) ->
    Field1 = from_erlang(?S8),
    ?assertEqual(Field1#field.children, []),

    Field2 = from_erlang(large_list, undefined, [Field1]),
    ?assertEqual(Field2#field.children, [Field1]).

valid_custom_metadata_on_from_erlang(_Config) ->
    ?assertEqual((from_erlang(?S8))#field.custom_metadata, []).

%%%%%%%%%%%
%% Utils %%
%%%%%%%%%%%

from_erlang(X) ->
    serde_arrow_ipc_field:from_erlang(X).

from_erlang(X, Y) ->
    serde_arrow_ipc_field:from_erlang(X, Y).

from_erlang(X, Y, Z) ->
    serde_arrow_ipc_field:from_erlang(X, Y, Z).
