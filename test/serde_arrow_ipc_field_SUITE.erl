-module(serde_arrow_ipc_field_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("serde_arrow_ipc_field.hrl").

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
    Field1 = from_erlang({s, 8}, "id"),
    ?assertEqual(Field1#field.name, "id"),

    Field2 = from_erlang({s, 8}),
    ?assertEqual(Field2#field.name, undefined).

valid_nullable_on_from_erlang(_Config) ->
    ?assertEqual((from_erlang({s, 8}))#field.nullable, true).

valid_type_on_from_erlang(_Config) ->
    ?assertEqual((from_erlang({s, 8}))#field.type, fixed_primitive),
    ?assertEqual((from_erlang({bin, undefined}))#field.type, variable_binary),
    ?assertEqual((from_erlang({fixed_list, {s, 8}, 4}))#field.type, fixed_list),
    ?assertEqual((from_erlang({variable_list, {s, 8}, undefined}))#field.type, variable_list).

valid_dictionary_on_from_erlang(_Config) ->
    ?assertEqual((from_erlang({s, 8}))#field.dictionary, undefined).

valid_children_on_from_erlang(_Config) ->
    Field1 = from_erlang({s, 8}),
    ?assertEqual(Field1#field.children, []),

    Field2 = from_erlang({bin, undefined}),
    ?assertEqual(Field2#field.children, []),

    Field3 = from_erlang({fixed_list, {s, 8}, 4}),
    ?assertEqual(Field3#field.children, [Field1]),

    Field4 = from_erlang({variable_list, {s, 8}, undefined}),
    ?assertEqual(Field4#field.children, [Field1]).

valid_custom_metadata_on_from_erlang(_Config) ->
    ?assertEqual((from_erlang({s, 8}))#field.custom_metadata, []).

%%%%%%%%%%%
%% Utils %%
%%%%%%%%%%%

from_erlang(X) ->
    serde_arrow_ipc_field:from_erlang(X).

from_erlang(X, Y) ->
    serde_arrow_ipc_field:from_erlang(X, Y).
