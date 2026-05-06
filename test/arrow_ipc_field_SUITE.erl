% Licensed to the Apache Software Foundation (ASF) under one
% or more contributor license agreements.  See the NOTICE file
% distributed with this work for additional information
% regarding copyright ownership.  The ASF licenses this file
% to you under the Apache License, Version 2.0 (the
% "License"); you may not use this file except in compliance
% with the License.  You may obtain a copy of the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing,
% software distributed under the License is distributed on an
% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
% KIND, either express or implied.  See the License for the
% specific language governing permissions and limitations
% under the License.

-module(arrow_ipc_field_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("arrow_ipc_field.hrl").

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
    arrow_ipc_field:from_erlang(X).

from_erlang(X, Y) ->
    arrow_ipc_field:from_erlang(X, Y).

from_erlang(X, Y, Z) ->
    arrow_ipc_field:from_erlang(X, Y, Z).
