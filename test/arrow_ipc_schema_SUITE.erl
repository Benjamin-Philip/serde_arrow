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

-module(arrow_ipc_schema_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("arrow_ipc_schema.hrl").

-include("arrow_ipc_marks_data.hrl").

all() ->
    [
        valid_endianness_on_from_erlang,
        valid_fields_on_from_erlang,
        valid_custom_metadata_on_from_erlang,
        valid_features_on_from_erlang
    ].

valid_endianness_on_from_erlang(_Config) ->
    ?assertEqual((?Schema)#schema.endianness, little).

valid_fields_on_from_erlang(_Config) ->
    ?assertEqual((?Schema)#schema.fields, ?Fields).

valid_custom_metadata_on_from_erlang(_Config) ->
    ?assertEqual((?Schema)#schema.custom_metadata, []).

valid_features_on_from_erlang(_Config) ->
    ?assertEqual((?Schema)#schema.features, [unused]).
