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

-module(arrow_utils_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [
        nesting,
        flatten,
        pad_len
    ].

nesting(_Config) ->
    ?assertEqual(arrow_utils:nesting([1, 2, 3]), 1),
    ?assertEqual(arrow_utils:nesting([undefined, nil, 3]), 1),
    ?assertEqual(arrow_utils:nesting([undefined, nil]), 1),
    ?assertEqual(arrow_utils:nesting([[1], [2], [3]]), 2),
    ?assertEqual(arrow_utils:nesting([[[[[1]]]]]), 5).

flatten(_Config) ->
    ?assertEqual(arrow_utils:flatten([1, 2, 3]), [1, 2, 3]),
    ?assertEqual(arrow_utils:flatten([[1, 2, 3]]), [1, 2, 3]),
    ?assertEqual(arrow_utils:flatten([[1], [2], [3]]), [1, 2, 3]),
    ?assertEqual(arrow_utils:flatten([[[[[1, 2], [3, 4]]]]]), [[[[1, 2], [3, 4]]]]),
    ?assertEqual(arrow_utils:flatten([[1], undefined, [2], nil, [3]]), [1, 2, 3]),
    ?assertEqual(
        arrow_utils:flatten([[1], undefined, [2], nil, [3]], fun() -> [foobar] end, 1),
        [
            1, foobar, 2, foobar, 3
        ]
    ),
    ?assertError(
        badarg,
        arrow_utils:flatten([[1], undefined, [2], nil, [3]], fun() -> [foobar] end, 2)
    ).

pad_len(_Config) ->
    ?assertEqual(arrow_utils:pad_len(10), 54),
    ?assertEqual(arrow_utils:pad_len(64), 0),
    ?assertEqual(arrow_utils:pad_len(74), 54),

    ?assertEqual(arrow_utils:pad_len(33, 8), 7).
