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

-module(arrow_test_utils).
-export([byte_buffer/1, pad/1]).

%% This function returns a buffer of type bin, given a binary as input
byte_buffer(Binary) ->
    arrow_buffer:from_erlang(Binary, {bin, undefined}).

%% Returns a binary with 0 padded to a certain length.
pad(ByteLen) ->
    <<<<0>> || _X <- lists:seq(1, ByteLen)>>.
