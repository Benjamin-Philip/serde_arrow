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

-cargo_header_version(1).
-ifndef(CARGO_LOAD_APP).
-define(CARGO_LOAD_APP, arrow).
-endif.
-ifndef(CARGO_HRL).
-define(CARGO_HRL, 1).
-define(load_nif_from_crate(__CRATE, __INIT),
    (fun() ->
        __APP = ?CARGO_LOAD_APP,
        __PATH = filename:join([code:priv_dir(__APP), "crates", __CRATE, __CRATE]),
        erlang:load_nif(__PATH, __INIT)
    end)()
).
-endif.
