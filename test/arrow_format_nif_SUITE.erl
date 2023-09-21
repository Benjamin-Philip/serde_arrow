-module(arrow_format_nif_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("serde_arrow_ipc_message.hrl").

-include("serde_arrow_ipc_marks_data.hrl").

-define(S8, schema({int, #{bit_width => 8, is_signed => true}})).
-define(S16, schema({int, #{bit_width => 16, is_signed => true}})).
-define(S32, schema({int, #{bit_width => 32, is_signed => true}})).
-define(S64, schema({int, #{bit_width => 64, is_signed => true}})).

-define(U8, schema({int, #{bit_width => 8, is_signed => false}})).
-define(U16, schema({int, #{bit_width => 16, is_signed => false}})).
-define(U32, schema({int, #{bit_width => 32, is_signed => false}})).
-define(U64, schema({int, #{bit_width => 64, is_signed => false}})).

-define(F16, schema({floating_point, #{precision => half}})).
-define(F32, schema({floating_point, #{precision => single}})).
-define(F64, schema({floating_point, #{precision => double}})).

-define(FixedSizeList, schema({fixed_size_list, #{list_size => 3}})).
-define(LargeBinary, schema(large_binary)).
-define(LargeList, schema(large_list)).

all() ->
    [test_decode, test_encode, serialize_message].

%%%%%%%%%%%%%%%%%
%% test_decode %%
%%%%%%%%%%%%%%%%%

test_decode(_Config) ->
    %% Type testing

    %% Signed Integers
    ?assertEqual(arrow_format_nif:test_decode(?S8), ok),
    ?assertEqual(arrow_format_nif:test_decode(?S16), ok),
    ?assertEqual(arrow_format_nif:test_decode(?S32), ok),
    ?assertEqual(arrow_format_nif:test_decode(?S64), ok),

    %% Unsigned Integers
    ?assertEqual(arrow_format_nif:test_decode(?U8), ok),
    ?assertEqual(arrow_format_nif:test_decode(?U16), ok),
    ?assertEqual(arrow_format_nif:test_decode(?U32), ok),
    ?assertEqual(arrow_format_nif:test_decode(?U64), ok),

    %% Floating Point
    ?assertEqual(arrow_format_nif:test_decode(?F16), ok),
    ?assertEqual(arrow_format_nif:test_decode(?F32), ok),
    ?assertEqual(arrow_format_nif:test_decode(?F64), ok),

    %% Fixed-Size List
    ?assertEqual(arrow_format_nif:test_decode(?FixedSizeList), ok),

    %% Large Binary
    ?assertEqual(arrow_format_nif:test_decode(?LargeBinary), ok),

    %% Large List
    ?assertEqual(arrow_format_nif:test_decode(?LargeList), ok),

    %% Test using the Marks Data
    ?assertEqual(arrow_format_nif:test_decode(?SchemaMsg), ok),

    %% Flatbuffers doesn't need the body. So, don't provide it.
    RecordBatchMsg = (?RecordBatchMsg)#message{body = undefined},
    ?assertEqual(arrow_format_nif:test_decode(RecordBatchMsg), ok).

%%%%%%%%%%%%%%%%%
%% test_encode %%
%%%%%%%%%%%%%%%%%

test_encode(_Config) ->
    ?assertEqual(arrow_format_nif:test_encode(schema), ?SchemaMsg),

    %% Flatbuffers can't access the body. So, the NIF won't return it.
    RecordBatchMsg = (?RecordBatchMsg)#message{body = undefined},
    ?assertEqual(arrow_format_nif:test_encode(record_batch), RecordBatchMsg).

%%%%%%%%%%%%%%%%%%%%%%%
%% serialize_message %%
%%%%%%%%%%%%%%%%%%%%%%%

serialize_message(_Config) ->
    %% Type testing

    %% Signed Integers
    ?assert(is_binary(arrow_format_nif:serialize_message(?S8))),
    ?assert(is_binary(arrow_format_nif:serialize_message(?S16))),
    ?assert(is_binary(arrow_format_nif:serialize_message(?S32))),
    ?assert(is_binary(arrow_format_nif:serialize_message(?S64))),

    %% Unsigned Integers
    ?assert(is_binary(arrow_format_nif:serialize_message(?U8))),
    ?assert(is_binary(arrow_format_nif:serialize_message(?U16))),
    ?assert(is_binary(arrow_format_nif:serialize_message(?U32))),
    ?assert(is_binary(arrow_format_nif:serialize_message(?U64))),

    %% Floating Point
    ?assert(is_binary(arrow_format_nif:serialize_message(?F16))),
    ?assert(is_binary(arrow_format_nif:serialize_message(?F32))),
    ?assert(is_binary(arrow_format_nif:serialize_message(?F64))),

    %% Fixed-Size List
    ?assert(is_binary(arrow_format_nif:serialize_message(?FixedSizeList))),

    %% Large Binary
    ?assert(is_binary(arrow_format_nif:serialize_message(?LargeBinary))),

    %% Large List
    ?assert(is_binary(arrow_format_nif:serialize_message(?LargeList))),

    %% Test using the Marks Data
    ?assert(is_binary(arrow_format_nif:serialize_message(?SchemaMsg))),

    %% Flatbuffers doesn't need the body. So, don't provide it.
    RecordBatchMsg = (?RecordBatchMsg)#message{body = undefined},
    ?assert(is_binary(arrow_format_nif:serialize_message(RecordBatchMsg))).

%%%%%%%%%%%
%% Utils %%
%%%%%%%%%%%

schema(Type) ->
    serde_arrow_ipc_message:from_erlang(
        serde_arrow_ipc_schema:from_erlang([serde_arrow_ipc_field:from_erlang(Type)])
    ).
