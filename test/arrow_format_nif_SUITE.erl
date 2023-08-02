-module(arrow_format_nif_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("serde_arrow_ipc_message.hrl").

-include("serde_arrow_ipc_marks_data.hrl").

all() ->
    [test_decode, test_encode].

%%%%%%%%%%%%%%%%%
%% test_decode %%
%%%%%%%%%%%%%%%%%

test_decode(_Config) ->
    %% Type testing

    %% Signed Integers
    ?assertEqual(
        arrow_format_nif:test_decode(schema({int, #{bit_width => 8, is_signed => true}})), ok
    ),
    ?assertEqual(
        arrow_format_nif:test_decode(schema({int, #{bit_width => 16, is_signed => true}})), ok
    ),
    ?assertEqual(
        arrow_format_nif:test_decode(schema({int, #{bit_width => 32, is_signed => true}})), ok
    ),
    ?assertEqual(
        arrow_format_nif:test_decode(schema({int, #{bit_width => 64, is_signed => true}})), ok
    ),

    %% Unsigned Integers
    ?assertEqual(
        arrow_format_nif:test_decode(schema({int, #{bit_width => 8, is_signed => false}})), ok
    ),
    ?assertEqual(
        arrow_format_nif:test_decode(schema({int, #{bit_width => 16, is_signed => false}})), ok
    ),
    ?assertEqual(
        arrow_format_nif:test_decode(schema({int, #{bit_width => 32, is_signed => false}})), ok
    ),
    ?assertEqual(
        arrow_format_nif:test_decode(schema({int, #{bit_width => 64, is_signed => false}})), ok
    ),

    %% Floating Point
    ?assertEqual(arrow_format_nif:test_decode(schema({floating_point, #{precision => half}})), ok),
    ?assertEqual(
        arrow_format_nif:test_decode(schema({floating_point, #{precision => single}})), ok
    ),
    ?assertEqual(
        arrow_format_nif:test_decode(schema({floating_point, #{precision => double}})), ok
    ),

    %% Fixed-Size List
    ?assertEqual(arrow_format_nif:test_decode(schema({fixed_size_list, #{list_size => 3}})), ok),

    %% Large Binary
    ?assertEqual(arrow_format_nif:test_decode(schema(large_binary)), ok),

    %% Large List
    ?assertEqual(arrow_format_nif:test_decode(schema(large_list)), ok),

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

%%%%%%%%%%%
%% Utils %%
%%%%%%%%%%%

schema(Type) ->
    serde_arrow_ipc_message:from_erlang(
        serde_arrow_ipc_schema:from_erlang([serde_arrow_ipc_field:from_erlang(Type)])
    ).
