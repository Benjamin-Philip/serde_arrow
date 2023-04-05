-module(serde_arrow_buffer_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("serde_arrow_buffer.hrl").

all() ->
    [
        valid_length_on_new,
        valid_element_length_on_new,
        valid_type_on_new,
        valid_byte_buffer_data_on_new,
        valid_regular_buffer_data_on_new
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Buffer Creation Tests %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%

valid_length_on_new(_Config) ->
    %% new/3
    Buffer1 = serde_arrow_buffer:new([<<1>>, <<2>>, <<3>>], byte, 3),
    ?assertEqual(Buffer1#buffer.length, 3),

    %% new/2
    Buffer2 = serde_arrow_buffer:new([<<1>>, <<2>>, <<3>>], byte),
    ?assertEqual(Buffer2#buffer.length, 3),

    Buffer3 = serde_arrow_buffer:new([1, 2, 3], {s, 8}),
    ?assertEqual(Buffer3#buffer.length, 3).

valid_element_length_on_new(_Config) ->
    %% Works with raw bytes
    Buffer1 = serde_arrow_buffer:new([<<1>>, <<2>>, <<3>>], byte),
    ?assertEqual(Buffer1#buffer.element_length, 1),

    %% Works with any other dtype
    Buffer2 = serde_arrow_buffer:new([1, 2, 3], {s, 32}),
    ?assertEqual(Buffer2#buffer.element_length, 4).

valid_type_on_new(_Config) ->
    Buffer1 = serde_arrow_buffer:new([<<1>>, <<2>>, <<3>>], byte),
    ?assertEqual(Buffer1#buffer.type, byte),

    Buffer2 = serde_arrow_buffer:new([1, 2, 3], {s, 32}),
    ?assertEqual(Buffer2#buffer.type, {s, 32}).

valid_byte_buffer_data_on_new(_Config) ->
    Buffer = serde_arrow_buffer:new([<<1>>, <<2>>, <<3>>], byte),
    Data = <<1, 2, 3, (alternate_pad(61))/bitstring>>,
    ?assertEqual(Buffer#buffer.data, Data).

valid_regular_buffer_data_on_new(_Config) ->
    %% Works without any nulls
    Buffer1 = serde_arrow_buffer:new([1, 2, 3], {s, 8}),
    Data1 =
        <<1/little-signed-integer, 2/little-signed-integer, 3/little-signed-integer,
            (alternate_pad(61))/bitstring>>,
    ?assertEqual(Buffer1#buffer.data, Data1),

    %% Works with undefined and nil
    Buffer2 = serde_arrow_buffer:new([1, 2, undefined, 3], {s, 8}),
    Data2 =
        <<1/little-signed-integer, 2/little-signed-integer, 0, 3/little-signed-integer,
            (alternate_pad(60))/bitstring>>,
    ?assertEqual(Buffer2#buffer.data, Data2),

    Buffer3 = serde_arrow_buffer:new([1, 2, nil, 3], {s, 8}),
    Data3 = Data2,
    ?assertEqual(Buffer3#buffer.data, Data3),

    Buffer4 = serde_arrow_buffer:new([1, 2, undefined, nil, 3], {s, 8}),
    Data4 =
        <<1/little-signed-integer, 2/little-signed-integer, 0, 0, 3/little-signed-integer,
            (alternate_pad(59))/bitstring>>,
    ?assertEqual(Buffer4#buffer.data, Data4).

%%%%%%%%%%%
%% Utils %%
%%%%%%%%%%%

alternate_pad(ByteLen) ->
    <<<<0>> || _X <- lists:seq(1, ByteLen)>>.
