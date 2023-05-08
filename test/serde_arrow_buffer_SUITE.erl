-module(serde_arrow_buffer_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("serde_arrow_buffer.hrl").

all() ->
    [
        %% new/3 and new/2
        valid_length_on_new,
        valid_type_on_new,
        valid_regular_buffer_data_on_new,

        %% from_binary/4, from_binary/3, from_binary/2 and from_binary/1
        valid_length_on_from_binary,
        %% valid_element_length_on_from_binary,
        valid_type_on_from_binary,
        valid_buffer_data_on_from_binary
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Buffer Creation Tests %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%

valid_length_on_new(_Config) ->
    %% With Fixed-Size data
    Buffer1 = serde_arrow_buffer:new([1, 2, 3], {s, 8}),
    ?assertEqual(Buffer1#buffer.length, 3),

    %% With binaries
    Buffer2 = serde_arrow_buffer:new([<<1, 2>>, <<3>>], {bin, undefined}),
    ?assertEqual(Buffer2#buffer.length, 3).

valid_type_on_new(_Config) ->
    Buffer1 = serde_arrow_buffer:new([1, 2, 3], {s, 32}),
    ?assertEqual(Buffer1#buffer.type, {s, 32}),

    Buffer2 = serde_arrow_buffer:new([<<1, 2>>, <<3>>], {bin, undefined}),
    ?assertEqual(Buffer2#buffer.type, {bin, undefined}).

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

valid_binary_buffer_data_on_new(_Config) ->
    %% Works without any nulls
    Buffer1 = serde_arrow_buffer:new([<<1>>, <<2>>, <<3>>], {bin, undefined}),
    Data1 = <<1, 2, 3, (alternate_pad(61))/bitstring>>,
    ?assertEqual(Buffer1#buffer.data, Data1),

    %% Works with undefined and nil
    Buffer2 = serde_arrow_buffer:new([1, 2, undefined, 3], {s, 8}),
    Data2 =
        <<1, 2, 0, 3, (alternate_pad(60))/bitstring>>,
    ?assertEqual(Buffer2#buffer.data, Data2),

    Buffer3 = serde_arrow_buffer:new([1, 2, nil, 3], {s, 8}),
    Data3 = Data2,
    ?assertEqual(Buffer3#buffer.data, Data3),

    Buffer4 = serde_arrow_buffer:new([1, 2, undefined, nil, 3], {s, 8}),
    Data4 =
        <<1/little-signed-integer, 2/little-signed-integer, 0, 0, 3/little-signed-integer,
            (alternate_pad(59))/bitstring>>,
    ?assertEqual(Buffer4#buffer.data, Data4).

%%%%%%%%%%%%%%%%%
%% from_binary %%
%%%%%%%%%%%%%%%%%

valid_length_on_from_binary(_Config) ->
    Buffer = serde_arrow_buffer:from_binary(<<1, 2, 3>>, {bin, undefined}, 3),
    ?assertEqual(Buffer#buffer.length, 3).

valid_type_on_from_binary(_Config) ->
    Buffer1 = serde_arrow_buffer:from_binary(<<1, 2, 3>>, {bin, undefined}, 3),
    ?assertEqual(Buffer1#buffer.type, {bin, undefined}),

    Buffer2 = serde_arrow_buffer:from_binary(<<1, 2, 3>>, {s, 8}, 3),
    ?assertEqual(Buffer2#buffer.type, {s, 8}).

valid_buffer_data_on_from_binary(_Config) ->
    Buffer = serde_arrow_buffer:from_binary(<<1, 2, 3>>, {bin, undefined}, 3),
    Data = <<1, 2, 3, (alternate_pad(61))/bitstring>>,
    ?assertEqual(Buffer#buffer.data, Data).

%%%%%%%%%%%
%% Utils %%
%%%%%%%%%%%

%% We need to use a simpler alternate pad function to test the buffer's pad
%% output.
alternate_pad(ByteLen) ->
    <<<<0>> || _X <- lists:seq(1, ByteLen)>>.