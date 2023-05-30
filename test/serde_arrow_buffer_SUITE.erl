-module(serde_arrow_buffer_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("serde_arrow_buffer.hrl").

all() ->
    [
        valid_length_on_from_erlang,
        valid_type_on_from_erlang,
        valid_data_on_from_erlang,
        crashes_on_list_of_binaries_on_from_erlang,

        valid_regular_buffer_data_on_to_arrow,
        valid_binary_buffer_data_on_to_arrow,
        crashes_on_non_buffer_input_on_to_arrow,

        to_erlang
    ].

%%%%%%%%%%%%%%%%%%%
%% from_erlang/2 %%
%%%%%%%%%%%%%%%%%%%

valid_length_on_from_erlang(_Config) ->
    %% With Fixed-Size data
    Buffer1 = serde_arrow_buffer:from_erlang([1, 2, undefined, 3, nil], {s, 8}),
    ?assertEqual(Buffer1#buffer.length, 5),

    Buffer2 = serde_arrow_buffer:from_erlang([1, 2, undefined, 3, nil], {s, 8}, 5),
    ?assertEqual(Buffer2#buffer.length, 5),

    %% With binaries
    Buffer3 = serde_arrow_buffer:from_erlang(<<1, 2, 3>>, {bin, undefined}),
    ?assertEqual(Buffer3#buffer.length, 3),

    Buffer4 = serde_arrow_buffer:from_erlang(<<1, 2, 3>>, {bin, undefined}, 3),
    ?assertEqual(Buffer4#buffer.length, 3).

valid_type_on_from_erlang(_Config) ->
    Buffer1 = serde_arrow_buffer:from_erlang([1, 2, 3], {s, 32}),
    ?assertEqual(Buffer1#buffer.type, {s, 32}),

    Buffer2 = serde_arrow_buffer:from_erlang(<<1, 2, 3>>, {bin, undefined}),
    ?assertEqual(Buffer2#buffer.type, {bin, undefined}).

valid_data_on_from_erlang(_Config) ->
    Buffer1 = serde_arrow_buffer:from_erlang([1, 2, 3], {s, 32}),
    ?assertEqual(Buffer1#buffer.data, [1, 2, 3]),

    Buffer2 = serde_arrow_buffer:from_erlang(<<1, 2, 3>>, {bin, undefined}),
    ?assertEqual(Buffer2#buffer.data, <<1, 2, 3>>).

crashes_on_list_of_binaries_on_from_erlang(_Config) ->
    ?assertError(
        badarg, serde_arrow_buffer:from_erlang([<<1, 2>>, undefined, <<3>>, nil], {bin, undefined})
    ).

%%%%%%%%%%%%%%%%
%% to_arrow/1 %%
%%%%%%%%%%%%%%%%

valid_regular_buffer_data_on_to_arrow(_Config) ->
    %% Works without any nulls
    Bin1 = serde_arrow_buffer:to_arrow(serde_arrow_buffer:from_erlang([1, 2, 3], {s, 8})),
    Data1 =
        <<1/little-signed-integer, 2/little-signed-integer, 3/little-signed-integer,
            (alternate_pad(61))/bitstring>>,
    ?assertEqual(Bin1, Data1),

    %% Works with undefined and nil
    Bin2 = serde_arrow_buffer:to_arrow(
        serde_arrow_buffer:from_erlang([1, 2, undefined, 3], {s, 8})
    ),
    Data2 =
        <<1/little-signed-integer, 2/little-signed-integer, 0, 3/little-signed-integer,
            (alternate_pad(60))/bitstring>>,
    ?assertEqual(Bin2, Data2),

    Bin3 = serde_arrow_buffer:to_arrow(serde_arrow_buffer:from_erlang([1, 2, nil, 3], {s, 8})),
    Data3 = Data2,
    ?assertEqual(Bin3, Data3),

    Bin4 = serde_arrow_buffer:to_arrow(
        serde_arrow_buffer:from_erlang([1, 2, undefined, nil, 3], {s, 8})
    ),
    Data4 =
        <<1/little-signed-integer, 2/little-signed-integer, 0, 0, 3/little-signed-integer,
            (alternate_pad(59))/bitstring>>,
    ?assertEqual(Bin4, Data4).

valid_binary_buffer_data_on_to_arrow(_Config) ->
    Data =
        <<1/little-signed-integer, 2/little-signed-integer, 3/little-signed-integer,
            (alternate_pad(61))/bitstring>>,

    %% Works without any nulls
    Bin1 = serde_arrow_buffer:to_arrow(
        serde_arrow_buffer:from_erlang(<<1, 2, 3>>, {bin, undefined})
    ),
    ?assertEqual(Bin1, Data),

    %% Works with undefined and nil
    Bin2 = serde_arrow_buffer:to_arrow(
        serde_arrow_buffer:from_erlang(<<1, 2, 3>>, {bin, undefined})
    ),
    ?assertEqual(Bin2, Data),

    Bin3 = serde_arrow_buffer:to_arrow(
        serde_arrow_buffer:from_erlang(<<1, 2, 3>>, {bin, undefined})
    ),
    ?assertEqual(Bin3, Data),

    Bin4 = serde_arrow_buffer:to_arrow(
        serde_arrow_buffer:from_erlang(<<1, 2, 3>>, {bin, undefined})
    ),
    ?assertEqual(Bin4, Data).

crashes_on_non_buffer_input_on_to_arrow(_Config) ->
    ?assertError(badarg, serde_arrow_buffer:to_arrow(bar)).

%%%%%%%%%%%%%%%%%
%% to_erlang/1 %%
%%%%%%%%%%%%%%%%%

to_erlang(_Config) ->
    Data = [1, 2, undefined, 3, nil],
    ?assertEqual(serde_arrow_buffer:to_erlang(serde_arrow_buffer:from_erlang(Data, {s, 8})), Data),

    ?assertError(badarg, serde_arrow_buffer:to_erlang([1, 2, 3])).

%%%%%%%%%%%
%% Utils %%
%%%%%%%%%%%

%% We need to use a simpler alternate pad function to test the buffer's pad
%% output.
alternate_pad(ByteLen) ->
    <<<<0>> || _X <- lists:seq(1, ByteLen)>>.
