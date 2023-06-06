-module(serde_arrow_array_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("serde_arrow_buffer.hrl").

all() ->
    [
        %% creation tests
        valid_array_on_from_erlang,

        %% data and metadata tests
        valid_layout_on_access,
        valid_type_on_access,
        valid_len_on_access,
        valid_element_len_on_access,
        valid_null_count_on_access,
        valid_validity_bitmap_on_access,
        valid_offsets_on_access,
        valid_data_on_access,

        %% serialization tests
        valid_binary_on_to_arrow
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Array Creation Tests %%
%%%%%%%%%%%%%%%%%%%%%%%%%%

valid_array_on_from_erlang(_Config) ->
    Given1 = serde_arrow_array:from_erlang(fixed_primitive, [1, 2, 3], #{type => {s, 8}}),
    Expected1 = serde_arrow_fixed_primitive_array:from_erlang([1, 2, 3], {s, 8}),
    ?assertEqual(Given1, Expected1),

    Given2 = serde_arrow_array:from_erlang(variable_binary, [<<1>>, <<2>>], #{}),
    Expected2 = serde_arrow_variable_binary_array:from_erlang([<<1>>, <<2>>]),
    ?assertEqual(Given2, Expected2).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Array Data and Metadata Access Tests %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

valid_layout_on_access(_Config) ->
    Array = serde_arrow_array:from_erlang(fixed_primitive, [1, 2, 3], {s, 8}),
    ?assertEqual(serde_arrow_array:layout(Array), fixed_primitive).

valid_type_on_access(_Config) ->
    Array = serde_arrow_array:from_erlang(fixed_primitive, [1, 2, 3], {s, 8}),
    ?assertEqual(serde_arrow_array:type(Array), {s, 8}).

valid_len_on_access(_Config) ->
    Array = serde_arrow_array:from_erlang(fixed_primitive, [1, 2, 3], {s, 8}),
    ?assertEqual(serde_arrow_array:len(Array), 3).

valid_element_len_on_access(_Config) ->
    Array = serde_arrow_array:from_erlang(fixed_primitive, [1, 2, 3], {s, 8}),
    ?assertEqual(serde_arrow_array:element_len(Array), undefined).

valid_null_count_on_access(_Config) ->
    Array = serde_arrow_array:from_erlang(fixed_primitive, [1, 2, 3], {s, 8}),
    ?assertEqual(serde_arrow_array:null_count(Array), 0).

valid_validity_bitmap_on_access(_Config) ->
    Array = serde_arrow_array:from_erlang(fixed_primitive, [1, 2, 3], {s, 8}),
    ?assertEqual(serde_arrow_array:validity_bitmap(Array), undefined).

valid_offsets_on_access(_Config) ->
    Array = serde_arrow_array:from_erlang(fixed_primitive, [1, 2, 3], {s, 8}),
    ?assertEqual(serde_arrow_array:offsets(Array), undefined).

valid_data_on_access(_Config) ->
    Array = serde_arrow_array:from_erlang(fixed_primitive, [1, 2, 3], {s, 8}),
    ?assertEqual(serde_arrow_array:data(Array), serde_arrow_buffer:from_erlang([1, 2, 3], {s, 8})).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Array Serialization Tests %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

valid_binary_on_to_arrow(_Config) ->
    %% Primitive Fixed-Size Array
    %% Also offsets is not allocated on no offsets

    Array1 = serde_arrow_array:from_erlang(fixed_primitive, [1, 2, undefined, 3], {s, 8}),
    Validity1 = pad(<<0:1, 0:1, 0:1, 0:1, 1:1, 0:1, 1:1, 1:1>>),
    Data1 = pad(
        <<1:8/integer-signed-little, 2:8/integer-signed-little, 0:8/integer-signed-little,
            3:8/integer-signed-little>>
    ),
    Bin1 = <<Validity1/binary, Data1/binary>>,
    ?assertEqual(serde_arrow_array:to_arrow(Array1), Bin1),

    %% Variable-Size Binary
    %% Also validity, offsets and data is correctly allocated
    Array2 = serde_arrow_array:from_erlang(
        variable_binary, [<<1>>, <<2, 3>>, undefined, <<4, 5, 6>>], {s, 8}
    ),
    Validity2 = Validity1,
    Offsets2 = pad(
        <<0:32/integer-signed-little, 1:32/integer-signed-little, 3:32/integer-signed-little,
            3:32/integer-signed-little, 6:32/integer-signed-little>>
    ),
    Data2 = pad(<<1, 2, 3, 4, 5, 6>>),
    Bin2 = <<Validity2/binary, Offsets2/binary, Data2/binary>>,
    ?assertEqual(serde_arrow_array:to_arrow(Array2), Bin2),

    %% Fixed-Sized List
    %% Also validity not allocated on no nulls and recursive data is allocated.
    Array3 = serde_arrow_array:from_erlang(
        fixed_list, [[1, 2, undefined, 3], [4, 5, 6, 7]], {s, 8}
    ),
    Bin3 = serde_arrow_array:to_arrow(serde_arrow_array:data(Array3)),
    ?assertEqual(serde_arrow_array:to_arrow(Array3), Bin3),

    %% Variable-Size List
    %% Also validity, offsets, and recursive data is correctly allocated
    Array4 = serde_arrow_array:from_erlang(
        variable_list, [[1, 2, undefined], [3, 4], undefined, [5]], {s, 8}
    ),
    Validity4 = Validity1,
    Offsets4 = pad(
        <<0:32/integer-signed-little, 3:32/integer-signed-little, 5:32/integer-signed-little,
            5:32/integer-signed-little, 6:32/integer-signed-little>>
    ),
    Data4 = serde_arrow_array:to_arrow(serde_arrow_array:data(Array4)),
    Bin4 = <<Validity4/binary, Offsets4/binary, Data4/binary>>,
    ?assertEqual(serde_arrow_array:to_arrow(Array4), Bin4).

%%%%%%%%%%%
%% Utils %%
%%%%%%%%%%%

pad(X) ->
    PadLen = serde_arrow_utils:pad_len(byte_size(X)),
    <<X/binary, (serde_arrow_test_utils:pad(PadLen))/binary>>.
