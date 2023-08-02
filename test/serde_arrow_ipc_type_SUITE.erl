-module(serde_arrow_ipc_type_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("serde_arrow_ipc_marks_data.hrl").

all() ->
    [
        int_from_erlang,
        floating_point_from_erlang,
        fixed_size_list_from_erlang,
        large_binary_from_erlang,
        large_list_from_erlang
    ].

int_from_erlang(_Config) ->
    %% Signed Integers
    ?assertEqual(fixed_primitive(s8), {int, #{bit_width => 8, is_signed => true}}),
    ?assertEqual(fixed_primitive(s16), {int, #{bit_width => 16, is_signed => true}}),
    ?assertEqual(fixed_primitive(s32), {int, #{bit_width => 32, is_signed => true}}),
    ?assertEqual(fixed_primitive(s64), {int, #{bit_width => 64, is_signed => true}}),

    %% Unsigned Integers
    ?assertEqual(fixed_primitive(u8), {int, #{bit_width => 8, is_signed => false}}),
    ?assertEqual(fixed_primitive(u16), {int, #{bit_width => 16, is_signed => false}}),
    ?assertEqual(fixed_primitive(u32), {int, #{bit_width => 32, is_signed => false}}),
    ?assertEqual(fixed_primitive(u64), {int, #{bit_width => 64, is_signed => false}}).

floating_point_from_erlang(_Config) ->
    ?assertEqual(fixed_primitive(f16), {floating_point, #{precision => half}}),
    ?assertEqual(fixed_primitive(f32), {floating_point, #{precision => single}}),
    ?assertEqual(fixed_primitive(f64), {floating_point, #{precision => double}}).

fixed_size_list_from_erlang(_Config) ->
    Type = from_erlang(serde_arrow_fixed_list_array:from_erlang([[1, 2, 3]], {s, 8})),
    ?assertEqual(Type, {fixed_size_list, #{list_size => 3}}).

large_binary_from_erlang(_Config) ->
    Type = from_erlang(
        serde_arrow_variable_binary_array:from_erlang([<<1>>, <<1, 2>>, <<1, 2, 3>>], {s, 8})
    ),
    ?assertEqual(Type, large_binary).

large_list_from_erlang(_Config) ->
    Type = from_erlang(
        serde_arrow_variable_list_array:from_erlang([[1], [1, 2], [1, 2, 3]], {s, 8})
    ),
    ?assertEqual(Type, large_list).

%%%%%%%%%%%
%% Utils %%
%%%%%%%%%%%

fixed_primitive(Type) ->
    from_erlang(serde_arrow_fixed_primitive_array:from_erlang([1, 2, 3], Type)).

from_erlang(Array) ->
    serde_arrow_ipc_type:from_erlang(Array).
