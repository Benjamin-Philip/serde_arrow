-module(serde_arrow_type_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%%%%%%%%%%%%%%%%%
%% Util Macros %%
%%%%%%%%%%%%%%%%%

-define(NormTest(Type, Char, Size),
    ?assertEqual(serde_arrow_type:normalize(Type), {Char, Size})
).

-define(NormErr(Type),
    ?assertError(badarg, serde_arrow_type:normalize(Type))
).

%%%%%%%%%%%%%%%%
%% Test Cases %%
%%%%%%%%%%%%%%%%

all() ->
    [
        normalize_on_longhand,
        normalize_on_shorthand,
        bit_length,
        byte_length,
        serialize
    ].

normalize_on_longhand(_Config) ->
    %% Boolean
    ?NormTest(bool, bool, undefined),
    %% Signed Integers
    ?NormTest({s, 8}, s, 8),
    ?NormTest({s, 16}, s, 16),
    ?NormTest({s, 32}, s, 32),
    ?NormTest({s, 64}, s, 64),
    %% Unsigned Integers
    ?NormTest({u, 8}, u, 8),
    ?NormTest({u, 16}, u, 16),
    ?NormTest({u, 32}, u, 32),
    ?NormTest({u, 64}, u, 64),
    %% Floats
    ?NormTest({f, 16}, f, 16),
    ?NormTest({f, 32}, f, 32),
    ?NormTest({f, 64}, f, 64),
    %% Binaries
    ?NormTest(bin, bin, undefined),
    %% Error on invalid types
    ?NormErr({foo_is_bar, 4}),
    ?NormErr({bar_is_foo, undefined}),
    ?NormErr({bool, 4}),
    ?NormErr({s, 128}),
    ?NormErr({u, 128}),
    ?NormErr({f, 128}),
    ?NormErr({bin, 4}).

normalize_on_shorthand(_Config) ->
    %% Booleans
    ?NormTest(bool, bool, undefined),
    ?NormTest(s16, s, 16),
    ?NormTest(s32, s, 32),
    ?NormTest(s64, s, 64),
    %% Unsigned Integers
    ?NormTest(u8, u, 8),
    ?NormTest(u16, u, 16),
    ?NormTest(u32, u, 32),
    ?NormTest(u64, u, 64),
    %% Floats
    ?NormTest(f16, f, 16),
    ?NormTest(f32, f, 32),
    ?NormTest(f64, f, 64),
    %% Binaries
    ?NormTest(bin, bin, undefined),
    %% Signed Integers
    ?NormTest(s8, s, 8),
    ?NormTest(s16, s, 16),
    ?NormTest(s32, s, 32),
    ?NormTest(s64, s, 64),
    %% Unsigned Integers
    ?NormTest(u8, u, 8),
    ?NormTest(u16, u, 16),
    ?NormTest(u32, u, 32),
    ?NormTest(u64, u, 64),
    %% Floats
    ?NormTest(f16, f, 16),
    ?NormTest(f32, f, 32),
    ?NormTest(f64, f, 64),
    %% Binaries
    ?NormTest(bin, bin, undefined),
    %% Error on invalid types
    ?NormErr(and_bar_is_foo),
    ?NormErr(u128),
    ?NormErr(s128),
    ?NormErr(f8),
    %% Nested Types
    ?assertEqual(serde_arrow_type:normalize({fixed_list, {s, 8}, 4}), {fixed_list, {s, 8}, 4}),
    ?assertEqual(serde_arrow_type:normalize({fixed_list, s8, 4}), {fixed_list, {s, 8}, 4}),
    ?assertEqual(
        serde_arrow_type:normalize({fixed_list, {fixed_list, {s, 8}, 4}, 4}),
        {fixed_list, {fixed_list, {s, 8}, 4}, 4}
    ),
    ?assertEqual(
        serde_arrow_type:normalize({fixed_list, {fixed_list, s8, 4}, 4}),
        {fixed_list, {fixed_list, {s, 8}, 4}, 4}
    ),
    ?assertError(badarg, serde_arrow_type:normalize({fixed_list, s8, undefined})),
    ?assertError(badarg, serde_arrow_type:normalize({fixed_list, {fixed_list, s8, undefined}, 4})).

bit_length(_Config) ->
    %% Normalizes
    ?assertEqual(serde_arrow_type:bit_length(s8), 8),

    ?assertEqual(serde_arrow_type:bit_length({s, 8}), 8),
    ?assertEqual(serde_arrow_type:bit_length({u, 8}), 8),
    ?assertEqual(serde_arrow_type:bit_length({f, 32}), 32),

    ?assertEqual(serde_arrow_type:bit_length(bool), 1),
    ?assertEqual(serde_arrow_type:bit_length({bool, undefined}), 1),

    ?assertError(badarg, serde_arrow_type:bit_length(bin)),
    ?assertError(badarg, serde_arrow_type:bit_length({bin, undefined})),

    ?assertError(badarg, serde_arrow_type:bit_length({fixed_list, s8, 4})).

byte_length(_Config) ->
    %% Normalizes
    ?assertEqual(serde_arrow_type:byte_length(s8), 1),

    ?assertEqual(serde_arrow_type:byte_length({s, 8}), 1),
    ?assertEqual(serde_arrow_type:byte_length({u, 8}), 1),
    ?assertEqual(serde_arrow_type:byte_length({f, 32}), 4),

    %% This case is stub for the current function's stub implementation
    ?assertEqual(serde_arrow_type:byte_length(bool), 1),
    ?assertEqual(serde_arrow_type:byte_length({bool, undefined}), 1),

    ?assertEqual(serde_arrow_type:byte_length({bin, undefined}), undefined),
    ?assertEqual(serde_arrow_type:byte_length({bin, undefined}), undefined),

    ?assertError(badarg, serde_arrow_type:byte_length({fixed_list, s8, 4})).

serialize(_Config) ->
    ?assertEqual(serde_arrow_type:serialize(1, {s, 64}), <<1:64/little-signed-integer>>),
    ?assertEqual(serde_arrow_type:serialize(1, {u, 64}), <<1:64/little-unsigned-integer>>),
    ?assertEqual(serde_arrow_type:serialize(1, {f, 64}), <<1.0:64/little-float>>).
