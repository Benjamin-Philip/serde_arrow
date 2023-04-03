%% @private
%% Utilitary functions for Array Implementations.
-module(serde_arrow_array_utils).
-export([validity/2, buffer/2]).

validity(Value, Len) ->
    ValidityList = lists:map(
        fun(X) ->
            case X of
                nil ->
                    0;
                _ ->
                    1
            end
        end,
        Value
    ),
    NullCount = Len - lists:sum(ValidityList),
    case NullCount of
        0 ->
            {0, nil};
        _ ->
            {NullCount, bitmap(ValidityList, Len)}
    end.

bitmap(ValidityList, Len) ->
    %% We need to split the list every 8th element and then pad the very last
    %% element such that every resulting list has a length of 8 elements.
    %%
    %% If we try to split a list with only 8 elements, we will get a copy of the
    %% list and an empty list in the resulting tuple. In order to get over this
    %% empty list, we pad one bit extra and drop the final list.

    PadLen = 9 - Len rem 8,
    Padding = lists:duplicate(PadLen, 0),
    SplitValidityList = lists:droplast(
        tuple_to_list(lists:split(8, lists:append(ValidityList, Padding)))
    ),

    %% So far each bit has been represented by an Erlang integer.
    %% Here we do two things:
    %%
    %% 1. We reverse the order of bits in each byte in order to repsect Arrow's
    %%    bit-endianness: https://en.wikipedia.org/wiki/Bit_numbering
    %%
    %% 2. We convert the integer bits to actual bits.
    %%
    %% We are doing 2 distinct operations over a single list traversal as it may
    %% have a noticeable performance difference for large validity buffer.
    %% We also don't generate the initial ValidityList as a binary because we
    %% need its sum for the null count.

    Bytes = lists:map(fun(Byte) -> <<<<X:1>> || X <- lists:reverse(Byte)>> end, SplitValidityList),
    %% buffer(Bytes, byte)
    Bytes.

%% TODO: Write a buffer implementation.

buffer(_Value, _Type) ->
    "Foobar!".
