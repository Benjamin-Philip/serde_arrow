%% @doc Validity Bitmap implementation for `serde_arrow'.
%%
%% Defines a function `validity_bitmap/1' to return the Validity Bitmap[1] along
%% with the Null Count[2], of an Array.
%%
%% An important thing to consider about our implementation of the Null Count is
%% that we need to support both `undefined' and `nil' as null values as they are
%% the conventions for null values in Erlang, and Elixir respectively.
%%
%% There are 5 important characteristics to remember about the validity bitmap:
%%
%% <ol>
%%  <li>
%%      A null value is represented by a 0 bit, and a non null value by a 1 bit.
%%  </li>
%%  <li>
%%      Every 8 elements's validities are batched into a byte, which are then
%%      reversed as Arrow uses least-significant bit (LSB) numbering (more in
%%      attached reference).
%%  </li>
%%  <li>
%%      If a "batch" consists of less than 8 elements, its validity needs to be
%%      padded by 0 bits so that it can make a byte.
%%  </li>
%%  <li>
%%      Each byte is stored in a slot of a Buffer (see docs for
%%      `serde_arrow_buffer'). This buffer with the validities of each batch of 8
%%      elements make up what is called the Validity Bitmap.
%%  </li>
%%  <li>
%%      If the Null Count is 0, we can allocate the Validity Bitmap as a NULL
%%      pointer (which in Erlang's case is `undefined').
%%  </li>
%% </ol>
%% [1]: [https://arrow.apache.org/docs/format/Columnar.html#validity-bitmaps]
%%
%% [2]: [https://arrow.apache.org/docs/format/Columnar.html#null-count]
%% @end
-module(serde_arrow_bitmap).
-export([validity_bitmap/1]).

-include("serde_arrow_buffer.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Validity Bitmap & Null Count %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Returns the Validity Bitmap along with the Null Count, of
%% an Array.
-spec validity_bitmap(Value :: [serde_arrow_type:native_type()]) ->
    {Bitmap :: #buffer{}, non_neg_integer()}.
validity_bitmap(Value) ->
    case (lists:member(undefined, Value)) orelse (lists:member(nil, Value)) of
        true ->
            bitmap(Value, <<>>, 0);
        false ->
            {undefined, 0}
    end.

-spec bitmap(
    Value :: [serde_arrow_type:native_type()],
    Acc :: binary(),
    NullCount :: non_neg_integer()
) -> {Bitmap :: #buffer{}, NullCount :: non_neg_integer()}.
bitmap([X1, X2, X3, X4, X5, X6, X7, X8 | Rest], Acc, NullCount) ->
    %% By assigning B8 as X1's validity, we are following LSB numbering.
    B8 = validity(X1),
    B7 = validity(X2),
    B6 = validity(X3),
    B5 = validity(X4),
    B4 = validity(X5),
    B3 = validity(X6),
    B2 = validity(X7),
    B1 = validity(X8),

    bitmap(
        Rest,
        <<Acc/binary, B1:1, B2:1, B3:1, B4:1, B5:1, B6:1, B7:1, B8:1>>,
        NullCount + (8 - (B1 + B2 + B3 + B4 + B5 + B6 + B7 + B8))
    );
bitmap([], Acc, NullCount) ->
    {serde_arrow_buffer:from_erlang(Acc, {bin, undefined}), NullCount};
bitmap(LeftOver, Acc, NullCount) ->
    Validities = lists:map(fun(X) -> validity(X) end, LeftOver),
    Len = length(Validities),
    Nulls = Len - lists:sum(Validities),

    %% Here we both pad as well as LSB number at one pass.
    PadLen = 8 - Len rem 8,
    Padded = lists:duplicate(PadLen, 0) ++ lists:reverse(Validities),

    [B1, B2, B3, B4, B5, B6, B7, B8] = Padded,
    bitmap(
        [],
        <<Acc/binary, B1:1, B2:1, B3:1, B4:1, B5:1, B6:1, B7:1, B8:1>>,
        NullCount + Nulls
    ).

-spec validity(X :: serde_arrow_type:native_type()) -> Validity :: 0 | 1.
validity(X) ->
    if
        (X =:= nil) orelse (X =:= undefined) ->
            0;
        true ->
            1
    end.
