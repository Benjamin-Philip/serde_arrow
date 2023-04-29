%% @doc Provides support for Arrow's Fixed-Size Primitive Layout.
%%
%% The Primitive Layout[1] uses the following metadata in the `serde_arrow_array':
%%
%% <ol>
%%  <li>
%%      `layout', of type {@link atom()}, and a constant value of `fixed_primitive'.
%%  </li>
%%  <li>
%%       `type', of type {@link serde_arrow_type:arrow_type()}, which represents
%%       the Logical Type of the Array.
%% </li>
%%  <li>`len', of type {@link pos_integer()}, which represents the Array's Length.</li>
%%  <li>
%%      `null_count', of type {@link non_neg_integer()}, which represents the Array's
%%      Null Count, or the number of undefined values in the Array.
%%  </li>
%%  <li>
%%      `validity_bitmap', which is a buffer (`serde_arrow_buffer') or the atom
%%       `undefined', which represents the Array's Validity Bitmap[2].
%%  </li>
%%  <li>
%%      `data', which is a buffer (`serde_arrow_buffer'), which represents the Array's Value Buffer.
%%  </li>
%% </ol>
%%
%% The `offsets' field will be allocated as `undefined'. Same goes for the
%% `validity_bitmap' field in the case of a 0 `null_count'.
%%
%% [1]: [https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout]
%%
%% [2]: [https://arrow.apache.org/docs/format/Columnar.html#validity-bitmaps]
%% @end
-module(serde_arrow_fixed_primitive_array).
-behaviour(serde_arrow_array).

-export([new/2]).

-include("serde_arrow_array.hrl").

%%%%%%%%%%%%%%%%%%%%
%% Array Creation %%
%%%%%%%%%%%%%%%%%%%%

%% @doc Creates a new primitive array, given its value and type.
%%
%% Accepts a proplist with the type, or the type directly.
%% @end
-spec new(
    Value :: [serde_arrow_type:erlang_type()],
    Type :: [proplist:property()] | serde_arrow_type:arrow_type()
) ->
    Array :: #array{}.
new(Value, Opts) when is_list(Opts) ->
    case proplists:get_value(type, Opts) of
        undefined ->
            erlang:error(badarg);
        Type when is_tuple(Type) orelse is_atom(Type) ->
            new(Value, Type)
    end;
new(Value, Type) when is_tuple(Type) orelse is_atom(Type) ->
    Len = length(Value),
    {Bitmap, NullCount} = serde_arrow_bitmap:validity_bitmap(Value),
    Bin = serde_arrow_buffer:new(Value, Type),
    #array{
        layout = fixed_primitive,
        type = Type,
        len = Len,
        null_count = NullCount,
        validity_bitmap = Bitmap,
        data = Bin
    }.
