%% @doc Provides types and functions to work with IPC types.
-module(serde_arrow_ipc_type).
-export_type([
    ipc_type/0,
    int/0,
    floating_point/0,
    fixed_size_binary/0,
    fixed_size_list/0,
    large_binary/0,
    large_list/0
]).

%% TODO: Add support for the commented types.
%%
%% Note that the types have been listed in the same order as the have been
%% defined in the Schema definitions.
-type ipc_type() ::
    %% null() |
    int()
    | floating_point()
    %% | binary()
    %% | utf8()
    %% | bool()
    %% | decimal()
    %% | data()
    %% | time()
    %% | timestamp()
    %% | interval()
    %% | list()
    %% | struct_()
    %% | union()
    | fixed_size_binary()
    | fixed_size_list()
    %% | map()
    %% | duration()
    | large_binary()
    %% | large_utf8()
    | large_list()
%% | run_end_encoded()
.
%% Represents and IPC Type. Of the from `{TypeName, Metadata}'.

-type int() :: {int, #{bit_width => pos_integer(), is_signed => boolean()}}.
%% Represents a Fixed-Size Primitive Layout Array of an integral type.

-type floating_point() :: {floating_point, #{precision => half | single | double}}.
%% Represents a Fixed-Size Primitive Layout Array of f16, f32 or f64.

-type fixed_size_binary() :: {fixed_size_binary, #{byte_width => pos_integer()}}.
%% Represents a Fixed-Size Binary Layout Array.

-type fixed_size_list() :: {fixed_size_list, #{list_size => pos_integer()}}.
%% Represents a Fixed-Size Binary Layout Array.

-type large_binary() :: {large_binary, undefined}.
%% Represents a Variable-Size Binary Layout Array with 64 bit offsets.

-type large_list() :: {large_list, undefined}.
%% Represents a Variable-Size List Layout Array with 64 bit offsets.
