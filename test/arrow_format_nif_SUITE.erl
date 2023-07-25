-module(arrow_format_nif_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("serde_arrow_ipc_message.hrl").

-include("serde_arrow_ipc_marks_data.hrl").

all() ->
    [print].

%%%%%%%%%%%
%% print %%
%%%%%%%%%%%

print(_Config) ->
    ?assertEqual(arrow_format_nif:print(?SchemaMsg), ok),

    %% Flatbuffers doesn't need the body. So, don't provide it.
    RecordBatchMsg = (?RecordBatchMsg)#message{body = undefined},
    ?assertEqual(arrow_format_nif:print(RecordBatchMsg), ok).
