%% This header file contains data for testing various modules for IPC.
%% It includes things from Schema definitions to IPC streams.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Schema Encapsulated Message %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(IDField, serde_arrow_ipc_field:from_erlang({s, 8}, "id")).
-define(NameField, serde_arrow_ipc_field:from_erlang({bin, undefined}, "name")).
-define(AgeField, serde_arrow_ipc_field:from_erlang({u, 8}, "age")).
-define(AnnualMarksField,
    serde_arrow_ipc_field:from_erlang({fixed_list, {u, 8}, 3}, "annual_marks")
).
-define(Fields, [?IDField, ?NameField, ?AgeField, ?AnnualMarksField]).
-define(Schema, serde_arrow_ipc_schema:from_erlang(?Fields)).
-define(SchemaMsg, serde_arrow_ipc_message:from_erlang(?Schema)).
-define(SchemaEMF, serde_arrow_ipc_message:to_ipc(?SchemaMsg)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% RecordBatch Encapsulated Message %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(ID, serde_arrow_array:from_erlang(fixed_primitive, [0, 1, 2, undefined], {s, 8})).
-define(Name,
    serde_arrow_array:from_erlang(
        variable_binary, [<<"alice">>, <<"bob">>, <<"charlie">>, undefined], {bin, undefined}
    )
).
-define(Age, serde_arrow_array:from_erlang(fixed_primitive, [10, 20, 30, undefined], {s, 8})).
-define(AnnualMarks,
    serde_arrow_array:from_erlang(
        fixed_list, [[100, 97, 98], [100, 99, 96], [100, 98, 95], undefined], {s, 8}
    )
).
-define(Columns, [?ID, ?Name, ?Age, ?AnnualMarks]).
-define(RecordBatch, serde_arrow_ipc_record_batch:from_erlang(?Columns)).
-define(Body, <<<<(serde_arrow_array:to_arrow(Array))/binary>> || Array <- ?Columns>>).
-define(RecordBatchMsg, serde_arrow_ipc_message:from_erlang(?RecordBatch, ?Body)).
-define(RecordBatchEMF, serde_arrow_ipc_message:to_ipc(?RecordBatchMsg)).
