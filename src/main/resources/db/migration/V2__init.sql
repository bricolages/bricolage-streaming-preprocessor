create table preproc_definition
( id bigserial primary key
, target_table text not null
, target_column text not null
, application_order integer not null default 0
, operator_id text not null
, params text not null   -- JSON format
, created_at timestamp not null default current_timestamp
, updated_at timestamp not null default current_timestamp
)
;

create index preproc_definition_target_table_idx on preproc_definition (target_table);

create table preproc_log
( id bigserial primary key
, src_data_file text
, dest_data_file text
, input_rows bigint
, output_rows bigint
, error_rows bigint
, status varchar(16)
, start_time timestamp with time zone default current_timestamp
, end_time timestamp with time zone
, message text
)
;
