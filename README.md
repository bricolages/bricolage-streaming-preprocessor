# Bricolage Streaming Preprocessor

Bricolage Streaming Preprocessor service processes JSON data stream (S3 to S3).

This software is written in working time in Cookpad, Inc.

## Pre-requisites

- OpenJDK 11

## Development (Local)

Building Executable JAR file and running all tests:
```
% ./script/build
```

To run the application, you must copy config/*.example files to config/* and edit them.
Execute:
```
% (cd db && bundle && ./ridgepole.sh --merge)   # Migrates database schema
% ./script/server
```

## Building Docker Image

Copy following config files and edit it (DB host, port, user, password).
All config files has the corresponding example file (*.example), just copy and edit it.

- config.docker/streaming-preprocessor.yml
- config.docker/application.yml (for Spring)
- config.docker/logback.xml (for LogBack)
- env.docker

Then build image:
```
% ./gradlew build
% docker-compose build
```

Run:
```
% docker-compose -d up db    # Starts database in the background
% docker-compose up mig      # Migrates database schema and exits
% docker-compose up app      # Runs main application
```

## Operators

bricolage-streaming-preprocessor task is defined by "operators".
Each operator transforms a JSON record according to the opeartor definition (parameters).
You can give operator definition via parameter table named `preproc_definition`.
Following table is an example image of `preproc_definition` table.

 id |  target_table   |  target_column  | operator_id |                      params
----|-----------------|-----------------|-------------|------------------------------------------------
  1 | activity.pv_log | user_id         | int         | {}
  2 | activity.pv_log | url             | text        | {"maxByteLength":1000,createOverflowFlag:true}

Details of each operator follow.

### `text`: Text Cleansing

Validates text values.

- `maxByteLength`: Declared max byte length.  Use with `dropIfOverflow` or `createOverflowFlag`.
- `dropIfOverflow`: Drops this column if the text is longer than `maxByteLength`.
- `createOverflowFlag`: Stores boolean overflow flag if the text is longer then `maxByteLength`.
- `pattern`: Drops this column if the text does not matches this pattern (Java regex).

### `int`, `bigint`: Integer Cleansing

Validates the value is an integer.
String values are automatically converted to integers e.g. `"82"` -> `82`.
This operator also drops non-integer value.

- No parameters.

### `unixtime`: Unix Time to Timestamp Conversion

Converts Unix epoch time given by integers (`1467954650`) to the timestamp string (`'2016-07-08T14:10:50+09:00'`).

- `zoneOffset`: Target timezone, given by the string like `'+09:00'`.

### `delete`: Unconditional column delete

Deletes specified column.

- No parameters.

### `rename`: Column rename

Renames specified column.

- `to`: the column name which is renamed to.

### `timezone`: Timezone Conversion

- `sourceOffset`: Expected source data timezone, given by the string like `'+00:00'`.
- `targetOffset`: Target timezone, given by the string like `'+09:00'`.

### `deletenulls`: Deletes All Null Columns

Deletes all null columns e.g. `{a: null, b: 1}` -> `{b: 1}`.
`target_column` must be `'*'`.

- No Parameters.

### `aggregate`: Aggregates Multiple Columns to One JSON String

Aggregates multiple target columns which have same prefix to one nested JSON column.
e.g. `{x:1,p_a:2,p_b:3}` -> `{x:1,p:{a:2,b:3}}`

- `targetColumns`: Target column names to aggreagate, given by the Java regex pattern (e.g. `"^p_"`).
  Note: matched string is removed from output column name.
- `aggregatedColumn`: Aggregated, output column name.

### `collectrest`: Aggregates Rest Columns to One JSON String

Aggregates rest columns except specified columns.

- `rejectColumns`: Kept columns

### `reject`: Reject Records Matched with Condition

Reject records matched with condition described in params.

#### `type`: `string`

- `value`: If this value(string) and `target_columns` value are equal, the record will be rejected.

#### `type`: `integer`

- `value`: If this value(integer) and `target_columns` value are equal, the record will be rejected.

#### `type`: `null`

If `target_columns` value is null, it will reject the record.

### `sequence`: Add Sequence Number to Target Column

### `dup`: Duplicate column

Duplicate the column specified in `from` param to `target_column`.

### `float`: Floating-Point Number Cleansing

Similar to `int` or `bigint`, but this operator accepts floating-point numbers.

### `metadata`: Put metadata into records

Puts metadata such as table name, schema name (infered from stream prefix) into data records.

- `component`: The component to put.  `tableName` or `schemaName`.
- `overwrite`: Overwrites the target value while it already exists.  Default is false.

## License

MIT license. See LICENSE file for details.

## Author

Minero Aoki


# Release Note

## version 1.15.1
- [fix] Configure Sentry stacktrace.app.packaages to supress warning.

## version 1.15.0
- [new] Migrates to new Sentry client: from raven-logback to sentry-logback.

## version 1.14.0
- [new] New column strload_columns.time_unit to support microseconds for unix times (default: seconds)

## version 1.13.1
- [fix] strload_tables record may not exist for the disabled streams or blackhole streams

## version 1.13.0
- [CHANGE] Moves columns strload_stream_bundles.dest_bucket, .dest_prefix to strload_tables (Only one dest prefix per stream).

## version 1.12.0
- Upgrade Java (8 -> 11)
- Upgrade Spring Boot (1.5 -> 2.1)
- Upgrade Spring (4.3 -> 5.1)

## version 1.11.0
cancelled

## version 1.10.12
- [fix] Register metadata op

## version 1.10.11
- [new] New operator "metadata".

## version 1.10.9
- [fix] sequence op: Do not generate overwrapped sequence. (rev.2)

## version 1.10.8
- [fix] fix null pointer exception for bad S3 events

## version 1.10.7
- [fix] Reduces lazy loading.

## version 1.10.6
- [fix] sequence op: Do not generate overwrapped sequence.

## version 1.10.5
- [fix] Reject invalid S3 objects whose object size equals 0.

## version 1.10.4
- [fix] Update chunks records only when it is really changed.

## version 1.10.3
- [fix] preflight: Allows partition_source property for timestamp domains.

## version 1.10.2
- [fix] Load PreprocMessage records first to reduce duplicated rows error.

## version 1.10.1
- [fix] Use Long object type instead of primitive long type because table_id may be null.

## version 1.10.0
- [CHANGE] Splits big preproc_log table into 4 small tables: strload_preproc_messages, strload_preproc_jobs, strload_packets, strload_chunks.

## version 1.9.0
- Records unknown, incoming columns to strload_columns as "unknown" column type.
  Preprocessor just ignores "unknown" columns.  In next version, preprocessor disables
  a stream which includes "unknown" columns.

## version 1.8.0
- Preflight now handles only preprocessing, it no longer generates .ct or .job files.

## version 1.7.4
- [new] `date` column processor accepts timestamp strings.

## version 1.7.3
- [new] `timezone` column processor accepts more timestamp formats: "2018-03-05T12:34:56+0700" (ISO instant variant), "2018-03-05 12:34:56+0700" (ruby date time variant)

## version 1.7.2
- [new] `timezone` column processor allows to keep source offset as-is

## version 1.7.1
- [new] Introduces stream columns and column-type-based cleansing.
  This function is enabled by strload_streams.column_initialized flag.
  This version requires DB schema change.

## version 1.6.7
- [fix] timezone op: support new timestamp format like "2018-02-07 12:34:56"

## version 1.6.6
- [fix] sequence op: Allocates the next sequence block when `sequence` op run out the first sequence block.

## version 1.6.5
- [fix] Catches more exceptions.

## version 1.6.4
- [fix] Catches and reports all exceptions thrown from event handlers.

## version 1.6.3
- [fix] Temporary file name was wrong.

## version 1.6.2
- refactoring only

## version 1.6.1
- [fix] Fixes critial configuration error.

## version 1.6.0
- [new] Adds "blackhole" routing.

## version 1.5.x
- Enhances preflight.
