# Release Note

## version 1.17.3
- [fix] Upgrades Log4J to avoid zero day attack

## version 1.17.2
- [new] Uses Eclipse Temurin for a Docker base image. (no code had changed)

## version 1.17.1
- [fix] Reduces log level for bad stream name (warn -> info).

## version 1.17.0
- [new] Ignores incoming streams that have invalid name.
- [new] Ignores incoming columns that have invalid name.

## version 1.16.1
- [new] Drops too long JSON rows (>4MB) not to stop Redshift loading (COPY).

## version 1.16.0
- Detects Unix times in milliseconds and converts it as milliseconds, automatically.

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
