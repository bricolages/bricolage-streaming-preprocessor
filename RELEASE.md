# Release Note

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
