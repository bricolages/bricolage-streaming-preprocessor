## `custom` domain
- `type`: `String`
- `encoding`: `ColumnEncoding`
- `filter`: `List`

## `integer` domain
32bit signed integral number

No parameters.

## `unixtime` domain
Timestamp converted from unix time

- `zoneOffset`: `String`
  - Target timezone, given by the string like '+09:00'

## `log_time` domain
Timestamp indicating when log was recorded

Usually this column becomes sortkey.

- `sourceOffset`: `String`
  - Expected source data timezone, given by the string like '+00:00'
- `targetOffset`: `String`
  - Target timezone, given by the string like '+09:00'
- `sourceColumn`: `String`
  - Column name which is renamed from

## `string` domain
Text

This provides a shorthand such as `!string [bytes]`

- `bytes`: `int`
  - Declares max byte length

## `boolean` domain
Boolean

No parameters.

## `bigint` domain
64bit signed integral number

No parameters.

## `float` domain
64bit floating point number

No parameters.

## `date` domain
Date time

- `sourceOffset`: `String`
  - Expected source data timezone, given by the string like '+00:00'
- `targetOffset`: `String`
  - Target timezone, given by the string like '+09:00'

