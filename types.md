## `integer` type
32bit signed integral number

No parameters.

## `unixtime` type
Timestamp converted from unix time

- `zone_offset`: `String`
  - Target timezone, given by the string like '+09:00'

## `string` type
Text

This provides a shorthand such as `!string [bytes]`

- `bytes`: `Integer`
  - Declares max byte length

## `boolean` type
Boolean

No parameters.

## `bigint` type
64bit signed integral number

No parameters.

## `float` type
64bit floating point number

No parameters.

## `date` type
Date

No parameters.

## `timestamp` type
Timestamp with zone adjust

- `source_offset`: `String`
  - Source timezone, given by the string like '+00:00'
- `target_offset`: `String`
  - Target timezone, given by the string like '+09:00'

## `domain` type
A special type to load domain by given name

This provides a shorthand such as `!domain [name]`

- `name`: `String`
  - Domain name to load

