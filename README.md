# Bricolage Streaming Preprocessor

Bricolage Streaming Preprocessor service processes JSON data stream (S3 to S3).

This software is written in working time in Cookpad, Inc.

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

### `timezone`: Timezone Conversion

- `sourceOffset`: Expected source data timezone, given by the string like `'+00:00'`.
- `targetOffset`: Target timezone, given by the string like `'+09:00'`.

### `removenull`: Removes All Null Columns

Removes all null columns e.g. {a: null, b: 1}` -> `{b: 1}`.
`target_column` must be `'*'`.

- No Parameters.

### `aggregate`: Aggregates Multiple Columns to One JSON String

Aggregates multiple target columns to one nested JSON column.
e.g. `{x:1,p_a:2,p_b:3}` -> `{x:1,p:{a:2,b:3}}`

- `targetColumns`: Target column names to aggreagate, given by the Java regex pattern (e.g. `"^p_"`).
  Note: matched string is removed from output column name.
- `aggregatedColumn`: Aggregated, output column name.

## Development

Building Executable JAR file and running all tests:
```
% gradle build
```

Executing built program:
```
% ./run.sh APP_CONFIG_FILE
```

## License

MIT license. See LICENSE file for details.

## Author

Minero Aoki
