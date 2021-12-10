# Filter Operators

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
