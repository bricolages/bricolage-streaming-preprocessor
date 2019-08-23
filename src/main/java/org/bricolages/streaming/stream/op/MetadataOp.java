package org.bricolages.streaming.stream.op;
import org.bricolages.streaming.object.Record;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;

public class MetadataOp extends Op {
    static final void register(OpBuilder builder) {
        builder.registerOperator("metadata", (def, ctx) ->
            new MetadataOp(def, def.mapParameters(Parameters.class), ctx.getStreamPrefix())
        );
    }

    @Getter
    @Setter
    public static class Parameters {
        String component;
        boolean overwrite = false;
    }

    static enum Component {
        TABLE_NAME,
        SCHEMA_NAME,
        NONE;

        static public final Component intern(String name) {
            if (Objects.equals(name, "tableName")) {
                return Component.TABLE_NAME;
            }
            else if (Objects.equals(name, "schemaName")) {
                return Component.SCHEMA_NAME;
            }
            else {
                return Component.NONE;
            }
        }
    }

    final String component;
    final boolean overwrite;
    final String value;

    MetadataOp(OperatorDefinition def, Parameters params, String streamPrefix) {
        this(def, params.component, params.overwrite, streamPrefix);
    }

    MetadataOp(OperatorDefinition def, String component, boolean overwrite, String streamPrefix) {
        super(def);
        this.component= component;
        this.overwrite = overwrite;

        switch (Component.intern(component)) {
        case TABLE_NAME:
            this.value = getTableName(streamPrefix);
            break;
        case SCHEMA_NAME:
            this.value = getSchemaName(streamPrefix);
            break;
        default:
            this.value = null;
        }
    }

    String getTableName(String s3UrlPrefix) {
        String[] tags = getFirstPrefix(s3UrlPrefix).split("\\.");
        return tags[tags.length - 1];
    }

    String getSchemaName(String s3UrlPrefix) {
        String[] tags = getFirstPrefix(s3UrlPrefix).split("\\.");
        return tags[tags.length - 2];
    }

    String getFirstPrefix(String s3UrlPrefix) {
        return s3UrlPrefix.replaceFirst("^s3://", "").split("/", 2)[0];
    }

    @Override
    public Record apply(Record record) {
        String column = getColumnName();
        if (this.value != null) {
            if (overwrite || record.get(column) != null) {
                record.put(column, value);
            }
        }
        return record;
    }
}
