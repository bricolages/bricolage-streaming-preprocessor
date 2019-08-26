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
        STREAM_NAME,
        NONE;

        static public final Component intern(String name) {
            if (Objects.equals(name, "streamName")) {
                return Component.STREAM_NAME;
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
        this.component = component;
        this.overwrite = overwrite;
        this.value = computeValue(component, streamPrefix);
    }

    String computeValue(String component, String streamPrefix) {
        switch (Component.intern(component)) {
        case STREAM_NAME:
            return getStreamName(streamPrefix);
        default:
            return null;
        }
    }

    String getStreamName(String prefix) {
        String firstPrefix = prefix.split("/")[0];
        String[] tags = firstPrefix.split("\\.");
        return tags[tags.length - 1];
    }

    @Override
    public Record apply(Record record) {
        String column = getColumnName();
        if (this.value != null) {
            if (overwrite || record.get(column) == null) {
                record.put(column, value);
            }
        }
        return record;
    }
}
