package org.bricolages.streaming.stream.op;
import org.bricolages.streaming.stream.processor.Cleanse;
import org.bricolages.streaming.stream.processor.CleanseException;
import org.bricolages.streaming.object.Record;
import java.time.ZoneOffset;
import lombok.*;

public class TimeZoneOp extends SingleColumnOp {
    static final void register(OpBuilder builder) {
        builder.registerOperator("timezone", (def) ->
            new TimeZoneOp(def, def.mapParameters(Parameters.class))
        );
    }

    @Getter
    @Setter
    public static class Parameters {
        String sourceOffset;
        String targetOffset;
        boolean truncate;
    }

    final ZoneOffset sourceOffset;
    final ZoneOffset targetOffset;
    final boolean truncate;

    TimeZoneOp(OperatorDefinition def, Parameters params) {
        this(def, params.sourceOffset, params.targetOffset, params.truncate);
    }

    TimeZoneOp(OperatorDefinition def, String sourceOffset, String targetOffset, boolean truncate) {
        super(def);
        this.sourceOffset = ZoneOffset.of(sourceOffset);
        this.targetOffset = ZoneOffset.of(targetOffset);
        this.truncate = truncate;
    }

    @Override
    public Object applyValue(Object value, Record record) throws FilterException, CleanseException {
        if (value == null) return null;
        return Cleanse.formatSqlTimestamp(Cleanse.getOffsetDateTime(value, sourceOffset, true).withOffsetSameInstant(targetOffset));
    }
}
