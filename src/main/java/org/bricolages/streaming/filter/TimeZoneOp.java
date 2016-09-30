package org.bricolages.streaming.filter;
import java.time.ZoneOffset;
import lombok.*;

class TimeZoneOp extends SingleColumnOp {
    static final void register() {
        Op.registerOperator("timezone", (def) ->
            new TimeZoneOp(def, def.mapParameters(Parameters.class))
        );
    }

    @Getter
    @Setter
    static class Parameters {
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
    public Object applyValue(Object value, Record record) throws FilterException {
        if (value == null) return null;
        return formatSqlTimestamp(getOffsetDateTime(value, sourceOffset, true).withOffsetSameInstant(targetOffset));
    }
}
