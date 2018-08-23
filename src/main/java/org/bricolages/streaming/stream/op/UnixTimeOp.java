package org.bricolages.streaming.stream.op;
import org.bricolages.streaming.stream.processor.Cleanse;
import org.bricolages.streaming.stream.processor.CleanseException;
import org.bricolages.streaming.object.Record;
import java.time.*;
import lombok.*;

public class UnixTimeOp extends SingleColumnOp {
    static final void register(OpBuilder builder) {
        builder.registerOperator("unixtime", (def) ->
            new UnixTimeOp(def, def.mapParameters(Parameters.class))
        );
    }

    @Getter
    @Setter
    public static class Parameters {
        String zoneOffset;
    }

    final ZoneOffset zoneOffset;

    UnixTimeOp(OperatorDefinition def, Parameters params) {
        this(def, params.zoneOffset);
    }

    UnixTimeOp(OperatorDefinition def, String offset) {
        super(def);
        this.zoneOffset = ZoneOffset.of(offset);
    }

    @Override
    public Object applyValue(Object value, Record record) throws FilterException, CleanseException {
        if (value == null) return null;
        if (Cleanse.isFloat(value)) {
            return Cleanse.formatSqlTimestamp(Cleanse.unixTimeToOffsetDateTime(Cleanse.getDouble(value), zoneOffset));
        }
        else {
            return Cleanse.formatSqlTimestamp(Cleanse.unixTimeToOffsetDateTime(Cleanse.getInteger(value), zoneOffset));
        }
    }
}
