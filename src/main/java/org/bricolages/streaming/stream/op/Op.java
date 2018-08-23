package org.bricolages.streaming.stream.op;
import org.bricolages.streaming.object.Record;
import java.time.*;
import java.util.regex.Pattern;
import java.time.format.DateTimeFormatter;
import lombok.extern.slf4j.Slf4j;
import lombok.*;

@Slf4j
public abstract class Op {
    final OperatorDefinition def;

    Op(OperatorDefinition def) {
        this.def = def;
    }

    protected String getColumnName() {
        return def.getTargetColumn();
    }

    public abstract Record apply(Record record);
}
