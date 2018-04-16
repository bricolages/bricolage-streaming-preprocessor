package org.bricolages.streaming.stream;
import org.bricolages.streaming.stream.processor.*;
import java.util.Objects;
import lombok.*;

@RequiredArgsConstructor
public enum StreamColumnType {
    // We cannot move these statements to each classes or static block,
    // because it may cause class loading problem.
    // Java static blocks are executed on loading the class,
    // but JVM loads a class when the class is used.
    // So we must refer processor classes here to load them.
    BIGINT("bigint", BigintColumnProcessor::build),
    BOOLEAN("boolean", BooleanColumnProcessor::build),
    DATE("date", DateColumnProcessor::build),
    DOUBLE("double", DoubleColumnProcessor::build),
    INTEGER("integer", IntegerColumnProcessor::build),
    REAL("real", RealColumnProcessor::build),
    SMALLINT("smallint", SmallintColumnProcessor::build),
    STRING("string", StringColumnProcessor::build),
    TIMESTAMP("timestamp", TimestampColumnProcessor::build),
    OBJECT("object", ObjectColumnProcessor::build),
    UNKNOWN("unknown", UnknownColumnProcessor::build);

    @Getter
    final String name;

    @Getter
    final StreamColumnProcessorBuilder processorBuilder;

    @Override
    public String toString() {
        return name;
    }

    static public StreamColumnType intern(String typeName) {
        return valueOf(typeName.toUpperCase());
    }
}
