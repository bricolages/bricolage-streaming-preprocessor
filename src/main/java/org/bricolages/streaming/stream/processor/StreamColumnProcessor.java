package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.Record;
import org.bricolages.streaming.filter.FilterException;
import org.bricolages.streaming.exception.*;
import java.util.Objects;
import java.util.Map;
import java.util.HashMap;
import java.util.function.BiFunction;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public abstract class StreamColumnProcessor {
    final StreamColumn column;

    static Map<String, BiFunction<StreamColumn, ProcessorContext, StreamColumnProcessor>> PROC_BUILDER = new HashMap<String, BiFunction<StreamColumn, ProcessorContext, StreamColumnProcessor>>();

    static boolean registered = false;

    static final void registerProcessorBuilders() {
        if (registered) return;

        // We cannot move these statements to each classes or static block,
        // because it may cause class loading problem.
        // Java static blocks are executed on loading the class,
        // but JVM loads a class when the class is used.
        // So we must refer processor classes here to load them.
        register("bigint", BigintColumnProcessor::build);
        register("boolean", BooleanColumnProcessor::build);
        register("date", DateColumnProcessor::build);
        register("double", DoubleColumnProcessor::build);
        register("integer", IntegerColumnProcessor::build);
        register("real", RealColumnProcessor::build);
        register("smallint", SmallintColumnProcessor::build);
        register("string", StringColumnProcessor::build);
        register("timestamp", TimestampColumnProcessor::build);
        register("object", ObjectColumnProcessor::build);

        registered = true;
    }

    static final void register(String typeId, BiFunction<StreamColumn, ProcessorContext, StreamColumnProcessor> f) {
        if (PROC_BUILDER.containsKey(typeId)) {
            throw new RuntimeException("FATAL: StreamColumnProcessor builder registered twice: " + typeId);
        }
        if (f == null) {
            throw new RuntimeException("FATAL: StreamColumnProcessor builder is null: " + typeId);
        }
        PROC_BUILDER.put(typeId, f);
    }

    static public final StreamColumnProcessor forColumn(StreamColumn column, ProcessorContext ctx) {
        registerProcessorBuilders();
        String typeId = column.getType();
        val f = PROC_BUILDER.get(typeId);
        if (f == null) {
            throw new ConfigError("unknown column type: column=" + column.getName() + ", type=" + typeId);
        }
        return f.apply(column, ctx);
    }

    public String getDestName() {
        return column.getName();
    }

    abstract public Object process(Record record);
}
