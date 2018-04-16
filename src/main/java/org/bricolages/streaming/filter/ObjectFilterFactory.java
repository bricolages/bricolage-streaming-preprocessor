package org.bricolages.streaming.filter;
import org.bricolages.streaming.stream.*;
import org.bricolages.streaming.stream.processor.StreamColumnProcessor;
import org.bricolages.streaming.stream.processor.ProcessorContext;
import org.bricolages.streaming.locator.LocatorIOManager;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.*;

@Slf4j
public class ObjectFilterFactory {
    @Autowired
    OpBuilder builder;

    @Autowired
    LocatorIOManager ioManager;

    @Autowired
    StreamColumnRepository columnRepos;

    public ObjectFilter load(PacketStream stream) {
        val ops = buildOperators(stream.getOperatorDefinitions());
        if (stream.doesUseColumn()) {
            log.debug("enables column processor: {}", stream.getStreamName());
            val procs = buildProcessors(stream);
            return new ObjectFilter(ioManager, ops, procs);
        }
        else {
            return new ObjectFilter(ioManager, ops);
        }
    }

    public ObjectFilter compose(List<OperatorDefinition> opDefs, List<StreamColumnProcessor> procs) {
        val ops = buildOperators(opDefs);
        return new ObjectFilter(ioManager, ops, procs);
    }

    List<Op> buildOperators(List<OperatorDefinition> defs) {
        return defs.stream().map(def -> builder.build(def)).collect(Collectors.toList());
    }

    List<StreamColumnProcessor> buildProcessors(PacketStream stream) {
        val columns = columnRepos.findColumns(stream);
        val ctx = new Context(stream, columns);
        return columns.stream().map(col -> col.buildProcessor(ctx)).collect(Collectors.toList());
    }

    @RequiredArgsConstructor
    static class Context implements ProcessorContext {
        final PacketStream stream;
        final List<StreamColumn> columns;

        public Set<String> getStreamColumns() {
            return columns.stream().map(col -> col.getSourceName()).collect(Collectors.toSet());
        }
    }
}
