package org.bricolages.streaming.stream;
import org.bricolages.streaming.stream.processor.StreamColumnProcessor;
import org.bricolages.streaming.stream.processor.ProcessorContext;
import org.bricolages.streaming.stream.op.OperatorDefinition;
import org.bricolages.streaming.stream.op.OpBuilder;
import org.bricolages.streaming.stream.op.OpContext;
import org.bricolages.streaming.stream.op.Op;
import org.bricolages.streaming.stream.op.SequencialNumberRepository;
import org.bricolages.streaming.object.ObjectIOManager;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PacketFilterFactory {
    // to avoid duplicated op registeration
    static final OpBuilder builder = new OpBuilder();

    @Autowired ObjectIOManager ioManager;
    @Autowired StreamColumnRepository columnRepos;
    @Autowired SequencialNumberRepository sequencialNumberRepository;

    @Autowired
    public PacketFilterFactory() {
    }

    public PacketFilter load(Route route) {
        val stream = route.getStream();
        val ctx = new OpContextImpl(route.getPrefix(), sequencialNumberRepository);
        val ops = buildOperators(stream.getOperatorDefinitions(), ctx);
        if (stream.doesUseColumn()) {
            log.debug("enables column processor: {}", stream.getStreamName());
            val procs = buildProcessors(stream);
            return new PacketFilter(ioManager, ops, procs);
        }
        else {
            return new PacketFilter(ioManager, ops);
        }
    }

    // For preflight
    public PacketFilter compose(List<OperatorDefinition> opDefs, List<StreamColumnProcessor> procs, String streamPrefix) {
        val ctx = new OpContextImpl(streamPrefix, sequencialNumberRepository);
        val ops = buildOperators(opDefs, ctx);
        return new PacketFilter(ioManager, ops, procs);
    }

    List<Op> buildOperators(List<OperatorDefinition> defs, OpContext ctx) {
        return defs.stream().map(def -> builder.build(def, ctx)).collect(Collectors.toList());
    }

    @AllArgsConstructor
    @RequiredArgsConstructor
    static public class OpContextImpl implements OpContext {
        @Getter final String streamPrefix;
        @Getter SequencialNumberRepository sequencialNumberRepository = null;
    }

    List<StreamColumnProcessor> buildProcessors(PacketStream stream) {
        val columns = columnRepos.findColumns(stream);
        val ctx = new ProcessorContextImpl(stream, columns);
        return columns.stream().map(col -> col.buildProcessor(ctx)).collect(Collectors.toList());
    }

    @RequiredArgsConstructor
    static public class ProcessorContextImpl implements ProcessorContext {
        final PacketStream stream;
        final List<StreamColumn> columns;

        public Set<String> getStreamColumns() {
            return columns.stream().map(col -> col.getSourceName()).collect(Collectors.toSet());
        }
    }
}
