package org.bricolages.streaming.preflight;
import org.bricolages.streaming.preflight.definition.ColumnDefinition;
import org.bricolages.streaming.preflight.definition.OperatorDefinitionEntry;
import org.bricolages.streaming.preflight.definition.StreamDefinitionEntry;
import org.bricolages.streaming.stream.PacketFilterFactory;
import org.bricolages.streaming.stream.PacketFilter;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.stream.processor.StreamColumnProcessor;
import org.bricolages.streaming.stream.processor.ProcessorContext;
import org.bricolages.streaming.stream.op.OperatorDefinition;
import org.bricolages.streaming.exception.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.val;

class PacketFilterGenerator {
    final PacketFilterFactory factory;
    final StreamDefinitionEntry streamDef;

    PacketFilterGenerator(PacketFilterFactory factory, StreamDefinitionEntry streamDef) {
        this.factory = factory;
        this.streamDef = streamDef;
    }

    public PacketFilter generate() {
        val ops = generateOperators();
        val procs = generateProcessors();
        return factory.compose(ops, procs, "t1.t2.sample_schema.sample_table");
    }

    List<OperatorDefinition> generateOperators() {
        val ops = new ArrayList<OperatorDefinition>();
        for (val columnDef : streamDef.getColumns()) {
            val columnName = columnDef.getName();
            try {
                val opDefs = columnDef.getDomain().getOperatorDefinitionEntries();
                for (val opDef: opDefs) {
                    ops.add(new OperatorDefinition(opDef.getOperatorId(), columnName, opDef.getParams(), ops.size() * 10));
                }
            }
            catch (ConfigError ex) {
                throw new ConfigError(columnName + " column: " + ex.getMessage());
            }
        }
        return ops;
    }

    List<StreamColumnProcessor> generateProcessors() {
        ProcessorContext ctx = new ProcessorContextImpl(streamDef);
        val procs = new ArrayList<StreamColumnProcessor>();
        for (val column : streamDef.getColumns()) {
            try {
                StreamColumnProcessor proc = column.getStreamColumn().buildProcessor(ctx);
                procs.add(proc);
            }
            catch (ConfigError ex) {
                throw new ConfigError(column.getName() + " column: " + ex.getMessage());
            }
        }
        return procs;
    }

    @RequiredArgsConstructor
    static final class ProcessorContextImpl implements ProcessorContext {
        final StreamDefinitionEntry streamDef;

        @Override
        public Set<String> getStreamColumns() {
            return streamDef.getColumns().stream().map(col -> col.getOriginalName()).collect(Collectors.toSet());
        }
    }
}
