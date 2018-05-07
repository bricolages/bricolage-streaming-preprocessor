package org.bricolages.streaming.preflight;
import org.bricolages.streaming.preflight.definition.ColumnDefinition;
import org.bricolages.streaming.preflight.definition.OperatorDefinitionEntry;
import org.bricolages.streaming.preflight.definition.StreamDefinitionEntry;
import org.bricolages.streaming.filter.ObjectFilterFactory;
import org.bricolages.streaming.filter.ObjectFilter;
import org.bricolages.streaming.filter.OperatorDefinition;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.stream.processor.StreamColumnProcessor;
import org.bricolages.streaming.stream.processor.ProcessorContext;
import org.bricolages.streaming.exception.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.*;

class ObjectFilterGenerator implements ProcessorContext {
    final ObjectFilterFactory factory;
    final StreamDefinitionEntry streamDef;

    ObjectFilterGenerator(ObjectFilterFactory factory, StreamDefinitionEntry streamDef) {
        this.factory = factory;
        this.streamDef = streamDef;
    }

    public ObjectFilter generate() {
        val ops = generateOperators();
        val procs = generateProcessors();
        return factory.compose(ops, procs);
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
        ProcessorContext ctx = this;
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

    // implements ProcessorContext
    public Set<String> getStreamColumns() {
        return streamDef.getColumns().stream().map(col -> col.getOriginalName()).collect(Collectors.toSet());
    }
}
