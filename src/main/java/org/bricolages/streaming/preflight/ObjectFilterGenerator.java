package org.bricolages.streaming.preflight;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bricolages.streaming.filter.OperatorDefinition;
import lombok.*;

class ObjectFilterGenerator {
    StreamDefinitionEntry streamDef;

    ObjectFilterGenerator(StreamDefinitionEntry streamDef) {
        this.streamDef = streamDef;
    }

    public List<OperatorDefinition> generate() {
        return generateOperatorDefinitions().collect(Collectors.toList());
    }

    private Stream<OperatorDefinition> generateOperatorDefinitions() {
        return streamDef.getColumns().stream().flatMap(this::generateSingleColumnOperators);
    }

    private Stream<OperatorDefinition> generateSingleColumnOperators(ColumnDefinitionEntry columnDef) {
        val columnName = columnDef.getColumName();
        val opDefs = columnDef.getParams().getOperatorDefinitionEntries(columnName);
        val ret = new ArrayList<OperatorDefinition>();
        for(val opDef: opDefs) {
            ret.add(new OperatorDefinition(opDef.getOperatorId(), opDef.getTargetColumn(), opDef.getParams(), ret.size() * 10));
        }
        return ret.stream();
    }
}
