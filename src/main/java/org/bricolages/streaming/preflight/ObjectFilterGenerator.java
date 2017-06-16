package org.bricolages.streaming.preflight;
import org.bricolages.streaming.filter.OperatorDefinition;
import org.bricolages.streaming.filter.RenameOp;
import org.bricolages.streaming.ConfigError;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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

    private Stream<OperatorDefinition> generateSingleColumnOperators(ColumnParametersEntry columnDef) {
        val columnName = columnDef.getName();
        try {
            val opDefs = columnDef.getOperatorDefinitionEntries();
            val originalName = columnDef.getOriginalName();
            val ret = new ArrayList<OperatorDefinition>();
            if (originalName != null) {
                val renameParams = new RenameOp.Parameters();
                renameParams.setTo(columnName);
                val opDef = new OperatorDefinitionEntry("rename", renameParams);
                ret.add(new OperatorDefinition(opDef.getOperatorId(), originalName, opDef.getParams(), 0));
            }
            for (val opDef: opDefs) {
                ret.add(new OperatorDefinition(opDef.getOperatorId(), columnName, opDef.getParams(), ret.size() * 10));
            }
            ret.add(new OperatorDefinition("deletenulls", "*", "{}", ret.size() * 10));
            return ret.stream();
        }
        catch (ConfigError ex) {
            throw new ConfigError(columnName + " column: " + ex.getMessage());
        }
    }
}
