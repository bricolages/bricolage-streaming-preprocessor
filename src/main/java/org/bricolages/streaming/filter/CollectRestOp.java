package org.bricolages.streaming.filter;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import lombok.*;

class CollectRestOp extends Op {
    static final void register() {
        Op.registerOperator("collectrest", (def) ->
            new CollectRestOp(def, def.mapParameters(Parameters.class))
        );
    }

    @Getter
    @Setter
    static class Parameters {
        String aggregatedColumn;
        List<String> rejectColumns;
    }

    final String aggregatedColumn;
    final Map<String, String> rejectColumns;

    CollectRestOp(OperatorDefinition def, Parameters params) {
        this(def, params.aggregatedColumn, params.rejectColumns);
    }

    CollectRestOp(OperatorDefinition def, String aggregatedColumn, List<String> rejectColumns) {
        super(def);
        this.aggregatedColumn = aggregatedColumn;
        val index = new HashMap<String, String>();
        for (String column : rejectColumns) {
            index.put(column, column);
        }
        this.rejectColumns = index;
    }

    @Override
    public Record apply(Record record) {
        val buf = new Record();
        val it = record.entries();
        while (it.hasNext()) {
            val ent = it.next();
            if (!rejectColumns.containsKey(ent.getKey())) {
                if (ent.getValue() != null) {
                    buf.put(ent.getKey(), ent.getValue());
                }
                it.remove();
            }
        }
        record.put(aggregatedColumn, buf.getObject());
        return record;
    }
}
