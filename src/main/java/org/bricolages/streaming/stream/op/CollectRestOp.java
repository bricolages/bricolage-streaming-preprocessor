package org.bricolages.streaming.stream.op;
import org.bricolages.streaming.object.Record;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import lombok.*;

public class CollectRestOp extends Op {
    static final void register(OpBuilder builder) {
        builder.registerOperator("collectrest", (def) ->
            new CollectRestOp(def, def.mapParameters(Parameters.class))
        );
    }

    @Getter
    @Setter
    public static class Parameters {
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
                val k = ent.getKey();
                val v = ent.getValue();
                if (k instanceof String && v != null) {
                    buf.put((String)k, v);
                }
                it.remove();
            }
        }
        record.put(aggregatedColumn, buf.getObject());
        return record;
    }
}
