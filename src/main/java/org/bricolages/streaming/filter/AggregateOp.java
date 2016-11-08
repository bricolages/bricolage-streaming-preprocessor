package org.bricolages.streaming.filter;

import lombok.*;

import java.util.regex.Pattern;

class AggregateOp extends Op {
    static final void register() {
        Op.registerOperator("aggregate", (def) ->
            new AggregateOp(def, def.mapParameters(Parameters.class))
        );
    }

    @Getter
    @Setter
    static class Parameters {
        String targetColumns;
        String aggregatedColumn;
        boolean dropTargetColumns = true;
    }

    final Pattern targetColumns;
    final String aggregatedColumn;
    final boolean dropTargetColumns;

    AggregateOp(OperatorDefinition def, Parameters params) {
        this(def, params.targetColumns, params.aggregatedColumn, params.dropTargetColumns);
    }

    AggregateOp(OperatorDefinition def, String targetColumns, String aggregatedColumn, boolean dropTargetColumns) {
        super(def);
        this.targetColumns = Pattern.compile(targetColumns);
        this.aggregatedColumn = aggregatedColumn;
        this.dropTargetColumns = dropTargetColumns;
    }

    @Override
    public Record apply(Record record) {
        val buf = new Record();
        val it = record.entries();
        while (it.hasNext()) {
            val ent = it.next();
            val m = targetColumns.matcher(ent.getKey());
            if (m.find()) {
                if (ent.getValue() != null) {
                    buf.put(m.replaceFirst(""), ent.getValue());
                }
                if(dropTargetColumns) {
                    it.remove();
                }
            }
        }
        record.put(aggregatedColumn, buf.getObject());
        return record;
    }
}
