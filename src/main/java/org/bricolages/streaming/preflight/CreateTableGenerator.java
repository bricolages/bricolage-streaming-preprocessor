package org.bricolages.streaming.preflight;

import java.util.StringJoiner;
import lombok.*;

class CreateTableGenerator {
    private final StreamDefinitionEntry streamDef;
    private final String fullTableName;
    CreateTableGenerator(StreamDefinitionEntry streamDef, String fullTableName) {
        this.streamDef = streamDef;
        this.fullTableName = fullTableName;
    }

    String generate() {
        val sb = new StringBuilder();
        sb.append("--dest-table: ");
        sb.append(fullTableName);
        sb.append("\n\n");
        sb.append("create table $dest_table \n(");
        generateColumnDefinitionList(sb);
        sb.append("\n)\n");
        sb.append("sortkey(jst_time)\n");
        sb.append(";\n");

        return sb.toString();
    }

    void generateColumnDefinitionList(StringBuilder sb) {
        val sj = new StringJoiner("\n,");
        for (val columnDef: streamDef.getColumns()) {
            sj.add(generateColumnDefinition(columnDef));
        }
        sb.append(sj.toString());
    }

    String generateColumnDefinition(ColumnDefinitionEntry columnDef) {
        val sj = new StringJoiner(" ", " ", "");
        sj.add(columnDef.getColumName());
        sj.add(columnDef.getParams().getType());
        sj.add("encode");
        sj.add(columnDef.getParams().getEncoding().toString());
        
        return sj.toString();
    }
}

