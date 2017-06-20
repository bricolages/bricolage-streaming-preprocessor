package org.bricolages.streaming.preflight;
import java.util.StringJoiner;
import lombok.*;

@RequiredArgsConstructor
class CreateTableGenerator {
    final StreamDefinitionEntry streamDef;
    final String fullTableName;

    String generate() {
        val sb = new StringBuilder();
        sb.append("--dest-table: ");
        sb.append(fullTableName);
        sb.append("\n\n");
        sb.append("create table $dest_table\n(");
        generateColumnDefinitionList(sb);
        sb.append("\n)\n");
        sb.append("diststyle even\n");
        sb.append("sortkey (jst_time)\n");
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

    String generateColumnDefinition(ColumnParametersEntry columnDef) {
        val sj = new StringJoiner(" ", " ", "");
        sj.add(columnDef.getName());
        sj.add(columnDef.getType());
        sj.add("encode");
        sj.add(columnDef.getEncoding().toString());
        
        return sj.toString();
    }
}
