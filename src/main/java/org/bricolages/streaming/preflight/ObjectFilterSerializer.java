package org.bricolages.streaming.preflight;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.bricolages.streaming.filter.OperatorDefinition;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import lombok.*;

class ObjectFilterSerializer {
    private OutputStream out;
    ObjectFilterSerializer(OutputStream out) {
        this.out = out;
    }

    void serialize(String streamName, List<OperatorDefinition> operators) throws IOException {
        CsvMapper mapper = new CsvMapper();
        
        val rows = operators.stream().map(op -> {
            ArrayList<String> row = new ArrayList<String>(5);
            row.add(streamName);
            if (op.isSingleColumn()) {
                row.add(op.getTargetColumn());
            } else {
                row.add("*");
            }
            row.add(String.valueOf(op.getApplicationOrder()));
            row.add(op.getOperatorId());
            row.add(op.getParams());
            return row;
        }).collect(Collectors.toList());
        mapper.writeValue(out, rows);
    }
}
