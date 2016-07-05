package org.bricolages.streaming.filter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.PrintWriter;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ObjectFilter {
    final List<Op> operators;
    final ObjectMapper mapper = new ObjectMapper();

    public ObjectFilter(List<Op> operators) {
        this.operators = operators;
    }

    public void apply(BufferedReader r, BufferedWriter w, String sourceName, FilterResult result) throws IOException {
        final PrintWriter out = new PrintWriter(w);
        r.lines().forEach((line) -> {
            result.inputRows++;
            try {
                String outStr = applyString(line);
                if (outStr != null) {
                    out.println(outStr);
                    result.outputRows++;
                }
            }
            catch (JsonProcessingException ex) {
                log.debug("JSON parse error: {}:{}: {}", sourceName, result.inputRows, ex.getMessage());
                result.errorRows++;
            }
        });
    }

    public String applyString(String json) throws JsonProcessingException {
        try {
            Map<String, Object> obj = (Map<String, Object>)mapper.readValue(json, Map.class);
            Record record = new Record(obj);
            for (Op op : operators) {
                record = op.apply(record);
                if (record == null) return null;
            }
            return mapper.writeValueAsString(record.getObject());
        }
        catch (JsonProcessingException ex) {
            throw ex;
        }
        catch (IOException ex) {
            log.error("IO exception while processing JSON???", ex);
            return null;
        }
    }
}
