package org.bricolages.streaming;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.PrintWriter;
import lombok.*;
import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class ObjectFilter {
    static final ObjectMapper mapper = new ObjectMapper();

    public ObjectFilter() {
        super();
    }

    public FilterResult apply(BufferedReader r, BufferedWriter w, String sourceName) throws IOException {
        final FilterResult result = FilterResult.empty();
        final PrintWriter out = new PrintWriter(w);
        r.lines().forEach((line) -> {
            result.inputLines++;
            try {
                String outStr = applyString(line);
                if (outStr != null) {
                    out.println(outStr);
                    result.outputLines++;
                }
            }
            catch (JsonProcessingException ex) {
                log.debug("JSON parse error: {}:{}: {}", sourceName, result.inputLines, ex.getMessage());
                result.jsonParseError++;
            }
        });
        return result;
    }

    public String applyString(String json) throws JsonProcessingException {
        try {
            Map<String, Object> obj = (Map<String, Object>)mapper.readValue(json, Map.class);
            Object result = applyObject(obj);
            return mapper.writeValueAsString(result);
        }
        catch (JsonProcessingException ex) {
            throw ex;
        }
        catch (IOException ex) {
            log.error("IO exception while processing JSON???", ex);
            return null;
        }
    }

    public Object applyObject(Map<String, Object> obj) {
        // FIXME: parameterize
        obj.put("extra", "value");
        return obj;
    }
}
