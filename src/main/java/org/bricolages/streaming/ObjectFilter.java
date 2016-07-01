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
            Object outObj = applyObject(obj);
            if (outObj != null) {
                return mapper.writeValueAsString(outObj);
            }
            else {
                return null;
            }
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
