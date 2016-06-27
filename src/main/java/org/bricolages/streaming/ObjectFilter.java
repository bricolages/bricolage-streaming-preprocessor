package org.bricolages.streaming;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.PrintWriter;
import lombok.*;

class ObjectFilter {
    static final ObjectMapper mapper = new ObjectMapper();

    public ObjectFilter() {
        super();
    }

    public void apply(BufferedReader r, BufferedWriter w) throws IOException {
        final PrintWriter out = new PrintWriter(w);
        r.lines().forEach((line) -> {
            String result = applyString(line);
            if (result != null) {
                out.println(result);
            }
        });
    }

    public String applyString(String json) {
        try {
            Map<String, Object> obj = mapper.readValue(json, Map.class);
            Object result = applyObject(obj);
            return mapper.writeValueAsString(result);
        }
        catch (JsonProcessingException ex) {
            // FIXME
            return null;
        }
        catch (IOException ex) {
            // FIXME
            return null;
        }
    }

    public Object applyObject(Map<String, Object> obj) {
        // FIXME: parameterize
        obj.put("extra", "value");
        return obj;
    }
}
