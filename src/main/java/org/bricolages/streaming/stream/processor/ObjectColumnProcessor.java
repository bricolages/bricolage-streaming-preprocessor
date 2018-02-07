package org.bricolages.streaming.stream.processor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.*;
import org.bricolages.streaming.exception.*;
import java.util.Map;
import java.io.IOException;
import lombok.*;

public class ObjectColumnProcessor extends SingleColumnProcessor {
    static ObjectColumnProcessor build(StreamColumn column, ProcessorContext ctx) {
        val len = column.getLength();
        if (len <= 0) {
            throw new ConfigError("object column requires positive length: " + column.getName());
        }
        return new ObjectColumnProcessor(column, len);
    }

    final int length;

    public ObjectColumnProcessor(StreamColumn column, int length) {
        super(column);
        this.length = length;
    }

    static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Object processValue(Object value) throws FilterException {
        if (value == null) return null;
        if (value instanceof String && ((String)value).startsWith("{")) {
            try {
                value = MAPPER.readValue((String)value, new TypeReference<Map<Object, Object>>() {});
            }
            catch (IOException ex) {
                throw new FilterException("JSON parse error: " + ex.getMessage());
            }
        }
        checkJSONObject(value);
        return value;
    }

    void checkJSONObject(Object obj) throws FilterException {
        try {
            val json = MAPPER.writeValueAsString(obj);
            if (json.length() > this.length) {
                throw new FilterException("object too long: length=" + json.length());
            }
        }
        catch (JsonProcessingException ex) {
            throw new FilterException("JSON serialize error: " + ex.getMessage());
        }
    }
}
