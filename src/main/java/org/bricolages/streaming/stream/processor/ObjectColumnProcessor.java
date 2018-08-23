package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.exception.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Map;
import java.io.IOException;
import lombok.*;

public class ObjectColumnProcessor extends SingleColumnProcessor {
    static public ObjectColumnProcessor build(ProcessorParams params, ProcessorContext ctx) {
        val lenObj = params.getLength();
        if (lenObj == null) {
            throw new ConfigError("length is required: " + params.getName());
        }
        val len = (int)lenObj;
        if (len <= 0) {
            throw new ConfigError("object params requires positive length: " + params.getName());
        }
        return new ObjectColumnProcessor(params, len);
    }

    final int length;

    public ObjectColumnProcessor(ProcessorParams params, int length) {
        super(params);
        this.length = length;
    }

    static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Object processValue(Object value) throws ProcessorException {
        if (value == null) return null;
        if (value instanceof String && ((String)value).startsWith("{")) {
            try {
                value = MAPPER.readValue((String)value, new TypeReference<Map<Object, Object>>() {});
            }
            catch (IOException ex) {
                throw new ProcessorException("JSON parse error: " + ex.getMessage());
            }
        }
        checkJSONObject(value);
        return value;
    }

    void checkJSONObject(Object obj) throws ProcessorException {
        try {
            val json = MAPPER.writeValueAsString(obj);
            if (json.length() > this.length) {
                throw new ProcessorException("object too long: length=" + json.length());
            }
        }
        catch (JsonProcessingException ex) {
            throw new ProcessorException("JSON serialize error: " + ex.getMessage());
        }
    }
}
