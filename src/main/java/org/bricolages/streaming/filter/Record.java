package org.bricolages.streaming.filter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import lombok.*;

@Slf4j
public class Record {
    static final ObjectMapper MAPPER = new ObjectMapper();

    static public Record parse(String json) throws JsonProcessingException {
        try {
            Map<String, Object> obj = (Map<String, Object>)MAPPER.readValue(json, Map.class);
            return new Record(obj);
        }
        catch (JsonProcessingException ex) {
            throw ex;
        }
        catch (IOException ex) {
            log.error("IO exception while processing JSON???", ex);
            return null;
        }
    }

    final Map<String, Object> object;
    final boolean wasEmpty;

    public Record() {
        this(new HashMap<String, Object>());
    }

    public Record(Map<String, Object> object) {
        this.object = object;
        this.wasEmpty = object.isEmpty();
    }

    public String serialize() throws JsonProcessingException {
        return MAPPER.writeValueAsString(object);
    }

    public Map<String, Object> getObject() {
        return object;
    }

    public Object get(String columnName) {
        return object.get(columnName);
    }

    public void put(String columnName, Object value) {
        object.put(columnName, value);
    }

    public void remove(String columnName) {
        object.remove(columnName);
    }

    public boolean isEmpty() {
        return object.isEmpty() && !wasEmpty;
    }

    public Iterator<Map.Entry<String, Object>> entries() {
        return object.entrySet().iterator();
    }
}
