package org.bricolages.streaming.filter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import lombok.*;

@Slf4j
public class Record {
    static final ObjectMapper MAPPER = new ObjectMapper();

    static public Record parse(String json) throws JSONException {
        try {
            Map<Object, Object> obj = MAPPER.readValue(json, new TypeReference<Map<Object, Object>>() {});
            return new Record(obj);
        }
        catch (ClassCastException ex) {
            throw new JSONException("record is not a map: " + json);
        }
        catch (JsonProcessingException ex) {
            throw new JSONException(ex.getMessage());
        }
        catch (IOException ex) {
            log.error("IO exception while processing JSON???", ex);
            return null;
        }
    }

    final Map<Object, Object> object;

    public Record() {
        this(new HashMap<Object, Object>());
    }

    public Record(Map<Object, Object> object) {
        this.object = object;
    }

    public String serialize() throws JSONException {
        try {
            return MAPPER.writeValueAsString(object);
        }
        catch (JsonProcessingException ex) {
            throw new JSONException(ex.getMessage());
        }
    }

    public Map<Object, Object> getObject() {
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
        return object.isEmpty();
    }

    public Iterator<Map.Entry<Object, Object>> entries() {
        return object.entrySet().iterator();
    }
}
