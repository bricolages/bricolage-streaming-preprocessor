package org.bricolages.streaming.filter;
import java.util.Map;
import java.util.HashMap;
import lombok.*;

public class Record {
    static public Record empty() {
        return new Record(new HashMap<String, Object>());
    }

    final Map<String, Object> object;
    final boolean wasEmpty;

    Record(Map<String, Object> object) {
        this.object = object;
        this.wasEmpty = object.isEmpty();
    }

    Map<String, Object> getObject() {
        return object;
    }

    Object get(String columnName) {
        return object.get(columnName);
    }

    void put(String columnName, Object value) {
        object.put(columnName, value);
    }

    void remove(String columnName) {
        object.remove(columnName);
    }

    boolean isEmpty() {
        return object.isEmpty() && !wasEmpty;
    }
}
