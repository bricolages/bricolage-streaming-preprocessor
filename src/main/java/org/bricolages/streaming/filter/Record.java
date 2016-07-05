package org.bricolages.streaming.filter;
import java.util.Map;
import lombok.*;

public class Record {
    final Map<String, Object> object;

    Record(Map<String, Object> object) {
        this.object = object;
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
        return object.isEmpty();
    }
}
