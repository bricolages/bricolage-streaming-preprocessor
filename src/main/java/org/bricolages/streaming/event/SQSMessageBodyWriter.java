package org.bricolages.streaming.event;
import java.util.regex.Pattern;
import java.time.Instant;
import lombok.*;

@NoArgsConstructor
public class SQSMessageBodyWriter {
    StringBuilder buf = new StringBuilder();
    boolean needSeparator = false;

    @Override
    public String toString() {
        return buf.toString();
    }

    public SQSMessageBodyWriter append(String s) {
        buf.append(s);
        return this;
    }

    public SQSMessageBodyWriter pair(String key, String value) {
        return key(key).value(value);
    }

    public SQSMessageBodyWriter pair(String key, long value) {
        return key(key).value(value);
    }

    public SQSMessageBodyWriter pair(String key, Instant value) {
        return key(key).value(value);
    }

    public SQSMessageBodyWriter key(String key) {
        if (needSeparator) {
            buf.append(",");
            this.needSeparator = false;
        }
        buf.append("\"").append(key).append("\":");
        return this;
    }

    public SQSMessageBodyWriter value(long value) {
        return value(Long.toString(value));
    }

    public SQSMessageBodyWriter value(Instant t) {
        return value(t.toString());
    }

    public SQSMessageBodyWriter value(String value) {
        buf.append("\"").append(escapeString(value)).append("\"");
        this.needSeparator = true;
        return this;
    }

    static final Pattern ESCAPE_CHARS = Pattern.compile("[\\\\\"]");

    public String escapeString(String str) {
        return ESCAPE_CHARS.matcher(str).replaceAll("\\\\$&");
    }

    public SQSMessageBodyWriter beginObject(String key) {
        return key(key).beginObject();
    }

    public SQSMessageBodyWriter beginObject() {
        buf.append("{");
        return this;
    }

    public SQSMessageBodyWriter endObject() {
        buf.append("}");
        this.needSeparator = true;
        return this;
    }

    public SQSMessageBodyWriter beginArray(String key) {
        return key(key).beginArray();
    }

    public SQSMessageBodyWriter beginArray() {
        buf.append("[");
        return this;
    }

    public SQSMessageBodyWriter endArray() {
        buf.append("]");
        this.needSeparator = true;
        return this;
    }
}
