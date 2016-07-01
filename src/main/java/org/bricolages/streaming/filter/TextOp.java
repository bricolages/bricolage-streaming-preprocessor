package org.bricolages.streaming.filter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import lombok.*;

class TextOp extends Op {
    long maxByteLength;
    final Pattern pattern;

    public TextOp(long maxByteLength, String pattern) {
        this.maxByteLength = maxByteLength;
        this.pattern = (pattern != null) ? Pattern.compile(pattern) : null;
    }

    static final Charset DATA_FILE_CHARSET = StandardCharsets.UTF_8;
    @Override
    public Object apply(Object value) throws FilterException {
        if (value == null) return null;
        if (!(value instanceof String)) return null;
        String str = (String)value;
        if (maxByteLength > 0 && str.getBytes(DATA_FILE_CHARSET).length > maxByteLength) return null;
        if (pattern != null && !pattern.matcher(str).lookingAt()) return null;
        return str;
    }
}
