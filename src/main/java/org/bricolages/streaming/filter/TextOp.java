package org.bricolages.streaming.filter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import lombok.*;

class TextOp extends SingleColumnOp {
    static final void register() {
        Op.registerOperator("text", (def) ->
            new TextOp(def, def.mapParameters(Parameters.class))
        );
    }

    @Getter
    @Setter
    static final class Parameters {
        long maxByteLength;
        boolean dropIfOverflow;
        boolean createOverflowFlag;
        String pattern;
    }

    final long maxByteLength;
    final boolean dropIfOverflow;
    final boolean createOverflowFlag;
    final Pattern pattern;

    TextOp(OperatorDefinition def, Parameters params) {
        this(def, params.maxByteLength, params.dropIfOverflow, params.createOverflowFlag, params.pattern);
    }

    TextOp(OperatorDefinition def, long maxByteLength, boolean dropIfOverflow, boolean createOverflowFlag, String pattern) {
        super(def);
        this.maxByteLength = maxByteLength;
        this.dropIfOverflow = dropIfOverflow;
        this.createOverflowFlag = createOverflowFlag;
        this.pattern = (pattern == null) ? null : Pattern.compile(pattern);
    }

    static final Charset DATA_FILE_CHARSET = StandardCharsets.UTF_8;

    @Override
    protected Object applyValue(Object value, Record record) throws FilterException {
        String str = removeHeadNullChars(castStringForce(value));
        if (str == null) return null;
        if (maxByteLength > 0) {
            boolean overflow = (str.getBytes(DATA_FILE_CHARSET).length > maxByteLength);
            if (createOverflowFlag) {
                record.put(overflowFlagName(), overflow);
            }
            if (overflow && dropIfOverflow) return null;
        }
        if (pattern != null) {
            if (!pattern.matcher(str).lookingAt()) return null;
        }
        return str;
    }

    String overflowFlagName() {
        return targetColumnName() + "_overflow";
    }

    String castStringForce(Object value) {
        if (value == null) return null;
        if (!(value instanceof String)) return null;
        return (String)value;
    }

    final Pattern nullCharsPattern = Pattern.compile("^\\x00+$");
    String removeHeadNullChars(String str) {
        if (str == null) return null;
        // return if first char is not \0
        if (0 != str.indexOf("\0")) return str;
        // return null if all chars are \0
        if (nullCharsPattern.matcher(str).matches()) return null;
        // remove heading \0(s) if some chars follows
        return str.replaceFirst("^\\x00+", "");
    }
}
