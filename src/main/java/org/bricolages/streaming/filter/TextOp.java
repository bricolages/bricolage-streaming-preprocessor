package org.bricolages.streaming.filter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import lombok.*;

class TextOp extends SingleColumnOp {
    static {
        Op.registerOperator("text", (def) ->
            new TextOp(def, def.mapParameters(Parameters.class))
        );
    }

    static final class Parameters {
        long maxByteLength;
        boolean createOverflowFlag;
        String pattern;
    }

    final long maxByteLength;
    final boolean createOverflowFlag;
    final Pattern pattern;

    TextOp(OperatorDefinition def, Parameters params) {
        this(def, params.maxByteLength, params.createOverflowFlag, params.pattern);
    }

    TextOp(OperatorDefinition def, long maxByteLength, boolean createOverflowFlag, String pattern) {
        super(def);
        this.maxByteLength = maxByteLength;
        this.createOverflowFlag = createOverflowFlag;
        this.pattern = (pattern == null) ? null : Pattern.compile(pattern);
    }

    static final Charset DATA_FILE_CHARSET = StandardCharsets.UTF_8;

    @Override
    protected Object applyValue(Object value, Record record) throws FilterException {
        String str = castStringForce(value);
        if (str == null) return null;
        boolean stringOverflow = (maxByteLength > 0 && str.getBytes(DATA_FILE_CHARSET).length > maxByteLength);
        if (createOverflowFlag) {
            record.put(overflowFlagName(), stringOverflow);
        }
        if (stringOverflow) return null;
        if (pattern != null && !pattern.matcher(str).lookingAt()) return null;
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
}
