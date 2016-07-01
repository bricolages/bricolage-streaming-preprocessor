package org.bricolages.streaming.filter;
import lombok.*;

@NoArgsConstructor
class IntOp extends Op {
    @Override
    public Object apply(Object value) throws FilterException {
        if (value == null) return null;
        long i = getInteger(value);
        if (Integer.MIN_VALUE <= i && i <= Integer.MAX_VALUE) {
            return Integer.valueOf((int)i);
        }
        else {
            return null;
        }
    }
}
