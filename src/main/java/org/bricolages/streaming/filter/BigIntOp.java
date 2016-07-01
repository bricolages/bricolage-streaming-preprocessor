package org.bricolages.streaming.filter;
import lombok.*;

@NoArgsConstructor
class BigIntOp extends Op {
    @Override
    public Object apply(Object value) throws FilterException {
        if (value == null) return null;
        long i = getInteger(value);
        return Long.valueOf(i);
    }
}
