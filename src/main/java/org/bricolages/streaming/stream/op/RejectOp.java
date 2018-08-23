package org.bricolages.streaming.stream.op;
import org.bricolages.streaming.object.Record;
import java.util.function.Function;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.*;

public class RejectOp extends Op {
    static final void register(OpBuilder builder) {
        builder.registerOperator("reject", (def) ->
            new RejectOp(def, def.mapParameters(Parameters.class))
        );
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = StringPatameters.class, name = "string"),
        @JsonSubTypes.Type(value = IntegerParameters.class, name = "integer"),
        @JsonSubTypes.Type(value = BooleanParameters.class, name = "boolean"),
        @JsonSubTypes.Type(value = NullParameters.class, name = "null") 
    })
    abstract static class Parameters {
        abstract Function<Object, Boolean> getMatcher();
    }
    public static class StringPatameters extends Parameters {
        @Getter @Setter String value;
        Function<Object, Boolean> getMatcher() {
            return (target) -> value.equals(target);
        }
    }
    public static class IntegerParameters extends Parameters {
        @Getter @Setter Integer value;
        Function<Object, Boolean> getMatcher() {
            return (target) -> value.equals(target);
        }
    }
    public static class BooleanParameters extends Parameters {
        @Getter @Setter Boolean value;
        Function<Object, Boolean> getMatcher() {
            return (target) -> value.equals(target);
        }
    }
    public static class NullParameters extends Parameters {
        Function<Object, Boolean> getMatcher() {
            return (target) -> target == null;
        }
    }

    Function<Object, Boolean> matcher;

    RejectOp(OperatorDefinition def, Parameters params) {
        this(def, params.getMatcher());
    }

    RejectOp(OperatorDefinition def, Function<Object, Boolean> matcher) {
        super(def);
        this.matcher = matcher;
    }

    @Override
    public Record apply(Record record) {
        String targetColumn = getColumnName();
        Object targetValue = record.get(targetColumn);
        if (matcher.apply(targetValue)) {
            return null;
        }
        
        return record;
    }
}
