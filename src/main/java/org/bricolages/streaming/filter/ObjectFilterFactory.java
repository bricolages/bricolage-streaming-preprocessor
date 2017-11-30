package org.bricolages.streaming.filter;
import org.bricolages.streaming.stream.DataStream;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.*;

@Slf4j
public class ObjectFilterFactory {
    @Autowired
    OpBuilder builder;

    public ObjectFilter load(DataStream stream) {
        val defs = stream.getOperatorDefinitions();
        return compose(defs);
    }

    public ObjectFilter compose(List<OperatorDefinition> defs) {
        val ops = defs.stream().map(def -> builder.build(def)).collect(Collectors.toList());
        return new ObjectFilter(ops);
    }
}
