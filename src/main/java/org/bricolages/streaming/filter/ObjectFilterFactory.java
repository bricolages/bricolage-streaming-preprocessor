package org.bricolages.streaming.filter;
import org.bricolages.streaming.StreamParamsRepository;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.*;

@Slf4j
public class ObjectFilterFactory {
    @Autowired
    StreamParamsRepository repos;

    @Autowired
    OpBuilder builder;

    public ObjectFilter load(TableId table) {
        List<OperatorDefinition> defs = repos.findParams(table).getOperatorDefinitions();
        List<Op> ops = defs.stream().map((def) -> {
            Op op = builder.build(def);
            log.debug("operator stacked: {}", op);
            return op;
        }).collect(Collectors.toList());
        return new ObjectFilter(ops);
    }
}
