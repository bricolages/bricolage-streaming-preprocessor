package org.bricolages.streaming.filter;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.*;

@Slf4j
public class ObjectFilterFactory {
    @Autowired
    OperatorDefinitionRepository repos;

    public ObjectFilter load(TableId table) {
        List<OperatorDefinition> defs = repos.findByTargetTableOrderByApplicationOrderAsc(table.toString());
        List<Op> ops = defs.stream().map((def) -> {
            Op op = Op.build(def);
            log.debug("operator stacked: {}", op);
            return op;
        }).collect(Collectors.toList());
        return new ObjectFilter(ops);
    }
}
