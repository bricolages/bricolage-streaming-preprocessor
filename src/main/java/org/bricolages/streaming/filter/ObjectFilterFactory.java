package org.bricolages.streaming.filter;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.List;
import java.util.stream.Collectors;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ObjectFilterFactory {
    @Autowired
    OperatorDefinitionRepository repos;

    public ObjectFilter load(TableId table) {
        List<OperatorDefinition> defs = repos.findByTargetTable(table.toString());
        List<Op> ops = defs.stream().map(Op::build).collect(Collectors.toList());
        return new ObjectFilter(ops);
    }
}
