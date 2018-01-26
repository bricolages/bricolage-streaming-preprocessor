package org.bricolages.streaming.filter;
import org.bricolages.streaming.stream.PacketStream;
import org.bricolages.streaming.locator.LocatorIOManager;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.*;

@Slf4j
public class ObjectFilterFactory {
    @Autowired
    OpBuilder builder;

    @Autowired
    LocatorIOManager ioManager;

    public ObjectFilter load(PacketStream stream) {
        val defs = stream.getOperatorDefinitions();
        return compose(defs);
    }

    public ObjectFilter compose(List<OperatorDefinition> defs) {
        val ops = defs.stream().map(def -> builder.build(def)).collect(Collectors.toList());
        return new ObjectFilter(ioManager, ops);
    }
}
