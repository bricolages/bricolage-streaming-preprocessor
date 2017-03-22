package org.bricolages.streaming.preflight;

import java.util.List;
import org.bricolages.streaming.preflight.domains.*;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(CustomColumnParametersEntry.class),
    @JsonSubTypes.Type(IntegerDomain.class),
    @JsonSubTypes.Type(UnixtimeDomain.class),
    @JsonSubTypes.Type(LogTimeDomain.class),
    @JsonSubTypes.Type(StringDomain.class),
    @JsonSubTypes.Type(BooleanDomain.class),
    @JsonSubTypes.Type(BigintDomain.class),
    @JsonSubTypes.Type(FloatDomain.class),
    @JsonSubTypes.Type(DateDomain.class),
})
public interface ColumnParametersEntry {
    String getType();

    ColumnEncoding getEncoding();

    List<OperatorDefinitionEntry> getOperatorDefinitionEntries(String columnName);
}
