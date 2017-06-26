package org.bricolages.streaming.preflight.definition;

import java.util.List;
import org.bricolages.streaming.preflight.domains.*;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    defaultImpl = DomainParametersEntry.class
)
@JsonSubTypes({
    @JsonSubTypes.Type(IntegerDomain.class),
    @JsonSubTypes.Type(UnixtimeDomain.class),
    @JsonSubTypes.Type(StringDomain.class),
    @JsonSubTypes.Type(BooleanDomain.class),
    @JsonSubTypes.Type(BigintDomain.class),
    @JsonSubTypes.Type(FloatDomain.class),
    @JsonSubTypes.Type(DateDomain.class),
    @JsonSubTypes.Type(TimestampDomain.class),
    @JsonSubTypes.Type(DomainParametersReference.class),
})
public interface DomainParameters {
    String getType();
    ColumnEncoding getEncoding();
    List<OperatorDefinitionEntry> getOperatorDefinitionEntries();
}
