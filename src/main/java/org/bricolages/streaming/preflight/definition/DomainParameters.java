package org.bricolages.streaming.preflight.definition;
import org.bricolages.streaming.preflight.domains.*;
import org.bricolages.streaming.stream.StreamColumn;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.List;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    defaultImpl = DomainParametersEntry.class
)
@JsonSubTypes({
    @JsonSubTypes.Type(SmallintDomain.class),
    @JsonSubTypes.Type(IntegerDomain.class),
    @JsonSubTypes.Type(UnixtimeDomain.class),
    @JsonSubTypes.Type(StringDomain.class),
    @JsonSubTypes.Type(BooleanDomain.class),
    @JsonSubTypes.Type(BigintDomain.class),
    @JsonSubTypes.Type(RealDomain.class),
    @JsonSubTypes.Type(DoubleDomain.class),
    @JsonSubTypes.Type(DateDomain.class),
    @JsonSubTypes.Type(TimestampDomain.class),
    @JsonSubTypes.Type(TimestamptzDomain.class),
    @JsonSubTypes.Type(DomainParametersReference.class),
})
public interface DomainParameters {
    String getType();
    ColumnEncoding getEncoding();
    List<OperatorDefinitionEntry> getOperatorDefinitionEntries();
    StreamColumn.Params getStreamColumnParams();
}
