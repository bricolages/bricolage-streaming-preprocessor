package org.bricolages.streaming.preflight;

import java.util.List;
import org.bricolages.streaming.preflight.types.*;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type",
    defaultImpl = ColumnDefinitionEntry.class
)
@JsonSubTypes({
    @JsonSubTypes.Type(IntegerType.class),
    @JsonSubTypes.Type(UnixtimeType.class),
    @JsonSubTypes.Type(StringType.class),
    @JsonSubTypes.Type(BooleanType.class),
    @JsonSubTypes.Type(BigintType.class),
    @JsonSubTypes.Type(FloatType.class),
    @JsonSubTypes.Type(DateType.class),
    @JsonSubTypes.Type(TimestampType.class),
    @JsonSubTypes.Type(DomainType.class),
})
public interface ColumnParametersEntry {
    String getName();
    String getType();
    ColumnEncoding getEncoding();
    String getOriginalName();
    List<OperatorDefinitionEntry> getOperatorDefinitionEntries(String columnName);
}
