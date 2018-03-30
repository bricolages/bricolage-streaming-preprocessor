package org.bricolages.streaming.preflight.definition;
import org.bricolages.streaming.stream.StreamColumn;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    defaultImpl = ColumnDefinitionEntry.class
)
@JsonSubTypes({
    @JsonSubTypes.Type(ColumnDefinitionReference.class),
})
public interface ColumnDefinition {
    public String getName();
    public String getOriginalName();
    public DomainParameters getDomain();
    public StreamColumn getStreamColumn();
}
