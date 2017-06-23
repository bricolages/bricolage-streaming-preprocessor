package org.bricolages.streaming.preflight.definition;

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
  public DomainParameters getDomain();

  public String getOriginalName();
}
