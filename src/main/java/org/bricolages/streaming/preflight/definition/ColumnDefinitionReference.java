package org.bricolages.streaming.preflight.definition;
import org.bricolages.streaming.preflight.definition.WellknownColumnCollection.WellknownColumnResolver;
import org.bricolages.streaming.stream.StreamColumn;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("column")
public class ColumnDefinitionReference implements ColumnDefinition {
    @JsonProperty("name")
    private String domainName;
    private WellknownColumnResolver resolver;
    
    private ColumnDefinition getColumnDefinition() {
        return resolver.resolve(domainName);
    }

    public String getName() {
        return getColumnDefinition().getName();
    }

    public String getOriginalName() {
        return getColumnDefinition().getOriginalName();
    }

    public DomainParameters getDomain() {
        return getColumnDefinition().getDomain();
    }

    public StreamColumn getStreamColumn() {
        return getColumnDefinition().getStreamColumn();
    }

    @JsonCreator
    public ColumnDefinitionReference(String domainName, @JacksonInject WellknownColumnResolver resolver) {
        this.domainName = domainName;
        this.resolver = resolver;
    }
}
