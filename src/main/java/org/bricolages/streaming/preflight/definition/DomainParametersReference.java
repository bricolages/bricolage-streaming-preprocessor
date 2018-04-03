package org.bricolages.streaming.preflight.definition;
import org.bricolages.streaming.preflight.definition.DomainCollection.DomainResolver;
import org.bricolages.streaming.stream.StreamColumn;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.List;

@JsonTypeName("domain")
public class DomainParametersReference implements DomainParameters {
    @JsonProperty("name")
    private String domainName;
    private DomainResolver resolver;

    @JsonCreator
    public DomainParametersReference(String domainName, @JacksonInject DomainResolver resolver) {
        this.domainName = domainName;
        this.resolver = resolver;
    }
    
    private DomainParameters getDomain() {
        return resolver.resolve(domainName);
    }

    public String getType() {
        return getDomain().getType();
    }

    public ColumnEncoding getEncoding() {
      return getDomain().getEncoding();
    }

    public List<OperatorDefinitionEntry> getOperatorDefinitionEntries() {
        return getDomain().getOperatorDefinitionEntries();
    }

    public StreamColumn.Params getStreamColumnParams() {
        return getDomain().getStreamColumnParams();
    }
}
